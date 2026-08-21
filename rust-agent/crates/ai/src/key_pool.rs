//! Shared API-key pool with per-key concurrency caps and a process-wide
//! registry so identically-configured clients share one pool.
//!
//! Moved verbatim from lib.rs.

use std::{
    collections::{HashMap, HashSet},
    env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, OnceLock, Weak,
    },
};

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

use crate::{direct_value_in_env_field, env_var, missing_api_key, AiError};

/// A shared pool of API keys with per-key concurrency caps.
///
/// Requests call [`ApiKeyPool::acquire`] to grab a key permit. The permit is held
/// for the whole request (including its retry loop) and released on drop, so the
/// per-key cap is enforced across the whole process even when many summary tasks
/// run in parallel. Keys are picked round-robin so load spreads across accounts;
/// when every key is busy the caller waits for the first candidate key.
#[derive(Debug)]
pub struct ApiKeyPool {
    slots: Vec<Arc<ApiKeySlot>>,
    next: AtomicUsize,
}

#[derive(Debug)]
struct ApiKeySlot {
    key: Arc<str>,
    semaphore: Option<Arc<Semaphore>>,
}

/// A granted key slot. Dropping it releases the per-key concurrency permit.
#[derive(Debug)]
pub struct ApiKeyPermit {
    key_index: usize,
    key: Arc<str>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl ApiKeyPermit {
    pub fn key_index(&self) -> usize {
        self.key_index
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl ApiKeyPool {
    /// Build a pool from raw keys. `max_concurrent_per_key == 0` means unlimited
    /// (round-robin distribution only, no per-key gating).
    pub fn from_keys(keys: Vec<String>, max_concurrent_per_key: usize) -> Self {
        let mut seen = HashSet::new();
        let slots = keys
            .into_iter()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty() && seen.insert(key.clone()))
            .map(|key| {
                Arc::new(ApiKeySlot {
                    key: Arc::from(key.as_str()),
                    semaphore: (max_concurrent_per_key > 0)
                        .then(|| Arc::new(Semaphore::new(max_concurrent_per_key))),
                })
            })
            .collect();
        Self {
            slots,
            next: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn keys(&self) -> Vec<&str> {
        self.slots.iter().map(|slot| slot.key.as_ref()).collect()
    }

    pub async fn acquire(&self) -> ApiKeyPermit {
        if self.slots.is_empty() {
            return ApiKeyPermit {
                key_index: 0,
                key: Arc::from(""),
                _permit: None,
            };
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        for offset in 0..self.slots.len() {
            let index = (start + offset) % self.slots.len();
            let slot = &self.slots[index];
            if let Some(semaphore) = &slot.semaphore {
                if let Ok(permit) = semaphore.clone().try_acquire_owned() {
                    return ApiKeyPermit {
                        key_index: index,
                        key: Arc::clone(&slot.key),
                        _permit: Some(permit),
                    };
                }
            } else {
                return ApiKeyPermit {
                    key_index: index,
                    key: Arc::clone(&slot.key),
                    _permit: None,
                };
            }
        }
        let slot = &self.slots[start];
        let permit = Arc::clone(slot.semaphore.as_ref().expect("slot has a semaphore"))
            .acquire_owned()
            .await
            .expect("key pool semaphore is never closed");
        ApiKeyPermit {
            key_index: start,
            key: Arc::clone(&slot.key),
            _permit: Some(permit),
        }
    }
}

static KEY_POOL_REGISTRY: OnceLock<StdMutex<HashMap<String, Weak<ApiKeyPool>>>> = OnceLock::new();

/// Get-or-create the process-wide pool for a resolved key set. Clients built from
/// the same credentials share one pool so per-key concurrency caps are global.
pub(crate) fn shared_key_pool(keys: Vec<String>, max_concurrent_per_key: usize) -> Arc<ApiKeyPool> {
    let fingerprint = key_pool_fingerprint(&keys, max_concurrent_per_key);
    let registry = KEY_POOL_REGISTRY.get_or_init(|| StdMutex::new(HashMap::new()));
    {
        let guard = registry.lock().unwrap();
        if let Some(pool) = guard.get(&fingerprint).and_then(Weak::upgrade) {
            return pool;
        }
    }
    let pool = Arc::new(ApiKeyPool::from_keys(keys, max_concurrent_per_key));
    registry
        .lock()
        .unwrap()
        .insert(fingerprint, Arc::downgrade(&pool));
    pool
}

/// Stable hash of the (deduped, order-insensitive) key set plus the per-key cap.
/// Used only as a registry key; never logged or exposed.
fn key_pool_fingerprint(keys: &[String], max_concurrent_per_key: usize) -> String {
    let mut sorted = keys
        .iter()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
        .collect::<Vec<_>>();
    sorted.sort();
    sorted.dedup();
    let mut hasher = Sha256::new();
    for key in &sorted {
        hasher.update(key.as_bytes());
        hasher.update(b"\n");
    }
    hasher.update(max_concurrent_per_key.to_string().as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Resolve the API key list for one client section.
///
/// Priority: explicit `api_keys` list, then `api_key` (may be comma/newline
/// separated), then `api_keys_env` (optional env var, may be comma/newline
/// separated), then `api_key_env`. Errors only when no key resolves at all.
pub(crate) fn resolve_api_keys(
    api_keys: &[String],
    api_key: Option<&str>,
    api_keys_env: &str,
    api_key_env: &str,
    purpose: &'static str,
) -> Result<Vec<String>, AiError> {
    let mut keys = split_api_key_list(&api_keys.join("\n"));
    if keys.is_empty() {
        if let Some(value) = api_key.map(str::trim).filter(|value| !value.is_empty()) {
            keys = split_api_key_list(value);
        }
    }
    if keys.is_empty() {
        let env_name = api_keys_env.trim();
        if !env_name.is_empty() {
            if let Some(direct) = direct_value_in_env_field(env_name, purpose) {
                keys = split_api_key_list(&direct);
            } else if let Ok(value) = env::var(env_name) {
                keys = split_api_key_list(&value);
            } else {
                warn!(
                    env = %env_name,
                    purpose,
                    "multi-key environment variable configured but not set; falling back to single-key resolution"
                );
            }
        }
    }
    if keys.is_empty() {
        keys = match direct_value_in_env_field(api_key_env, purpose) {
            Some(direct) => split_api_key_list(&direct),
            None => split_api_key_list(&env_var(api_key_env, purpose)?),
        };
    }
    if keys.is_empty() {
        return Err(missing_api_key(api_key_env));
    }
    Ok(keys)
}

pub(crate) fn split_api_key_list(value: &str) -> Vec<String> {
    value
        .split([',', '\n', '\r', ';'])
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
