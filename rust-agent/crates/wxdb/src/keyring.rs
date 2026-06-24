pub use crate::scanner::KeyEntry;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use crate::config::{self, RuntimeConfig};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoredKeyring {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub stores: BTreeMap<String, BTreeMap<String, StoredKey>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredKey {
    pub enc_key: String,
    #[serde(default)]
    pub salt: String,
    #[serde(default)]
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyRefreshReport {
    pub db_dir: PathBuf,
    pub before_keys: usize,
    pub imported_legacy_keys: usize,
    pub scanned_keys: usize,
    pub after_keys: usize,
    pub scan_error: Option<String>,
}

fn default_version() -> u32 {
    1
}

pub fn refresh_keys(config: &RuntimeConfig) -> Result<Vec<KeyRefreshReport>> {
    let mut keyring = load_keyring(&config.keys_file).unwrap_or_default();
    let mut reports = Vec::new();
    for db_dir in &config.db_dirs {
        let report = refresh_keys_for_db_dir(config, db_dir, &mut keyring, true)?;
        reports.push(report);
    }
    save_keyring(&config.keys_file, &keyring)?;
    Ok(reports)
}

pub(crate) fn ensure_keys_for_db_dir(
    config: &RuntimeConfig,
    db_dir: &Path,
) -> Result<HashMap<String, String>> {
    let mut keyring = load_keyring(&config.keys_file).unwrap_or_default();
    let _ = refresh_keys_for_db_dir(config, db_dir, &mut keyring, false)?;
    save_keyring(&config.keys_file, &keyring)?;
    Ok(keys_for_db_dir(&keyring, db_dir))
}

pub(crate) fn load_keyring(path: &Path) -> Result<StoredKeyring> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("读取 wxdb keyring 失败: {}", path.display()))?;
    let mut keyring: StoredKeyring = serde_json::from_str(&content)
        .with_context(|| format!("解析 wxdb keyring 失败: {}", path.display()))?;
    if keyring.version == 0 {
        keyring.version = 1;
    }
    Ok(keyring)
}

pub(crate) fn save_keyring(path: &Path, keyring: &StoredKeyring) -> Result<()> {
    config::ensure_parent(path)?;
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    std::fs::write(&tmp, serde_json::to_string_pretty(keyring)?)?;
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("写入 wxdb keyring 失败: {}", path.display()))?;
    Ok(())
}

pub(crate) fn keys_for_db_dir(keyring: &StoredKeyring, db_dir: &Path) -> HashMap<String, String> {
    let store_key = store_key(db_dir);
    keyring
        .stores
        .get(&store_key)
        .map(|keys| {
            keys.iter()
                .map(|(rel, key)| (rel.clone(), key.enc_key.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn refresh_keys_for_db_dir(
    config: &RuntimeConfig,
    db_dir: &Path,
    keyring: &mut StoredKeyring,
    force_scan: bool,
) -> Result<KeyRefreshReport> {
    let db_key = store_key(db_dir);
    let before_keys = keyring
        .stores
        .get(&db_key)
        .map(|keys| keys.len())
        .unwrap_or(0);
    let mut imported_legacy_keys = 0usize;
    let mut scanned_keys = 0usize;
    let mut scan_error = None;

    if let Some(legacy_keys_file) = config.legacy_keys_file_for(db_dir) {
        if let Ok(content) = std::fs::read_to_string(&legacy_keys_file) {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&content) {
                let legacy_keys = config::flatten_key_map(&value);
                imported_legacy_keys =
                    merge_plain_keys(keyring, db_dir, legacy_keys, "legacy-wx-cli".to_string());
            }
        }
    }

    let message_keys = config::message_db_keys(db_dir);
    let current_keys = keys_for_db_dir(keyring, db_dir);
    let current_store = keyring.stores.get(&db_key);
    let missing_message_key = message_keys
        .iter()
        .any(|rel| !current_keys.contains_key(rel));
    let has_contact_key = current_keys.contains_key("contact/contact.db");
    let has_session_key = current_keys.contains_key("session/session.db");
    let has_unverified_legacy_key = current_store
        .map(|store| {
            ["contact/contact.db", "session/session.db"]
                .into_iter()
                .chain(message_keys.iter().map(|rel| rel.as_str()))
                .any(|rel| {
                    store
                        .get(rel)
                        .map(|key| key.salt.is_empty() || key.source != "memory-scan")
                        .unwrap_or(false)
                })
        })
        .unwrap_or(false);

    if force_scan
        || missing_message_key
        || !has_contact_key
        || !has_session_key
        || has_unverified_legacy_key
    {
        match crate::scanner::scan_keys(db_dir) {
            Ok(entries) => {
                scanned_keys = merge_scanned_keys(keyring, db_dir, entries);
            }
            Err(error) => {
                scan_error = Some(error.to_string());
            }
        }
    }

    let after_keys = keyring
        .stores
        .get(&db_key)
        .map(|keys| keys.len())
        .unwrap_or(0);
    Ok(KeyRefreshReport {
        db_dir: db_dir.to_path_buf(),
        before_keys,
        imported_legacy_keys,
        scanned_keys,
        after_keys,
        scan_error,
    })
}

fn merge_plain_keys(
    keyring: &mut StoredKeyring,
    db_dir: &Path,
    keys: HashMap<String, String>,
    source: String,
) -> usize {
    let db_key = store_key(db_dir);
    let store = keyring.stores.entry(db_key).or_default();
    let mut inserted = 0usize;
    for (rel, enc_key) in keys {
        if enc_key.len() != 64 {
            continue;
        }
        if store
            .get(&rel)
            .map(|existing| existing.source == "memory-scan" && !existing.salt.is_empty())
            .unwrap_or(false)
        {
            continue;
        }
        if !store.contains_key(&rel) {
            inserted += 1;
        }
        store.insert(
            rel,
            StoredKey {
                enc_key: enc_key.to_ascii_lowercase(),
                salt: String::new(),
                source: source.clone(),
            },
        );
    }
    inserted
}

fn merge_scanned_keys(keyring: &mut StoredKeyring, db_dir: &Path, entries: Vec<KeyEntry>) -> usize {
    let db_key = store_key(db_dir);
    let store = keyring.stores.entry(db_key).or_default();
    let mut upserted = 0usize;
    for entry in entries {
        if entry.enc_key.len() != 64 {
            continue;
        }
        let should_count = store
            .get(&entry.db_name)
            .map(|existing| {
                existing.enc_key != entry.enc_key
                    || existing.salt != entry.salt
                    || existing.source != "memory-scan"
            })
            .unwrap_or(true);
        if should_count {
            upserted += 1;
        }
        store.insert(
            entry.db_name,
            StoredKey {
                enc_key: entry.enc_key.to_ascii_lowercase(),
                salt: entry.salt,
                source: "memory-scan".to_string(),
            },
        );
    }
    upserted
}

fn store_key(db_dir: &Path) -> String {
    config::normalize_path(db_dir.to_path_buf())
        .to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_are_keyed_case_insensitively() {
        let path = PathBuf::from(r"C:\Temp\xwechat\wxid\db_storage");
        assert!(store_key(&path).contains("db_storage"));
        assert!(!store_key(&path).contains('\\'));
    }
}
