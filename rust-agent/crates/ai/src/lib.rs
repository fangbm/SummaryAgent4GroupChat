use std::{
    collections::{HashMap, HashSet},
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, OnceLock, Weak,
    },
    time::Duration,
};

use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use reqwest::header::{HeaderMap, ACCEPT, CONTENT_TYPE, RETRY_AFTER};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout, Instant};
use tracing::{info, warn};
use uuid::Uuid;
use wechat_summary_core::{
    config::{
        ImageCaptionConfig, ImageGenConfig, LlmConfig, ProxyConfig, VideoCaptionConfig,
        VoiceTranscriptionConfig,
    },
    ImageArtifact,
};

const HTTP_RETRY_INITIAL_DELAY_MS: u64 = 1_000;
const HTTP_RETRY_MAX_DELAY_MS: u64 = 30_000;
const HTTP_RETRY_TOTAL_DELAY_BUDGET_MS: u64 = 180_000;

static HTTP_RATE_LIMIT_RETRY_QUEUE: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone, Copy)]
struct RetryBudget {
    started: Instant,
    deadline: Option<Instant>,
    retry_delay_budget: Option<Duration>,
}

impl RetryBudget {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            deadline: None,
            retry_delay_budget: Some(Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS)),
        }
    }

    fn with_deadline(deadline: Instant) -> Self {
        Self {
            started: Instant::now(),
            deadline: Some(deadline),
            retry_delay_budget: None,
        }
    }

    fn allows(&self, delay_ms: u64) -> bool {
        delay_ms <= self.remaining_ms()
    }

    fn remaining_ms(&self) -> u64 {
        self.remaining_duration().as_millis() as u64
    }

    fn remaining_duration(&self) -> Duration {
        let retry_budget = self
            .retry_delay_budget
            .map(|budget| budget.saturating_sub(self.started.elapsed()))
            .unwrap_or(Duration::MAX);
        let deadline_budget = self.deadline.map(|deadline| {
            let now = Instant::now();
            if deadline > now {
                deadline - now
            } else {
                Duration::ZERO
            }
        });
        deadline_budget.map_or(retry_budget, |budget| retry_budget.min(budget))
    }

    fn deadline_exhausted(&self) -> bool {
        self.deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(false)
    }
}

#[derive(Debug, Error)]
pub enum AiError {
    #[error("missing API key; set environment variable {env_var} or configure api_key directly")]
    MissingApiKey { env_var: String },
    #[error("missing environment variable {name} for {purpose}")]
    MissingEnv { name: String, purpose: &'static str },
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("streaming response failed: {0}")]
    Stream(String),
    #[error("rate limited: {0}")]
    RateLimited(String),
    #[error("image task failed: {0}")]
    ImageTaskFailed(String),
    #[error("image task timed out after {0} seconds")]
    ImageTaskTimeout(u64),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
}

#[derive(Debug)]
enum ChatCompletionRequestError {
    Http(reqwest::Error),
    FirstEventTimeout(u64),
}

impl ChatCompletionRequestError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Http(error) => should_retry_http_transport_error(error),
            Self::FirstEventTimeout(_) => true,
        }
    }

    fn retry_reason(&self) -> String {
        match self {
            Self::Http(error) => retry_reason_from_transport_error(error),
            Self::FirstEventTimeout(seconds) => {
                format!("stream first event timed out after {seconds} seconds")
            }
        }
    }

    fn into_ai_error(self) -> AiError {
        match self {
            Self::Http(error) => AiError::Http(error),
            Self::FirstEventTimeout(seconds) => AiError::Stream(format!(
                "did not receive response headers within {seconds} seconds"
            )),
        }
    }
}

impl std::fmt::Display for ChatCompletionRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(error) => error.fmt(formatter),
            Self::FirstEventTimeout(seconds) => write!(
                formatter,
                "stream first event timed out after {seconds} seconds before response headers"
            ),
        }
    }
}

#[derive(Debug)]
enum ChatCompletionStreamError {
    FirstEventTimeout(u64),
    IdleTimeout(u64),
    EndedWithoutDone,
    Read(reqwest::Error),
    InvalidEvent(String),
}

impl ChatCompletionStreamError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::FirstEventTimeout(_)
                | Self::IdleTimeout(_)
                | Self::EndedWithoutDone
                | Self::Read(_)
        )
    }

    fn into_ai_error(self) -> AiError {
        AiError::Stream(self.to_string())
    }
}

impl std::fmt::Display for ChatCompletionStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FirstEventTimeout(seconds) => {
                write!(
                    formatter,
                    "did not receive first SSE event within {seconds} seconds"
                )
            }
            Self::IdleTimeout(seconds) => {
                write!(formatter, "SSE stream was idle for {seconds} seconds")
            }
            Self::EndedWithoutDone => write!(formatter, "SSE stream ended before [DONE]"),
            Self::Read(error) => write!(formatter, "failed to read SSE stream: {error}"),
            Self::InvalidEvent(error) => write!(formatter, "invalid SSE event: {error}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AiRetryNotice {
    pub operation: &'static str,
    pub attempt: usize,
    pub max_attempts: usize,
    pub retry_after_ms: u64,
    pub reason: String,
}

pub type RetryNotifier =
    Arc<dyn Fn(AiRetryNotice) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

#[derive(Debug, Clone, Default, Serialize)]
pub struct AiTraceContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_total: Option<usize>,
}

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
fn shared_key_pool(keys: Vec<String>, max_concurrent_per_key: usize) -> Arc<ApiKeyPool> {
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
fn resolve_api_keys(
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

fn split_api_key_list(value: &str) -> Vec<String> {
    value
        .split([',', '\n', '\r', ';'])
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Clone)]
pub struct OpenAiCompatibleLlm {
    config: LlmConfig,
    client: reqwest::Client,
    key_pool: Arc<ApiKeyPool>,
    base_url: String,
    model: String,
    retry_notifier: Option<RetryNotifier>,
    trace_dir: Option<PathBuf>,
    trace_context: Option<AiTraceContext>,
}

impl OpenAiCompatibleLlm {
    pub fn new(config: LlmConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let keys = resolve_api_keys(
            &config.api_keys,
            config.api_key.as_deref(),
            &config.api_keys_env,
            &config.api_key_env,
            "LLM API key",
        )?;
        let base_url = config_value_or_env(
            config.base_url.as_deref(),
            &config.base_url_env,
            "LLM base URL",
        )?;
        let model =
            config_value_or_env(config.model.as_deref(), &config.model_env, "LLM model name")?;
        let client = http_client(config.timeout_seconds, proxy)?;
        let max_concurrent_per_key = config.max_concurrent_per_key;
        Ok(Self {
            config,
            client,
            key_pool: shared_key_pool(keys, max_concurrent_per_key),
            base_url,
            model,
            retry_notifier: None,
            trace_dir: None,
            trace_context: None,
        })
    }

    pub fn with_retry_notifier(mut self, retry_notifier: RetryNotifier) -> Self {
        self.retry_notifier = Some(retry_notifier);
        self
    }

    pub fn with_trace_dir(mut self, trace_dir: impl Into<PathBuf>) -> Self {
        self.trace_dir = Some(trace_dir.into());
        self
    }

    pub fn with_trace_context(mut self, trace_context: AiTraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    pub async fn summarize(&self, merged_input: &str) -> Result<String, AiError> {
        self.complete(&self.config.system_prompt, merged_input)
            .await
    }

    pub async fn complete(
        &self,
        system_prompt: &str,
        user_content: &str,
    ) -> Result<String, AiError> {
        self.complete_with_max_tokens(
            system_prompt,
            user_content,
            Some(self.config.max_output_tokens),
        )
        .await
    }

    pub async fn complete_without_max_tokens(
        &self,
        system_prompt: &str,
        user_content: &str,
    ) -> Result<String, AiError> {
        self.complete_with_max_tokens(system_prompt, user_content, None)
            .await
    }

    async fn complete_with_max_tokens(
        &self,
        system_prompt: &str,
        user_content: &str,
        max_tokens: Option<u32>,
    ) -> Result<String, AiError> {
        let endpoint = chat_completions_endpoint(&self.base_url);
        let mut payload = chat_completion_payload(
            &self.model,
            system_prompt,
            user_content,
            self.config.temperature,
            max_tokens,
        );
        apply_request_body_overrides(&mut payload, &self.config.request_body_overrides);
        if self.config.stream {
            payload["stream"] = Value::Bool(true);
        }

        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let mut thinking_fallback_used = false;
        let permit = self.key_pool.acquire().await;
        let key_index = permit.key_index();
        'request_variants: loop {
            let retry_budget = RetryBudget::new();
            for attempt in 1..=max_attempts {
                let trace_id = Uuid::new_v4();
                let started = Instant::now();
                info!(
                    trace_id = %trace_id,
                    base_url = %self.base_url,
                    model = %self.model,
                    key_index,
                    key_count = self.key_pool.len(),
                    system_chars = system_prompt.chars().count(),
                    user_chars = user_content.chars().count(),
                    max_tokens = ?max_tokens,
                    timeout_seconds = self.config.timeout_seconds,
                    stream = self.config.stream,
                    stream_first_event_timeout_seconds = self.config.stream_first_event_timeout_seconds,
                    stream_idle_timeout_seconds = self.config.stream_idle_timeout_seconds,
                    attempt,
                    max_attempts,
                    thinking_fallback = thinking_fallback_used,
                    "LLM chat completion request started"
                );
                let stream_first_event_timeout_seconds =
                    self.config.stream_first_event_timeout_seconds.max(1);
                let request = self
                    .client
                    .post(&endpoint)
                    .bearer_auth(permit.key())
                    .json(&payload);
                let response = match send_chat_completion_request(
                    request,
                    self.config
                        .stream
                        .then(|| Duration::from_secs(stream_first_event_timeout_seconds)),
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        let elapsed_ms = started.elapsed().as_millis();
                        let retry = attempt < max_attempts
                            && error.is_retryable()
                            && retry_budget.allows(http_retry_delay_ms(attempt));
                        let retry_after_ms = if retry {
                            http_retry_delay_ms(attempt)
                        } else {
                            0
                        };
                        self.write_http_trace(AiHttpTrace {
                            trace_id,
                            operation: "llm_chat_completion",
                            context: self.trace_context.as_ref(),
                            method: "POST",
                            endpoint: &endpoint,
                            model: Some(&self.model),
                            attempt,
                            max_attempts,
                            elapsed_ms,
                            status: None,
                            retry,
                            retry_after_ms,
                            max_tokens,
                            request_body: Some(&payload),
                            response_body: None,
                            error: Some(&error.to_string()),
                        });
                        warn!(
                            trace_id = %trace_id,
                            elapsed_ms,
                            attempt,
                            max_attempts,
                            retry,
                            retry_after_ms,
                            error = %error,
                            "LLM chat completion transport failed"
                        );
                        if retry {
                            notify_retry(
                                &self.retry_notifier,
                                "LLM chat completion",
                                attempt,
                                max_attempts,
                                retry_after_ms,
                                error.retry_reason(),
                            )
                            .await;
                            sleep(http_retry_delay(attempt)).await;
                            continue;
                        }
                        return Err(error.into_ai_error());
                    }
                };
                let status = response.status();
                let server_retry_after_ms =
                    retry_after_ms_from_headers(response.headers(), Utc::now());
                let elapsed_ms = started.elapsed().as_millis();
                if !status.is_success() {
                    let body = response.text().await.unwrap_or_else(|error| {
                        format!("failed to read error response body: {error}")
                    });
                    let snippet = truncate_for_log(&body, 500);
                    let response_for_log = full_response_for_log(&body);
                    let retry_candidate = attempt < max_attempts
                        && should_retry_chat_completion_failure(status, &snippet)
                        && retry_budget.allows(http_retry_delay_ms_for_status(
                            status,
                            attempt,
                            server_retry_after_ms,
                        ));
                    let retry_delay_ms = if retry_candidate {
                        http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                    } else {
                        0
                    };
                    let retry_after_ms = if retry_candidate {
                        wait_before_status_retry(
                            "LLM chat completion",
                            status,
                            attempt,
                            max_attempts,
                            &retry_budget,
                            retry_delay_ms,
                        )
                        .await
                        .unwrap_or(0)
                    } else {
                        0
                    };
                    let retry = retry_after_ms > 0;
                    self.write_http_trace(AiHttpTrace {
                        trace_id,
                        operation: "llm_chat_completion",
                        context: self.trace_context.as_ref(),
                        method: "POST",
                        endpoint: &endpoint,
                        model: Some(&self.model),
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: Some(status.as_u16()),
                        retry,
                        retry_after_ms,
                        max_tokens,
                        request_body: Some(&payload),
                        response_body: Some(&body),
                        error: None,
                    });
                    warn!(
                        trace_id = %trace_id,
                        status = %status,
                        elapsed_ms,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        response = %response_for_log,
                        "LLM chat completion request failed"
                    );
                    if retry {
                        notify_retry(
                            &self.retry_notifier,
                            "LLM chat completion",
                            attempt,
                            max_attempts,
                            retry_after_ms,
                            retry_reason_from_status(status, &snippet),
                        )
                        .await;
                        continue;
                    }
                    let message =
                        format!("chat completion API returned {status}: {response_for_log}");
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        return Err(AiError::RateLimited(message));
                    }
                    return Err(AiError::InvalidResponse(message));
                }
                info!(
                    trace_id = %trace_id,
                    status = %status,
                    elapsed_ms,
                    attempt,
                    max_attempts,
                    "LLM chat completion HTTP request completed"
                );

                let (body, response) = if self.config.stream && response_is_sse(&response) {
                    let remaining_first_event_timeout =
                        Duration::from_secs(stream_first_event_timeout_seconds)
                            .saturating_sub(started.elapsed());
                    match collect_streamed_chat_completion(
                        response,
                        remaining_first_event_timeout,
                        Duration::from_secs(self.config.stream_idle_timeout_seconds.max(1)),
                    )
                    .await
                    {
                        Ok(streamed) => (streamed.raw_body, streamed.response),
                        Err(error) => {
                            let elapsed_ms = started.elapsed().as_millis();
                            let retry = attempt < max_attempts
                                && error.is_retryable()
                                && retry_budget.allows(http_retry_delay_ms(attempt));
                            let retry_after_ms = if retry {
                                http_retry_delay_ms(attempt)
                            } else {
                                0
                            };
                            let error_message = error.to_string();
                            self.write_http_trace(AiHttpTrace {
                                trace_id,
                                operation: "llm_chat_completion",
                                context: self.trace_context.as_ref(),
                                method: "POST",
                                endpoint: &endpoint,
                                model: Some(&self.model),
                                attempt,
                                max_attempts,
                                elapsed_ms,
                                status: Some(status.as_u16()),
                                retry,
                                retry_after_ms,
                                max_tokens,
                                request_body: Some(&payload),
                                response_body: None,
                                error: Some(&error_message),
                            });
                            warn!(
                                trace_id = %trace_id,
                                elapsed_ms,
                                attempt,
                                max_attempts,
                                retry,
                                retry_after_ms,
                                error = %error_message,
                                "LLM chat completion stream failed"
                            );
                            if retry {
                                notify_retry(
                                    &self.retry_notifier,
                                    "LLM chat completion",
                                    attempt,
                                    max_attempts,
                                    retry_after_ms,
                                    error_message,
                                )
                                .await;
                                sleep(http_retry_delay(attempt)).await;
                                continue;
                            }
                            return Err(error.into_ai_error());
                        }
                    }
                } else {
                    let body = response.text().await?;
                    let response = match serde_json::from_str::<Value>(&body) {
                        Ok(response) => response,
                        Err(error) => {
                            self.write_http_trace(AiHttpTrace {
                                trace_id,
                                operation: "llm_chat_completion",
                                context: self.trace_context.as_ref(),
                                method: "POST",
                                endpoint: &endpoint,
                                model: Some(&self.model),
                                attempt,
                                max_attempts,
                                elapsed_ms,
                                status: Some(status.as_u16()),
                                retry: false,
                                retry_after_ms: 0,
                                max_tokens,
                                request_body: Some(&payload),
                                response_body: Some(&body),
                                error: None,
                            });
                            let response_for_log = full_response_for_log(&body);
                            warn!(
                                trace_id = %trace_id,
                                elapsed_ms,
                                response = %response_for_log,
                                "LLM chat completion JSON parsing failed"
                            );
                            return Err(AiError::InvalidResponse(format!(
                                "invalid chat completion JSON: {error}; response={response_for_log}"
                            )));
                        }
                    };
                    (body, response)
                };
                let elapsed_ms = started.elapsed().as_millis();
                let Some(content) = extract_chat_completion_content(&response) else {
                    let finish_reason = chat_completion_finish_reason(&response);
                    let retry_without_thinking = !thinking_fallback_used
                        && should_retry_empty_length_completion_without_thinking(
                            &payload, &response,
                        );
                    self.write_http_trace(AiHttpTrace {
                        trace_id,
                        operation: "llm_chat_completion",
                        context: self.trace_context.as_ref(),
                        method: "POST",
                        endpoint: &endpoint,
                        model: Some(&self.model),
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: Some(status.as_u16()),
                        retry: retry_without_thinking,
                        retry_after_ms: 0,
                        max_tokens,
                        request_body: Some(&payload),
                        response_body: Some(&body),
                        error: None,
                    });
                    let response_for_log = full_response_for_log(&response.to_string());
                    warn!(
                        trace_id = %trace_id,
                        finish_reason = %finish_reason,
                        retry = retry_without_thinking,
                        fallback = if retry_without_thinking { "thinking_disabled" } else { "none" },
                        response = %response_for_log,
                        "LLM chat completion response is missing content"
                    );
                    if retry_without_thinking {
                        disable_chat_completion_thinking(&mut payload);
                        thinking_fallback_used = true;
                        continue 'request_variants;
                    }
                    return Err(AiError::InvalidResponse(format!(
                    "missing chat completion content (finish_reason={finish_reason}); response={response_for_log}"
                )));
                };
                self.write_http_trace(AiHttpTrace {
                    trace_id,
                    operation: "llm_chat_completion",
                    context: self.trace_context.as_ref(),
                    method: "POST",
                    endpoint: &endpoint,
                    model: Some(&self.model),
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    status: Some(status.as_u16()),
                    retry: false,
                    retry_after_ms: 0,
                    max_tokens,
                    request_body: Some(&payload),
                    response_body: Some(&body),
                    error: None,
                });
                info!(
                    trace_id = %trace_id,
                    output_chars = content.chars().count(),
                    "LLM chat completion response parsed"
                );
                return Ok(content);
            }

            unreachable!("chat completion retry loop always returns")
        }
    }

    fn write_http_trace(&self, trace: AiHttpTrace<'_>) {
        let Some(trace_dir) = &self.trace_dir else {
            return;
        };
        write_ai_http_trace(trace_dir, trace);
    }
}

struct AiHttpTrace<'a> {
    trace_id: Uuid,
    operation: &'static str,
    context: Option<&'a AiTraceContext>,
    method: &'static str,
    endpoint: &'a str,
    model: Option<&'a str>,
    attempt: usize,
    max_attempts: usize,
    elapsed_ms: u128,
    status: Option<u16>,
    retry: bool,
    retry_after_ms: u64,
    max_tokens: Option<u32>,
    request_body: Option<&'a Value>,
    response_body: Option<&'a str>,
    error: Option<&'a str>,
}

fn write_ai_http_trace(trace_dir: &Path, trace: AiHttpTrace<'_>) {
    if let Err(error) = fs::create_dir_all(trace_dir) {
        warn!(error = %error, path = %trace_dir.display(), "failed to create AI trace directory");
        return;
    }
    let timestamp = Utc::now();
    let operation_slug = trace.operation.replace('_', "-");
    let file_name = format!(
        "{}-{}-{}-attempt-{}.json",
        timestamp.format("%Y%m%d-%H%M%S-%3f"),
        operation_slug,
        trace.trace_id,
        trace.attempt
    );
    let path = trace_dir.join(file_name);
    let response_json = trace
        .response_body
        .and_then(|body| serde_json::from_str::<Value>(body).ok());
    let response_body = match response_json {
        Some(value) => redact_json_for_trace(&value),
        None => trace
            .response_body
            .map(|body| Value::String(redact_trace_text(body)))
            .unwrap_or(Value::Null),
    };
    let request_body = trace
        .request_body
        .map(redact_json_for_trace)
        .unwrap_or(Value::Null);
    let context = trace
        .context
        .and_then(|context| serde_json::to_value(context).ok())
        .unwrap_or(Value::Null);
    let payload = json!({
        "trace_id": trace.trace_id.to_string(),
        "operation": trace.operation,
        "context": context,
        "created_at_utc": timestamp.to_rfc3339(),
        "method": trace.method,
        "endpoint": redact_endpoint_for_trace(trace.endpoint),
        "model": trace.model,
        "attempt": trace.attempt,
        "max_attempts": trace.max_attempts,
        "elapsed_ms": trace.elapsed_ms,
        "status": trace.status,
        "retry": trace.retry,
        "retry_after_ms": trace.retry_after_ms,
        "max_tokens": trace.max_tokens,
        "request_body": request_body,
        "response_body": response_body,
        "error": trace.error.map(redact_trace_text),
    });
    match serde_json::to_string_pretty(&payload) {
        Ok(text) => {
            if let Err(error) = fs::write(&path, text) {
                warn!(error = %error, path = %path.display(), "failed to write AI trace");
            } else {
                info!(
                    trace_id = %trace.trace_id,
                    operation = trace.operation,
                    path = %path.display(),
                    "AI trace written"
                );
            }
        }
        Err(error) => warn!(error = %error, "failed to serialize AI trace"),
    }
}

fn write_optional_ai_http_trace(trace_dir: &Option<PathBuf>, trace: AiHttpTrace<'_>) {
    if let Some(trace_dir) = trace_dir {
        write_ai_http_trace(trace_dir, trace);
    }
}

#[derive(Clone)]
pub struct OpenAiVisionCaptionClient {
    config: ImageCaptionConfig,
    client: reqwest::Client,
    key_pool: Arc<ApiKeyPool>,
    base_url: String,
    model: String,
    retry_notifier: Option<RetryNotifier>,
    trace_dir: Option<PathBuf>,
    trace_context: Option<AiTraceContext>,
}

impl OpenAiVisionCaptionClient {
    pub fn new(config: ImageCaptionConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let keys = resolve_api_keys(
            &config.api_keys,
            config.api_key.as_deref(),
            &config.api_keys_env,
            &config.api_key_env,
            "image caption API key",
        )?;
        let base_url = config_value_or_env(
            config.base_url.as_deref(),
            &config.base_url_env,
            "image caption API base URL",
        )?;
        let model = config_value_or_env(
            config.model.as_deref(),
            &config.model_env,
            "image caption model name",
        )?;
        let client = http_client(config.timeout_seconds, proxy)?;
        let max_concurrent_per_key = config.max_concurrent_per_key;
        Ok(Self {
            config,
            client,
            key_pool: shared_key_pool(keys, max_concurrent_per_key),
            base_url,
            model,
            retry_notifier: None,
            trace_dir: None,
            trace_context: None,
        })
    }

    pub fn with_retry_notifier(mut self, retry_notifier: RetryNotifier) -> Self {
        self.retry_notifier = Some(retry_notifier);
        self
    }

    pub fn with_trace_dir(mut self, trace_dir: impl Into<PathBuf>) -> Self {
        self.trace_dir = Some(trace_dir.into());
        self
    }

    pub fn with_trace_context(mut self, trace_context: AiTraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    pub async fn caption_image(&self, image_source: &str) -> Result<String, AiError> {
        let image_url = self.image_url_for_multimodal_request(image_source).await?;
        self.complete_with_image_url(&image_url).await
    }

    async fn image_url_for_multimodal_request(&self, source: &str) -> Result<String, AiError> {
        let trimmed = source.trim();
        if trimmed.starts_with("data:") {
            validate_image_data_url(trimmed)?;
            return Ok(trimmed.to_string());
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return self.download_image_as_data_url(trimmed).await;
        }
        let path = Path::new(trimmed);
        let bytes = fs::read(path)?;
        image_data_url_from_bytes(Some(path), &bytes)
    }

    async fn download_image_as_data_url(&self, url: &str) -> Result<String, AiError> {
        let source = redacted_url_source(url);
        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let retry_budget = RetryBudget::new();
        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(
                source = %source,
                attempt,
                max_attempts,
                "image caption remote image download started"
            );
            let response = match self.client.get(url).send().await {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    warn!(
                        source = %source,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        elapsed_ms = started.elapsed().as_millis(),
                        error = %error,
                        "image caption remote image download transport failed"
                    );
                    if retry {
                        notify_retry(
                            &self.retry_notifier,
                            "image caption remote image download",
                            attempt,
                            max_attempts,
                            retry_after_ms,
                            retry_reason_from_transport_error(&error),
                        )
                        .await;
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };
            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned);
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read image response body: {error}"));
                let snippet = truncate_for_log(&body, 300);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_http_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "image caption remote image download",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                warn!(
                    source = %source,
                    status = %status,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    elapsed_ms = started.elapsed().as_millis(),
                    response = %response_for_log,
                    "image caption remote image download failed"
                );
                if retry {
                    notify_retry(
                        &self.retry_notifier,
                        "image caption remote image download",
                        attempt,
                        max_attempts,
                        retry_after_ms,
                        retry_reason_from_status(status, &snippet),
                    )
                    .await;
                    continue;
                }
                return Err(AiError::InvalidResponse(format!(
                    "remote image download returned {status}: {response_for_log}"
                )));
            }
            let bytes = response.bytes().await?;
            let data_url = image_data_url_from_bytes(None, &bytes)?;
            info!(
                source = %source,
                content_type = content_type.as_deref().unwrap_or("unknown"),
                bytes = bytes.len(),
                attempt,
                max_attempts,
                elapsed_ms = started.elapsed().as_millis(),
                "image caption remote image downloaded"
            );
            return Ok(data_url);
        }

        unreachable!("image caption remote image download retry loop always returns")
    }

    async fn complete_with_image_url(&self, image_url: &str) -> Result<String, AiError> {
        let endpoint = chat_completions_endpoint(&self.base_url);
        let mut payload = image_caption_payload(
            &self.model,
            &self.config.system_prompt,
            &self.config.user_prompt,
            image_url,
            self.config.temperature,
            self.config.max_output_tokens,
        );
        apply_request_body_overrides(&mut payload, &self.config.request_body_overrides);

        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let permit = self.key_pool.acquire().await;
        let key_index = permit.key_index();
        let retry_budget = RetryBudget::new();
        for attempt in 1..=max_attempts {
            let trace_id = Uuid::new_v4();
            let started = Instant::now();
            info!(
                trace_id = %trace_id,
                base_url = %self.base_url,
                model = %self.model,
                key_index,
                key_count = self.key_pool.len(),
                prompt_chars = self.config.user_prompt.chars().count(),
                max_tokens = self.config.max_output_tokens,
                timeout_seconds = self.config.timeout_seconds,
                attempt,
                max_attempts,
                image_source_kind = if image_url.starts_with("data:") { "data_url" } else { "url" },
                "image caption request started"
            );
            let response = match self
                .client
                .post(&endpoint)
                .bearer_auth(permit.key())
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    write_optional_ai_http_trace(
                        &self.trace_dir,
                        AiHttpTrace {
                            trace_id,
                            operation: "image_caption",
                            context: self.trace_context.as_ref(),
                            method: "POST",
                            endpoint: &endpoint,
                            model: Some(&self.model),
                            attempt,
                            max_attempts,
                            elapsed_ms,
                            status: None,
                            retry,
                            retry_after_ms,
                            max_tokens: Some(self.config.max_output_tokens),
                            request_body: Some(&payload),
                            response_body: None,
                            error: Some(&error.to_string()),
                        },
                    );
                    warn!(
                        trace_id = %trace_id,
                        elapsed_ms,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        error = %error,
                        "image caption transport failed"
                    );
                    if retry {
                        notify_retry(
                            &self.retry_notifier,
                            "image caption request",
                            attempt,
                            max_attempts,
                            retry_after_ms,
                            retry_reason_from_transport_error(&error),
                        )
                        .await;
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };

            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let elapsed_ms = started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "image caption request",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                write_optional_ai_http_trace(
                    &self.trace_dir,
                    AiHttpTrace {
                        trace_id,
                        operation: "image_caption",
                        context: self.trace_context.as_ref(),
                        method: "POST",
                        endpoint: &endpoint,
                        model: Some(&self.model),
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: Some(status.as_u16()),
                        retry,
                        retry_after_ms,
                        max_tokens: Some(self.config.max_output_tokens),
                        request_body: Some(&payload),
                        response_body: Some(&body),
                        error: None,
                    },
                );
                warn!(
                    trace_id = %trace_id,
                    status = %status,
                    elapsed_ms,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "image caption request failed"
                );
                if retry {
                    notify_retry(
                        &self.retry_notifier,
                        "image caption request",
                        attempt,
                        max_attempts,
                        retry_after_ms,
                        retry_reason_from_status(status, &snippet),
                    )
                    .await;
                    continue;
                }
                let message = format!("image caption API returned {status}: {response_for_log}");
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(AiError::RateLimited(message));
                }
                return Err(AiError::InvalidResponse(message));
            }

            let body = response.text().await?;
            write_optional_ai_http_trace(
                &self.trace_dir,
                AiHttpTrace {
                    trace_id,
                    operation: "image_caption",
                    context: self.trace_context.as_ref(),
                    method: "POST",
                    endpoint: &endpoint,
                    model: Some(&self.model),
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    status: Some(status.as_u16()),
                    retry: false,
                    retry_after_ms: 0,
                    max_tokens: Some(self.config.max_output_tokens),
                    request_body: Some(&payload),
                    response_body: Some(&body),
                    error: None,
                },
            );
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let response_for_log = full_response_for_log(&body);
                warn!(
                    trace_id = %trace_id,
                    response = %response_for_log,
                    "image caption JSON parsing failed"
                );
                AiError::InvalidResponse(format!(
                    "invalid image caption JSON: {error}; response={response_for_log}"
                ))
            })?;
            let content = extract_chat_completion_content(&response).ok_or_else(|| {
                let finish_reason = chat_completion_finish_reason(&response);
                let response_for_log = full_response_for_log(&response.to_string());
                warn!(
                    trace_id = %trace_id,
                    finish_reason = %finish_reason,
                    response = %response_for_log,
                    "image caption response is missing content"
                );
                AiError::InvalidResponse(format!(
                    "missing image caption content (finish_reason={finish_reason}); response={response_for_log}"
                ))
            })?;
            info!(
                trace_id = %trace_id,
                output_chars = content.chars().count(),
                "image caption response parsed"
            );
            return Ok(content);
        }

        unreachable!("image caption retry loop always returns")
    }
}

#[derive(Clone)]
pub struct OpenAiAudioTranscriptionClient {
    config: VoiceTranscriptionConfig,
    client: reqwest::Client,
    key_pool: Arc<ApiKeyPool>,
    base_url: String,
    model: String,
    trace_dir: Option<PathBuf>,
    trace_context: Option<AiTraceContext>,
}

#[derive(Clone)]
pub struct OpenAiVideoCaptionClient {
    config: VideoCaptionConfig,
    client: reqwest::Client,
    key_pool: Arc<ApiKeyPool>,
    base_url: String,
    model: String,
    trace_dir: Option<PathBuf>,
    trace_context: Option<AiTraceContext>,
}

impl OpenAiVideoCaptionClient {
    pub fn new(config: VideoCaptionConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let keys = resolve_api_keys(
            &config.api_keys,
            config.api_key.as_deref(),
            &config.api_keys_env,
            &config.api_key_env,
            "video caption API key",
        )?;
        let base_url = config_value_or_env(
            config.base_url.as_deref(),
            &config.base_url_env,
            "video caption API base URL",
        )?;
        let model = config_value_or_env(
            config.model.as_deref(),
            &config.model_env,
            "video caption model name",
        )?;
        let client = http_client(config.timeout_seconds, proxy)?;
        let max_concurrent_per_key = config.max_concurrent_per_key;
        Ok(Self {
            config,
            client,
            key_pool: shared_key_pool(keys, max_concurrent_per_key),
            base_url,
            model,
            trace_dir: None,
            trace_context: None,
        })
    }

    pub fn with_trace_dir(mut self, trace_dir: impl Into<PathBuf>) -> Self {
        self.trace_dir = Some(trace_dir.into());
        self
    }

    pub fn with_trace_context(mut self, trace_context: AiTraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    pub async fn caption_video(&self, video_source: &str) -> Result<String, AiError> {
        let video_url = self.video_url_for_multimodal_request(video_source).await?;
        self.complete_with_video_url(&video_url).await
    }

    async fn video_url_for_multimodal_request(&self, source: &str) -> Result<String, AiError> {
        let trimmed = source.trim();
        if trimmed.starts_with("data:") {
            validate_video_data_url(trimmed, self.config.max_video_bytes)?;
            return Ok(trimmed.to_string());
        }
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return self.download_video_as_data_url(trimmed).await;
        }
        let path = Path::new(trimmed);
        let bytes = fs::read(path)?;
        video_data_url_from_bytes(Some(path), &bytes, self.config.max_video_bytes)
    }

    async fn download_video_as_data_url(&self, url: &str) -> Result<String, AiError> {
        let source = redacted_url_source(url);
        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let retry_budget = RetryBudget::new();
        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(
                source = %source,
                attempt,
                max_attempts,
                "video caption remote video download started"
            );
            let response = match self.client.get(url).send().await {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    warn!(
                        source = %source,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        elapsed_ms = started.elapsed().as_millis(),
                        error = %error,
                        "video caption remote video download transport failed"
                    );
                    if retry {
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };
            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned);
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read video response body: {error}"));
                let snippet = truncate_for_log(&body, 300);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_http_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "video caption remote video download",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                warn!(
                    source = %source,
                    status = %status,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    elapsed_ms = started.elapsed().as_millis(),
                    response = %response_for_log,
                    "video caption remote video download failed"
                );
                if retry {
                    continue;
                }
                return Err(AiError::InvalidResponse(format!(
                    "remote video download returned {status}: {response_for_log}"
                )));
            }
            let bytes = response.bytes().await?;
            let data_url = video_data_url_from_bytes(None, &bytes, self.config.max_video_bytes)?;
            info!(
                source = %source,
                content_type = content_type.as_deref().unwrap_or("unknown"),
                bytes = bytes.len(),
                attempt,
                max_attempts,
                elapsed_ms = started.elapsed().as_millis(),
                "video caption remote video downloaded"
            );
            return Ok(data_url);
        }

        unreachable!("video caption remote video download retry loop always returns")
    }

    async fn complete_with_video_url(&self, video_url: &str) -> Result<String, AiError> {
        let endpoint = chat_completions_endpoint(&self.base_url);
        let mut payload = video_caption_payload(
            &self.model,
            &self.config.system_prompt,
            &self.config.user_prompt,
            video_url,
            self.config.temperature,
            self.config.max_output_tokens,
        );
        apply_request_body_overrides(&mut payload, &self.config.request_body_overrides);

        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let permit = self.key_pool.acquire().await;
        let key_index = permit.key_index();
        let retry_budget = RetryBudget::new();
        for attempt in 1..=max_attempts {
            let trace_id = Uuid::new_v4();
            let started = Instant::now();
            info!(
                trace_id = %trace_id,
                base_url = %self.base_url,
                model = %self.model,
                key_index,
                key_count = self.key_pool.len(),
                prompt_chars = self.config.user_prompt.chars().count(),
                max_tokens = self.config.max_output_tokens,
                timeout_seconds = self.config.timeout_seconds,
                attempt,
                max_attempts,
                video_source_kind = if video_url.starts_with("data:") { "data_url" } else { "url" },
                "video caption request started"
            );
            let response = match self
                .client
                .post(&endpoint)
                .bearer_auth(permit.key())
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    write_optional_ai_http_trace(
                        &self.trace_dir,
                        AiHttpTrace {
                            trace_id,
                            operation: "video_caption",
                            context: self.trace_context.as_ref(),
                            method: "POST",
                            endpoint: &endpoint,
                            model: Some(&self.model),
                            attempt,
                            max_attempts,
                            elapsed_ms,
                            status: None,
                            retry,
                            retry_after_ms,
                            max_tokens: Some(self.config.max_output_tokens),
                            request_body: Some(&payload),
                            response_body: None,
                            error: Some(&error.to_string()),
                        },
                    );
                    warn!(
                        trace_id = %trace_id,
                        elapsed_ms,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        error = %error,
                        "video caption transport failed"
                    );
                    if retry {
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };

            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let elapsed_ms = started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "video caption request",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                write_optional_ai_http_trace(
                    &self.trace_dir,
                    AiHttpTrace {
                        trace_id,
                        operation: "video_caption",
                        context: self.trace_context.as_ref(),
                        method: "POST",
                        endpoint: &endpoint,
                        model: Some(&self.model),
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: Some(status.as_u16()),
                        retry,
                        retry_after_ms,
                        max_tokens: Some(self.config.max_output_tokens),
                        request_body: Some(&payload),
                        response_body: Some(&body),
                        error: None,
                    },
                );
                warn!(
                    trace_id = %trace_id,
                    status = %status,
                    elapsed_ms,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "video caption request failed"
                );
                if retry {
                    continue;
                }
                let message = format!("video caption API returned {status}: {response_for_log}");
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(AiError::RateLimited(message));
                }
                return Err(AiError::InvalidResponse(message));
            }

            let body = response.text().await?;
            write_optional_ai_http_trace(
                &self.trace_dir,
                AiHttpTrace {
                    trace_id,
                    operation: "video_caption",
                    context: self.trace_context.as_ref(),
                    method: "POST",
                    endpoint: &endpoint,
                    model: Some(&self.model),
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    status: Some(status.as_u16()),
                    retry: false,
                    retry_after_ms: 0,
                    max_tokens: Some(self.config.max_output_tokens),
                    request_body: Some(&payload),
                    response_body: Some(&body),
                    error: None,
                },
            );
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let response_for_log = full_response_for_log(&body);
                warn!(
                    trace_id = %trace_id,
                    response = %response_for_log,
                    "video caption JSON parsing failed"
                );
                AiError::InvalidResponse(format!(
                    "invalid video caption JSON: {error}; response={response_for_log}"
                ))
            })?;
            let content = extract_chat_completion_content(&response).ok_or_else(|| {
                let finish_reason = chat_completion_finish_reason(&response);
                let response_for_log = full_response_for_log(&response.to_string());
                warn!(
                    trace_id = %trace_id,
                    finish_reason = %finish_reason,
                    response = %response_for_log,
                    "video caption response is missing content"
                );
                AiError::InvalidResponse(format!(
                    "missing video caption content (finish_reason={finish_reason}); response={response_for_log}"
                ))
            })?;
            info!(
                trace_id = %trace_id,
                output_chars = content.chars().count(),
                "video caption response parsed"
            );
            return Ok(content);
        }

        unreachable!("video caption retry loop always returns")
    }
}

impl OpenAiAudioTranscriptionClient {
    pub fn new(config: VoiceTranscriptionConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let keys = resolve_api_keys(
            &config.api_keys,
            config.api_key.as_deref(),
            &config.api_keys_env,
            &config.api_key_env,
            "voice transcription API key",
        )?;
        let base_url = config_value_or_env(
            config.base_url.as_deref(),
            &config.base_url_env,
            "voice transcription API base URL",
        )?;
        let model = config_value_or_env(
            config.model.as_deref(),
            &config.model_env,
            "voice transcription model name",
        )?;
        let client = http_client(config.timeout_seconds, proxy)?;
        let max_concurrent_per_key = config.max_concurrent_per_key;
        Ok(Self {
            config,
            client,
            key_pool: shared_key_pool(keys, max_concurrent_per_key),
            base_url,
            model,
            trace_dir: None,
            trace_context: None,
        })
    }

    pub fn with_trace_dir(mut self, trace_dir: impl Into<PathBuf>) -> Self {
        self.trace_dir = Some(trace_dir.into());
        self
    }

    pub fn with_trace_context(mut self, trace_context: AiTraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    pub async fn transcribe_audio(&self, audio_path: &str) -> Result<String, AiError> {
        let path = Path::new(audio_path.trim());
        let bytes = fs::read(path)?;
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("voice.audio")
            .to_string();
        if self.uses_stepfun_asr() {
            return self.transcribe_audio_stepfun(path, bytes, file_name).await;
        }

        let endpoint = audio_transcriptions_endpoint(&self.base_url);
        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let permit = self.key_pool.acquire().await;
        let key_index = permit.key_index();
        let retry_budget = RetryBudget::new();

        for attempt in 1..=max_attempts {
            let trace_id = Uuid::new_v4();
            let started = Instant::now();
            let mut file_part =
                reqwest::multipart::Part::bytes(bytes.clone()).file_name(file_name.clone());
            let mime = audio_mime_type(path);
            if let Some(mime) = audio_mime_type(path) {
                file_part = file_part
                    .mime_str(mime)
                    .map_err(|error| AiError::InvalidResponse(error.to_string()))?;
            }
            let mut form = reqwest::multipart::Form::new()
                .text("model", self.model.clone())
                .part("file", file_part);
            if !self.config.language.trim().is_empty() {
                form = form.text("language", self.config.language.trim().to_string());
            }
            if !self.config.prompt.trim().is_empty() {
                form = form.text("prompt", self.config.prompt.trim().to_string());
            }
            if !self.config.response_format.trim().is_empty() {
                form = form.text(
                    "response_format",
                    self.config.response_format.trim().to_string(),
                );
            }
            for (key, value) in &self.config.request_body_overrides {
                if key == "file" || key == "model" {
                    continue;
                }
                form = form.text(key.clone(), toml_value_to_multipart_string(value));
            }
            let trace_payload =
                voice_transcription_multipart_trace_payload(self, &file_name, mime, &bytes);

            info!(
                trace_id = %trace_id,
                base_url = %self.base_url,
                model = %self.model,
                key_index,
                key_count = self.key_pool.len(),
                file = %file_name,
                bytes = bytes.len(),
                timeout_seconds = self.config.timeout_seconds,
                attempt,
                max_attempts,
                "voice transcription request started"
            );
            let response = match self
                .client
                .post(&endpoint)
                .bearer_auth(permit.key())
                .multipart(form)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    write_optional_ai_http_trace(
                        &self.trace_dir,
                        AiHttpTrace {
                            trace_id,
                            operation: "voice_transcription",
                            context: self.trace_context.as_ref(),
                            method: "POST",
                            endpoint: &endpoint,
                            model: Some(&self.model),
                            attempt,
                            max_attempts,
                            elapsed_ms,
                            status: None,
                            retry,
                            retry_after_ms,
                            max_tokens: None,
                            request_body: Some(&trace_payload),
                            response_body: None,
                            error: Some(&error.to_string()),
                        },
                    );
                    warn!(
                        trace_id = %trace_id,
                        elapsed_ms,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        error = %error,
                        "voice transcription transport failed"
                    );
                    if retry {
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };

            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let elapsed_ms = started.elapsed().as_millis();
            let body = response.text().await?;
            if !status.is_success() {
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "voice transcription request",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                write_optional_ai_http_trace(
                    &self.trace_dir,
                    AiHttpTrace {
                        trace_id,
                        operation: "voice_transcription",
                        context: self.trace_context.as_ref(),
                        method: "POST",
                        endpoint: &endpoint,
                        model: Some(&self.model),
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: Some(status.as_u16()),
                        retry,
                        retry_after_ms,
                        max_tokens: None,
                        request_body: Some(&trace_payload),
                        response_body: Some(&body),
                        error: None,
                    },
                );
                warn!(
                    trace_id = %trace_id,
                    status = %status,
                    elapsed_ms,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "voice transcription request failed"
                );
                if retry {
                    continue;
                }
                let message =
                    format!("voice transcription API returned {status}: {response_for_log}");
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(AiError::RateLimited(message));
                }
                return Err(AiError::InvalidResponse(message));
            }

            write_optional_ai_http_trace(
                &self.trace_dir,
                AiHttpTrace {
                    trace_id,
                    operation: "voice_transcription",
                    context: self.trace_context.as_ref(),
                    method: "POST",
                    endpoint: &endpoint,
                    model: Some(&self.model),
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    status: Some(status.as_u16()),
                    retry: false,
                    retry_after_ms: 0,
                    max_tokens: None,
                    request_body: Some(&trace_payload),
                    response_body: Some(&body),
                    error: None,
                },
            );
            let text = extract_transcription_text(&body, &self.config.response_format)?;
            info!(
                trace_id = %trace_id,
                output_chars = text.chars().count(),
                elapsed_ms,
                "voice transcription response parsed"
            );
            return Ok(text);
        }

        unreachable!("voice transcription retry loop always returns")
    }

    fn uses_stepfun_asr(&self) -> bool {
        let provider = self.config.provider.trim().to_ascii_lowercase();
        let base_url = self.base_url.to_ascii_lowercase();
        let model = self.model.to_ascii_lowercase();
        provider == "stepfun"
            || provider == "step"
            || model.contains("stepaudio")
            || base_url.contains("api.stepfun.com")
    }

    async fn transcribe_audio_stepfun(
        &self,
        path: &Path,
        bytes: Vec<u8>,
        file_name: String,
    ) -> Result<String, AiError> {
        let endpoint = stepfun_asr_endpoint(&self.base_url);
        let payload = stepfun_asr_payload(&self.model, &self.config, path, &bytes)?;
        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let permit = self.key_pool.acquire().await;
        let key_index = permit.key_index();
        let retry_budget = RetryBudget::new();

        for attempt in 1..=max_attempts {
            let trace_id = Uuid::new_v4();
            let started = Instant::now();
            info!(
                trace_id = %trace_id,
                base_url = %self.base_url,
                model = %self.model,
                key_index,
                key_count = self.key_pool.len(),
                file = %file_name,
                bytes = bytes.len(),
                timeout_seconds = self.config.timeout_seconds,
                attempt,
                max_attempts,
                "StepFun voice transcription request started"
            );
            let response = match self
                .client
                .post(&endpoint)
                .bearer_auth(permit.key())
                .header(ACCEPT, "text/event-stream")
                .header(CONTENT_TYPE, "application/json")
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    write_optional_ai_http_trace(
                        &self.trace_dir,
                        AiHttpTrace {
                            trace_id,
                            operation: "voice_transcription_stepfun",
                            context: self.trace_context.as_ref(),
                            method: "POST",
                            endpoint: &endpoint,
                            model: Some(&self.model),
                            attempt,
                            max_attempts,
                            elapsed_ms,
                            status: None,
                            retry,
                            retry_after_ms,
                            max_tokens: None,
                            request_body: Some(&payload),
                            response_body: None,
                            error: Some(&error.to_string()),
                        },
                    );
                    warn!(
                        trace_id = %trace_id,
                        elapsed_ms,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        error = %error,
                        "StepFun voice transcription transport failed"
                    );
                    if retry {
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };

            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let elapsed_ms = started.elapsed().as_millis();
            let body = response.text().await?;
            if !status.is_success() {
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "StepFun voice transcription request",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                write_optional_ai_http_trace(
                    &self.trace_dir,
                    AiHttpTrace {
                        trace_id,
                        operation: "voice_transcription_stepfun",
                        context: self.trace_context.as_ref(),
                        method: "POST",
                        endpoint: &endpoint,
                        model: Some(&self.model),
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: Some(status.as_u16()),
                        retry,
                        retry_after_ms,
                        max_tokens: None,
                        request_body: Some(&payload),
                        response_body: Some(&body),
                        error: None,
                    },
                );
                warn!(
                    trace_id = %trace_id,
                    status = %status,
                    elapsed_ms,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "StepFun voice transcription request failed"
                );
                if retry {
                    continue;
                }
                let message = format!(
                    "StepFun voice transcription API returned {status}: {response_for_log}"
                );
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(AiError::RateLimited(message));
                }
                return Err(AiError::InvalidResponse(message));
            }

            write_optional_ai_http_trace(
                &self.trace_dir,
                AiHttpTrace {
                    trace_id,
                    operation: "voice_transcription_stepfun",
                    context: self.trace_context.as_ref(),
                    method: "POST",
                    endpoint: &endpoint,
                    model: Some(&self.model),
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    status: Some(status.as_u16()),
                    retry: false,
                    retry_after_ms: 0,
                    max_tokens: None,
                    request_body: Some(&payload),
                    response_body: Some(&body),
                    error: None,
                },
            );
            let text = extract_stepfun_asr_text(&body)?;
            info!(
                trace_id = %trace_id,
                output_chars = text.chars().count(),
                elapsed_ms,
                "StepFun voice transcription response parsed"
            );
            return Ok(text);
        }

        unreachable!("StepFun voice transcription retry loop always returns")
    }
}

fn audio_transcriptions_endpoint(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/audio/transcriptions") {
        base.to_string()
    } else {
        format!("{base}/audio/transcriptions")
    }
}

fn stepfun_asr_endpoint(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/audio/asr/sse") {
        base.to_string()
    } else {
        format!("{base}/audio/asr/sse")
    }
}

fn stepfun_asr_payload(
    model: &str,
    config: &VoiceTranscriptionConfig,
    path: &Path,
    bytes: &[u8],
) -> Result<Value, AiError> {
    let mut transcription = Map::new();
    transcription.insert("model".into(), json!(model));
    if !config.language.trim().is_empty() {
        transcription.insert("language".into(), json!(config.language.trim()));
    }

    let format = stepfun_audio_format(path)?;
    let mut payload = json!({
        "audio": {
            "data": general_purpose::STANDARD.encode(bytes),
            "input": {
                "transcription": Value::Object(transcription),
                "format": format
            }
        }
    });
    apply_request_body_overrides(&mut payload, &config.request_body_overrides);
    Ok(payload)
}

fn stepfun_audio_format(path: &Path) -> Result<Value, AiError> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match extension.as_str() {
        "mp3" => Ok(json!({ "type": "mp3" })),
        "wav" => Ok(json!({ "type": "wav" })),
        "ogg" => Ok(json!({ "type": "ogg" })),
        "pcm" => Ok(json!({
            "type": "pcm",
            "codec": "pcm_s16le",
            "rate": 16000,
            "bits": 16,
            "channel": 1
        })),
        _ => Err(AiError::InvalidResponse(format!(
            "StepFun ASR supports ogg/mp3/wav/pcm audio, got '{}'; enable MP3 transcoding or configure voice_transcription.request_body_overrides.audio.input.format",
            extension
        ))),
    }
}

fn audio_mime_type(path: &Path) -> Option<&'static str> {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("mp3") => Some("audio/mpeg"),
        Some("wav") => Some("audio/wav"),
        Some("m4a") => Some("audio/mp4"),
        Some("aac") => Some("audio/aac"),
        Some("ogg") => Some("audio/ogg"),
        Some("flac") => Some("audio/flac"),
        Some("amr") => Some("audio/amr"),
        Some("webm") => Some("audio/webm"),
        _ => None,
    }
}

fn toml_value_to_multipart_string(value: &toml::Value) -> String {
    match value {
        toml::Value::String(value) => value.clone(),
        toml::Value::Integer(value) => value.to_string(),
        toml::Value::Float(value) => value.to_string(),
        toml::Value::Boolean(value) => value.to_string(),
        toml::Value::Datetime(value) => value.to_string(),
        toml::Value::Array(_) | toml::Value::Table(_) => value.to_string(),
    }
}

fn voice_transcription_multipart_trace_payload(
    client: &OpenAiAudioTranscriptionClient,
    file_name: &str,
    mime: Option<&str>,
    bytes: &[u8],
) -> Value {
    let mut fields = Map::new();
    fields.insert("model".to_string(), json!(client.model));
    fields.insert(
        "file".to_string(),
        json!({
            "file_name": file_name,
            "mime": mime,
            "media_type": mime,
            "encoded_length": general_purpose::STANDARD.encode(bytes).len(),
            "decoded_size": bytes.len(),
            "size_bytes": bytes.len(),
            "sha256": sha256_hex(bytes),
        }),
    );
    if !client.config.language.trim().is_empty() {
        fields.insert("language".to_string(), json!(client.config.language.trim()));
    }
    if !client.config.prompt.trim().is_empty() {
        fields.insert("prompt".to_string(), json!(client.config.prompt.trim()));
    }
    if !client.config.response_format.trim().is_empty() {
        fields.insert(
            "response_format".to_string(),
            json!(client.config.response_format.trim()),
        );
    }
    for (key, value) in &client.config.request_body_overrides {
        if key == "file" || key == "model" {
            continue;
        }
        fields.insert(key.clone(), json!(toml_value_to_multipart_string(value)));
    }
    Value::Object(fields)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn extract_transcription_text(body: &str, response_format: &str) -> Result<String, AiError> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Err(AiError::InvalidResponse(
            "empty voice transcription response".into(),
        ));
    }

    let format = response_format.trim().to_ascii_lowercase();
    if matches!(format.as_str(), "text" | "srt" | "vtt") {
        return Ok(trimmed.to_string());
    }

    match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => {
            if let Some(text) = value.get("text").and_then(Value::as_str) {
                let text = text.trim();
                if !text.is_empty() {
                    return Ok(text.to_string());
                }
            }
            if let Some(text) = value
                .pointer("/data/text")
                .and_then(Value::as_str)
                .or_else(|| value.pointer("/result/text").and_then(Value::as_str))
            {
                let text = text.trim();
                if !text.is_empty() {
                    return Ok(text.to_string());
                }
            }
            Err(AiError::InvalidResponse(
                "missing voice transcription text".into(),
            ))
        }
        Err(_) => Ok(trimmed.to_string()),
    }
}

fn extract_stepfun_asr_text(body: &str) -> Result<String, AiError> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Err(AiError::InvalidResponse(
            "empty StepFun voice transcription response".into(),
        ));
    }

    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|event_type| event_type == "error")
        {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("StepFun ASR error event");
            return Err(AiError::InvalidResponse(message.to_string()));
        }
        return extract_stepfun_asr_event_text(&value, &mut String::new())
            .ok_or_else(|| AiError::InvalidResponse("missing StepFun ASR text".into()));
    }

    let mut delta_text = String::new();
    let mut done_text = None;
    for event in stepfun_sse_json_events(trimmed) {
        if event
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|event_type| event_type == "error")
        {
            let message = event
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("StepFun ASR error event");
            return Err(AiError::InvalidResponse(message.to_string()));
        }
        if let Some(text) = extract_stepfun_asr_event_text(&event, &mut delta_text) {
            done_text = Some(text);
        }
    }

    done_text
        .or_else(|| (!delta_text.trim().is_empty()).then(|| delta_text.trim().to_string()))
        .ok_or_else(|| AiError::InvalidResponse("missing StepFun ASR text".into()))
}

fn stepfun_sse_json_events(body: &str) -> Vec<Value> {
    let mut events = Vec::new();
    let mut data_lines = Vec::new();
    for line in body.lines() {
        let line = line.trim_end();
        if line.is_empty() {
            push_stepfun_sse_event(&mut events, &mut data_lines);
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.trim_start().to_string());
        }
    }
    push_stepfun_sse_event(&mut events, &mut data_lines);
    events
}

fn push_stepfun_sse_event(events: &mut Vec<Value>, data_lines: &mut Vec<String>) {
    if data_lines.is_empty() {
        return;
    }
    let data = data_lines.join("\n");
    data_lines.clear();
    let trimmed = data.trim();
    if trimmed.is_empty() || trimmed == "[DONE]" {
        return;
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        events.push(value);
    }
}

fn extract_stepfun_asr_event_text(event: &Value, delta_text: &mut String) -> Option<String> {
    match event.get("type").and_then(Value::as_str) {
        Some("transcript.text.done") => event
            .get("text")
            .and_then(Value::as_str)
            .map(|text| text.trim().to_string())
            .filter(|text| !text.is_empty()),
        Some("transcript.text.delta") => {
            if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                delta_text.push_str(delta);
            }
            None
        }
        _ => event
            .get("text")
            .and_then(Value::as_str)
            .map(|text| text.trim().to_string())
            .filter(|text| !text.is_empty()),
    }
}

fn should_retry_chat_completion_failure(status: reqwest::StatusCode, body: &str) -> bool {
    should_retry_http_failure(status, body)
}

fn should_retry_http_failure(status: reqwest::StatusCode, body: &str) -> bool {
    if is_content_policy_block(status, body) {
        return false;
    }
    status.is_server_error()
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || body.contains("UPSTREAM_FAILED")
        || body.contains("UPSTREAM_REQUEST_FAILED")
}

fn is_content_policy_block(status: reqwest::StatusCode, body: &str) -> bool {
    if status == reqwest::StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS {
        return true;
    }
    let lower = body.to_ascii_lowercase();
    lower.contains("censorship_blocked")
        || lower.contains("content_filter")
        || lower.contains("content policy")
        || lower.contains("machine outputted is blocked")
        || lower.contains("content you provided")
}

fn should_retry_http_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect()
}

fn http_max_attempts(retry_attempts: usize) -> usize {
    retry_attempts.saturating_add(1)
}

fn http_retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(http_retry_delay_ms(attempt))
}

fn http_retry_delay_ms_for_status(
    status: reqwest::StatusCode,
    attempt: usize,
    server_retry_after_ms: Option<u64>,
) -> u64 {
    let exponential = http_retry_delay_ms(attempt);
    let base = if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        exponential.max(server_retry_after_ms.unwrap_or(0))
    } else {
        exponential
    };
    base.saturating_add(deterministic_retry_jitter_ms(base, attempt))
        .min(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS)
}

fn deterministic_retry_jitter_ms(base_ms: u64, attempt: usize) -> u64 {
    let window = (base_ms / 5).min(5_000);
    if window == 0 {
        return 0;
    }
    let seed = (attempt as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    seed % (window + 1)
}

fn retry_after_ms_from_headers(headers: &HeaderMap, now: chrono::DateTime<Utc>) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds.saturating_mul(1_000));
    }
    let retry_at = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    let millis = retry_at
        .with_timezone(&Utc)
        .signed_duration_since(now)
        .num_milliseconds();
    Some(millis.max(0) as u64)
}

async fn sleep_for_retry_budget(retry_budget: &RetryBudget, delay: Duration) -> bool {
    let remaining = retry_budget.remaining_duration();
    if delay > remaining {
        if !remaining.is_zero() {
            sleep(remaining).await;
        }
        return false;
    }
    sleep(delay).await;
    true
}

fn http_retry_delay_ms(attempt: usize) -> u64 {
    let mut delay = HTTP_RETRY_INITIAL_DELAY_MS;
    for _ in 1..attempt {
        delay = delay.saturating_mul(2).min(HTTP_RETRY_MAX_DELAY_MS);
        if delay == HTTP_RETRY_MAX_DELAY_MS {
            break;
        }
    }
    delay
}

async fn wait_before_status_retry(
    operation: &'static str,
    status: reqwest::StatusCode,
    attempt: usize,
    max_attempts: usize,
    retry_budget: &RetryBudget,
    delay_ms: u64,
) -> Option<u64> {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let queue = HTTP_RATE_LIMIT_RETRY_QUEUE.get_or_init(|| Mutex::new(()));
        wait_in_rate_limit_retry_queue(
            queue,
            operation,
            attempt,
            max_attempts,
            retry_budget,
            delay_ms,
        )
        .await
    } else {
        let wait_ms = delay_ms.min(retry_budget.remaining_ms());
        if wait_ms == 0 {
            return None;
        }
        sleep(Duration::from_millis(wait_ms)).await;
        Some(wait_ms)
    }
}

async fn wait_in_rate_limit_retry_queue(
    queue: &Mutex<()>,
    operation: &'static str,
    attempt: usize,
    max_attempts: usize,
    retry_budget: &RetryBudget,
    delay_ms: u64,
) -> Option<u64> {
    let remaining_ms = retry_budget.remaining_ms();
    if remaining_ms == 0 {
        return None;
    }
    let Ok(_guard) = tokio::time::timeout(Duration::from_millis(remaining_ms), queue.lock()).await
    else {
        info!(
            operation,
            attempt,
            max_attempts,
            wait_ms = 0,
            "AI request rate limit retry budget exhausted in serial retry queue"
        );
        return None;
    };

    let wait_ms = delay_ms.min(retry_budget.remaining_ms());
    if wait_ms == 0 {
        return None;
    }
    info!(
        operation,
        attempt, max_attempts, wait_ms, "AI request rate limited; waiting in serial retry queue"
    );
    sleep(Duration::from_millis(wait_ms)).await;
    Some(wait_ms)
}

async fn notify_retry(
    notifier: &Option<RetryNotifier>,
    operation: &'static str,
    attempt: usize,
    max_attempts: usize,
    retry_after_ms: u64,
    reason: String,
) {
    if let Some(notifier) = notifier {
        notifier(AiRetryNotice {
            operation,
            attempt,
            max_attempts,
            retry_after_ms,
            reason,
        })
        .await;
    }
}

fn retry_reason_from_transport_error(error: &reqwest::Error) -> String {
    truncate_for_log(&error.to_string(), 300)
}

fn retry_reason_from_status(status: reqwest::StatusCode, snippet: &str) -> String {
    truncate_for_log(&format!("{status}: {snippet}"), 300)
}

fn chat_completion_payload(
    model: &str,
    system_prompt: &str,
    user_content: &str,
    temperature: f32,
    max_tokens: Option<u32>,
) -> Value {
    let mut payload = serde_json::json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ],
        "temperature": temperature,
    });
    if let Some(max_tokens) = max_tokens {
        if let Some(payload_object) = payload.as_object_mut() {
            payload_object.insert("max_tokens".into(), json!(max_tokens));
        }
    }
    payload
}

fn image_caption_payload(
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
    image_url: &str,
    temperature: f32,
    max_tokens: u32,
) -> Value {
    serde_json::json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_prompt},
                    {"type": "image_url", "image_url": {"url": image_url}}
                ]
            }
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    })
}

fn video_caption_payload(
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
    video_url: &str,
    temperature: f32,
    max_tokens: u32,
) -> Value {
    json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_prompt},
                    {"type": "video_url", "video_url": {"url": video_url}}
                ]
            }
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    })
}

fn image_data_url_from_bytes(path: Option<&Path>, bytes: &[u8]) -> Result<String, AiError> {
    if let Some(mime) = image_mime_type(path, bytes) {
        let encoded = general_purpose::STANDARD.encode(bytes);
        return Ok(format!("data:{mime};base64,{encoded}"));
    }
    convert_image_to_jpeg_data_url(path, bytes)
}

fn convert_image_to_jpeg_data_url(path: Option<&Path>, bytes: &[u8]) -> Result<String, AiError> {
    let source = image_source_label(path);
    let decoded = image::load_from_memory(bytes).map_err(|error| {
        AiError::InvalidResponse(format!(
            "unsupported image format for captioning: {source}; jpeg fallback failed: {error}"
        ))
    })?;
    let rgb = decoded.to_rgb8();
    let (width, height) = rgb.dimensions();
    let mut encoded = Vec::new();
    let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut encoded, 90);
    encoder
        .encode(
            &rgb,
            width,
            height,
            image::ExtendedColorType::Rgb8,
        )
        .map_err(|error| {
            AiError::InvalidResponse(format!(
                "unsupported image format for captioning: {source}; jpeg fallback encode failed: {error}"
            ))
        })?;
    info!(
        source = %source,
        input_bytes = bytes.len(),
        output_bytes = encoded.len(),
        width,
        height,
        "image caption unsupported image converted to jpeg"
    );
    Ok(format!(
        "data:image/jpeg;base64,{}",
        general_purpose::STANDARD.encode(encoded)
    ))
}

fn image_source_label(path: Option<&Path>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_else(|| "downloaded image".to_string())
}

fn validate_image_data_url(data_url: &str) -> Result<(), AiError> {
    let Some((metadata, _payload)) = data_url.split_once(',') else {
        return Err(AiError::InvalidResponse(
            "invalid image data URL for captioning".into(),
        ));
    };
    let metadata = metadata.to_ascii_lowercase();
    let supported = metadata == "data:image/jpeg;base64"
        || metadata == "data:image/jpg;base64"
        || metadata == "data:image/png;base64"
        || metadata == "data:image/gif;base64"
        || metadata == "data:image/webp;base64";
    if supported {
        Ok(())
    } else {
        Err(AiError::InvalidResponse(format!(
            "unsupported image data URL media type for captioning: {metadata}"
        )))
    }
}

fn video_data_url_from_bytes(
    path: Option<&Path>,
    bytes: &[u8],
    max_video_bytes: u64,
) -> Result<String, AiError> {
    validate_video_byte_len(bytes.len(), max_video_bytes)?;
    let mime = video_mime_type(path, bytes).ok_or_else(|| {
        AiError::InvalidResponse(format!(
            "unsupported video format for captioning: {}",
            video_source_label(path)
        ))
    })?;
    Ok(format!(
        "data:{mime};base64,{}",
        general_purpose::STANDARD.encode(bytes)
    ))
}

fn validate_video_data_url(data_url: &str, max_video_bytes: u64) -> Result<(), AiError> {
    let Some((metadata, payload)) = data_url.split_once(',') else {
        return Err(AiError::InvalidResponse(
            "invalid video data URL for captioning".into(),
        ));
    };
    let metadata = metadata.to_ascii_lowercase();
    let supported = metadata == "data:video/mp4;base64"
        || metadata == "data:video/quicktime;base64"
        || metadata == "data:video/x-matroska;base64"
        || metadata == "data:video/webm;base64";
    if !supported {
        return Err(AiError::InvalidResponse(format!(
            "unsupported video data URL media type for captioning: {metadata}"
        )));
    }

    validate_video_byte_len(estimated_base64_decoded_len(payload), max_video_bytes)
}

fn estimated_base64_decoded_len(payload: &str) -> usize {
    let trimmed = payload.trim();
    let non_padding = trimmed
        .as_bytes()
        .iter()
        .filter(|byte| !byte.is_ascii_whitespace() && **byte != b'=')
        .count();
    non_padding.saturating_mul(3) / 4
}

fn validate_video_byte_len(len: usize, max_video_bytes: u64) -> Result<(), AiError> {
    if max_video_bytes == 0 || len as u64 <= max_video_bytes {
        return Ok(());
    }
    Err(AiError::InvalidResponse(format!(
        "video caption source is too large: {} bytes > configured max_video_bytes {}",
        len, max_video_bytes
    )))
}

fn video_source_label(path: Option<&Path>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_else(|| "downloaded video".to_string())
}

fn image_mime_type(path: Option<&Path>, bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 3 && bytes[..3] == [0xff, 0xd8, 0xff] {
        return Some("image/jpeg");
    }
    if bytes.len() >= 4 && bytes[..4] == [0x89, 0x50, 0x4e, 0x47] {
        return Some("image/png");
    }
    if bytes.len() >= 3 && &bytes[..3] == b"GIF" {
        return gif_is_static(bytes).then_some("image/gif");
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    match path?
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "gif" if gif_is_static(bytes) => Some("image/gif"),
        "webp" => Some("image/webp"),
        _ => None,
    }
}

fn video_mime_type(path: Option<&Path>, bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        let brand = &bytes[8..12];
        if brand.starts_with(b"qt") {
            return Some("video/quicktime");
        }
        return Some("video/mp4");
    }
    if bytes.len() >= 4 && bytes[..4] == [0x1a, 0x45, 0xdf, 0xa3] {
        return Some("video/x-matroska");
    }
    if bytes.len() >= 4 && &bytes[..4] == b"RIFF" && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
        return None;
    }
    match path?
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "mp4" | "m4v" => Some("video/mp4"),
        "mov" => Some("video/quicktime"),
        "mkv" => Some("video/x-matroska"),
        "webm" => Some("video/webm"),
        _ => None,
    }
}

fn gif_is_static(bytes: &[u8]) -> bool {
    if bytes.len() < 13 || (!bytes.starts_with(b"GIF87a") && !bytes.starts_with(b"GIF89a")) {
        return false;
    }
    let mut index = 13usize;
    let packed = bytes[10];
    if packed & 0x80 != 0 {
        index = index.saturating_add(3 * (1usize << ((packed & 0x07) + 1)));
    }

    let mut frames = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            0x2c => {
                frames += 1;
                if frames > 1 {
                    return false;
                }
                index += 10;
                if index > bytes.len() {
                    return false;
                }
                let image_packed = bytes[index - 1];
                if image_packed & 0x80 != 0 {
                    index = index.saturating_add(3 * (1usize << ((image_packed & 0x07) + 1)));
                }
                if index >= bytes.len() {
                    return false;
                }
                index += 1;
                if !skip_gif_sub_blocks(bytes, &mut index) {
                    return false;
                }
            }
            0x21 => {
                index += 2;
                if !skip_gif_sub_blocks(bytes, &mut index) {
                    return false;
                }
            }
            0x3b => break,
            _ => return false,
        }
    }
    frames <= 1
}

fn skip_gif_sub_blocks(bytes: &[u8], index: &mut usize) -> bool {
    loop {
        let Some(size) = bytes.get(*index).copied() else {
            return false;
        };
        *index += 1;
        if size == 0 {
            return true;
        }
        *index = (*index).saturating_add(size as usize);
        if *index > bytes.len() {
            return false;
        }
    }
}

fn apply_request_body_overrides(
    payload: &mut Value,
    overrides: &std::collections::BTreeMap<String, toml::Value>,
) {
    if overrides.is_empty() {
        return;
    }
    let Some(payload_object) = payload.as_object_mut() else {
        return;
    };
    let Ok(Value::Object(override_object)) = serde_json::to_value(overrides) else {
        return;
    };
    for (key, value) in override_object {
        match payload_object.get_mut(&key) {
            Some(existing) => merge_json_override(existing, value),
            None => {
                payload_object.insert(key, value);
            }
        }
    }
}

fn merge_json_override(target: &mut Value, override_value: Value) {
    match (target, override_value) {
        (Value::Object(target), Value::Object(overrides)) => {
            for (key, value) in overrides {
                match target.get_mut(&key) {
                    Some(existing) => merge_json_override(existing, value),
                    None => {
                        target.insert(key, value);
                    }
                }
            }
        }
        (target, override_value) => *target = override_value,
    }
}

#[derive(Clone)]
pub struct OpenAiImageClient {
    config: ImageGenConfig,
    client: reqwest::Client,
    key_pool: Arc<ApiKeyPool>,
    base_url: String,
    model: String,
    retry_notifier: Option<RetryNotifier>,
    trace_dir: Option<PathBuf>,
    trace_context: Option<AiTraceContext>,
}

impl OpenAiImageClient {
    pub fn new(config: ImageGenConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let keys = resolve_api_keys(
            &config.api_keys,
            config.api_key.as_deref(),
            &config.api_keys_env,
            &config.api_key_env,
            "image API key",
        )?;
        let base_url = config_value_or_env(
            config.base_url.as_deref(),
            &config.base_url_env,
            "image API base URL",
        )?;
        let model = config_value_or_env(
            config.model.as_deref(),
            &config.model_env,
            "image model name",
        )?;
        let client = http_client(config.timeout_seconds, proxy)?;
        let max_concurrent_per_key = config.max_concurrent_per_key;
        Ok(Self {
            config,
            client,
            key_pool: shared_key_pool(keys, max_concurrent_per_key),
            base_url,
            model,
            retry_notifier: None,
            trace_dir: None,
            trace_context: None,
        })
    }

    pub fn with_retry_notifier(mut self, retry_notifier: RetryNotifier) -> Self {
        self.retry_notifier = Some(retry_notifier);
        self
    }

    pub fn with_trace_dir(mut self, trace_dir: impl Into<PathBuf>) -> Self {
        self.trace_dir = Some(trace_dir.into());
        self
    }

    pub fn with_trace_context(mut self, trace_context: AiTraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    pub async fn generate(
        &self,
        summary: &str,
        output_dir: impl AsRef<Path>,
    ) -> Result<ImageArtifact, AiError> {
        let prompt = self
            .config
            .prompt_template
            .as_deref()
            .unwrap_or("{summary}")
            .replace("{summary}", summary);
        self.generate_from_prompt(&prompt, output_dir).await
    }

    pub async fn generate_from_prompt(
        &self,
        prompt: &str,
        output_dir: impl AsRef<Path>,
    ) -> Result<ImageArtifact, AiError> {
        let endpoint = image_generations_endpoint(&self.base_url);
        let payload = self.image_generation_payload(prompt);

        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let permit = self.key_pool.acquire().await;
        let key_index = permit.key_index();
        let retry_budget = RetryBudget::new();
        for attempt in 1..=max_attempts {
            let trace_id = Uuid::new_v4();
            let started = Instant::now();
            info!(
                trace_id = %trace_id,
                base_url = %self.base_url,
                model = %self.model,
                key_index,
                key_count = self.key_pool.len(),
                prompt_chars = prompt.chars().count(),
                size = %self.config.size,
                quality = ?self.config.quality,
                resolution = ?self.config.resolution,
                timeout_seconds = self.config.timeout_seconds,
                attempt,
                max_attempts,
                "image generation request started"
            );
            let response = match self
                .client
                .post(&endpoint)
                .bearer_auth(permit.key())
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    write_optional_ai_http_trace(
                        &self.trace_dir,
                        AiHttpTrace {
                            trace_id,
                            operation: "image_generation",
                            context: self.trace_context.as_ref(),
                            method: "POST",
                            endpoint: &endpoint,
                            model: Some(&self.model),
                            attempt,
                            max_attempts,
                            elapsed_ms,
                            status: None,
                            retry,
                            retry_after_ms,
                            max_tokens: None,
                            request_body: Some(&payload),
                            response_body: None,
                            error: Some(&error.to_string()),
                        },
                    );
                    warn!(
                        trace_id = %trace_id,
                        elapsed_ms,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        error = %error,
                        "image generation transport failed"
                    );
                    if retry {
                        notify_retry(
                            &self.retry_notifier,
                            "image generation request",
                            attempt,
                            max_attempts,
                            retry_after_ms,
                            retry_reason_from_transport_error(&error),
                        )
                        .await;
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };
            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let elapsed_ms = started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_http_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "image generation request",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                write_optional_ai_http_trace(
                    &self.trace_dir,
                    AiHttpTrace {
                        trace_id,
                        operation: "image_generation",
                        context: self.trace_context.as_ref(),
                        method: "POST",
                        endpoint: &endpoint,
                        model: Some(&self.model),
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: Some(status.as_u16()),
                        retry,
                        retry_after_ms,
                        max_tokens: None,
                        request_body: Some(&payload),
                        response_body: Some(&body),
                        error: None,
                    },
                );
                warn!(
                    trace_id = %trace_id,
                    status = %status,
                    elapsed_ms,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "image generation request failed"
                );
                if retry {
                    notify_retry(
                        &self.retry_notifier,
                        "image generation request",
                        attempt,
                        max_attempts,
                        retry_after_ms,
                        retry_reason_from_status(status, &snippet),
                    )
                    .await;
                    continue;
                }
                return Err(AiError::InvalidResponse(format!(
                    "image generation API returned {status}: {response_for_log}"
                )));
            }
            info!(
                trace_id = %trace_id,
                status = %status,
                elapsed_ms,
                attempt,
                max_attempts,
                "image generation HTTP request completed"
            );

            let body = response.text().await?;
            write_optional_ai_http_trace(
                &self.trace_dir,
                AiHttpTrace {
                    trace_id,
                    operation: "image_generation",
                    context: self.trace_context.as_ref(),
                    method: "POST",
                    endpoint: &endpoint,
                    model: Some(&self.model),
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    status: Some(status.as_u16()),
                    retry: false,
                    retry_after_ms: 0,
                    max_tokens: None,
                    request_body: Some(&payload),
                    response_body: Some(&body),
                    error: None,
                },
            );
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let response_for_log = full_response_for_log(&body);
                warn!(
                    trace_id = %trace_id,
                    response = %response_for_log,
                    "image generation JSON parsing failed"
                );
                AiError::InvalidResponse(format!(
                    "invalid image generation JSON: {error}; response={response_for_log}"
                ))
            })?;
            let bytes = self
                .image_bytes_from_generation_response(response, permit.key())
                .await?;
            return self.write_image(output_dir, &bytes);
        }

        unreachable!("image generation retry loop always returns")
    }

    fn image_generation_payload(&self, prompt: &str) -> Value {
        let mut payload = Map::new();
        payload.insert("model".into(), json!(self.model));
        payload.insert("prompt".into(), json!(prompt));
        payload.insert("n".into(), json!(1));

        if !self.config.size.trim().is_empty() {
            payload.insert("size".into(), json!(self.config.size));
        }
        if let Some(resolution) = self
            .config
            .resolution
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            payload.insert("resolution".into(), json!(resolution));
        } else if let Some(quality) = self
            .config
            .quality
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            payload.insert("quality".into(), json!(quality));
        }
        if self.config.official_fallback {
            payload.insert("official_fallback".into(), json!(true));
        }

        Value::Object(payload)
    }

    async fn image_bytes_from_generation_response(
        &self,
        response: Value,
        api_key: &str,
    ) -> Result<Vec<u8>, AiError> {
        if let Some(image) = direct_image_data(&response) {
            info!("image generation returned direct image data");
            return self.image_bytes_from_data(image).await;
        }
        if let Some(task_id) = apimart_task_id(&response) {
            info!(task_id = %task_id, "image generation task accepted; polling started");
            return self.poll_apimart_task(&task_id, api_key).await;
        }
        Err(AiError::InvalidResponse(format!(
            "image response has neither direct image data nor task_id: {response}"
        )))
    }

    async fn poll_apimart_task(&self, task_id: &str, api_key: &str) -> Result<Vec<u8>, AiError> {
        let deadline = Instant::now() + Duration::from_secs(self.config.timeout_seconds.max(1));
        let retry_budget = RetryBudget::with_deadline(deadline);
        info!(
            task_id = %task_id,
            initial_delay_seconds = self.config.poll_initial_delay_seconds.min(self.config.timeout_seconds),
            poll_interval_seconds = self.config.poll_interval_seconds.max(1),
            "image task initial poll delay started"
        );
        let initial_delay = Duration::from_secs(
            self.config
                .poll_initial_delay_seconds
                .min(self.config.timeout_seconds),
        );
        if !sleep_for_retry_budget(&retry_budget, initial_delay).await {
            return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
        }

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
            let response = {
                let mut final_response = None;
                for http_attempt in 1..=max_attempts {
                    let trace_id = Uuid::new_v4();
                    let poll_started = Instant::now();
                    let endpoint = format!("{}/tasks/{}", api_root_url(&self.base_url), task_id);
                    let request_body = json!({ "task_id": task_id });
                    let remaining = retry_budget.remaining_duration();
                    if remaining.is_zero() {
                        return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
                    }
                    let response = match timeout(
                        remaining,
                        self.client.get(&endpoint).bearer_auth(api_key).send(),
                    )
                    .await
                    {
                        Err(_) => {
                            return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
                        }
                        Ok(Ok(response)) => response,
                        Ok(Err(error)) => {
                            let elapsed_ms = poll_started.elapsed().as_millis();
                            let retry = http_attempt < max_attempts
                                && should_retry_http_transport_error(&error)
                                && retry_budget.allows(http_retry_delay_ms(http_attempt));
                            let retry_after_ms = if retry {
                                http_retry_delay_ms(http_attempt)
                            } else {
                                0
                            };
                            write_optional_ai_http_trace(
                                &self.trace_dir,
                                AiHttpTrace {
                                    trace_id,
                                    operation: "image_task_poll",
                                    context: self.trace_context.as_ref(),
                                    method: "GET",
                                    endpoint: &endpoint,
                                    model: Some(&self.model),
                                    attempt: http_attempt,
                                    max_attempts,
                                    elapsed_ms,
                                    status: None,
                                    retry,
                                    retry_after_ms,
                                    max_tokens: None,
                                    request_body: Some(&request_body),
                                    response_body: None,
                                    error: Some(&error.to_string()),
                                },
                            );
                            warn!(
                                trace_id = %trace_id,
                                task_id = %task_id,
                                attempt,
                                http_attempt,
                                max_attempts,
                                retry,
                                retry_after_ms,
                                elapsed_ms,
                                error = %error,
                                "image task poll transport failed"
                            );
                            if retry {
                                notify_retry(
                                    &self.retry_notifier,
                                    "image task poll",
                                    http_attempt,
                                    max_attempts,
                                    retry_after_ms,
                                    retry_reason_from_transport_error(&error),
                                )
                                .await;
                                if !sleep_for_retry_budget(
                                    &retry_budget,
                                    http_retry_delay(http_attempt),
                                )
                                .await
                                {
                                    return Err(AiError::ImageTaskTimeout(
                                        self.config.timeout_seconds,
                                    ));
                                }
                                continue;
                            }
                            return Err(AiError::Http(error));
                        }
                    };
                    let status = response.status();
                    let server_retry_after_ms =
                        retry_after_ms_from_headers(response.headers(), Utc::now());
                    let elapsed_ms = poll_started.elapsed().as_millis();
                    if !status.is_success() {
                        let body = match timeout(retry_budget.remaining_duration(), response.text())
                            .await
                        {
                            Ok(Ok(body)) => body,
                            Ok(Err(error)) => {
                                format!("failed to read error response body: {error}")
                            }
                            Err(_) => {
                                return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
                            }
                        };
                        let snippet = truncate_for_log(&body, 500);
                        let response_for_log = full_response_for_log(&body);
                        let retry_candidate = http_attempt < max_attempts
                            && should_retry_http_failure(status, &snippet)
                            && retry_budget.allows(http_retry_delay_ms_for_status(
                                status,
                                http_attempt,
                                server_retry_after_ms,
                            ));
                        let retry_delay_ms = if retry_candidate {
                            http_retry_delay_ms_for_status(
                                status,
                                http_attempt,
                                server_retry_after_ms,
                            )
                        } else {
                            0
                        };
                        let retry_after_ms = if retry_candidate {
                            wait_before_status_retry(
                                "image task poll",
                                status,
                                http_attempt,
                                max_attempts,
                                &retry_budget,
                                retry_delay_ms,
                            )
                            .await
                            .unwrap_or(0)
                        } else {
                            0
                        };
                        if retry_candidate
                            && retry_after_ms == 0
                            && (retry_budget.deadline_exhausted()
                                || retry_budget.remaining_duration().is_zero())
                        {
                            return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
                        }
                        let retry = retry_after_ms > 0;
                        write_optional_ai_http_trace(
                            &self.trace_dir,
                            AiHttpTrace {
                                trace_id,
                                operation: "image_task_poll",
                                context: self.trace_context.as_ref(),
                                method: "GET",
                                endpoint: &endpoint,
                                model: Some(&self.model),
                                attempt: http_attempt,
                                max_attempts,
                                elapsed_ms,
                                status: Some(status.as_u16()),
                                retry,
                                retry_after_ms,
                                max_tokens: None,
                                request_body: Some(&request_body),
                                response_body: Some(&body),
                                error: None,
                            },
                        );
                        warn!(
                            trace_id = %trace_id,
                            task_id = %task_id,
                            attempt,
                            http_attempt,
                            max_attempts,
                            retry,
                            retry_after_ms,
                            status = %status,
                            elapsed_ms,
                            response = %response_for_log,
                            "image task poll failed"
                        );
                        if retry {
                            notify_retry(
                                &self.retry_notifier,
                                "image task poll",
                                http_attempt,
                                max_attempts,
                                retry_after_ms,
                                retry_reason_from_status(status, &snippet),
                            )
                            .await;
                            continue;
                        }
                        return Err(AiError::InvalidResponse(format!(
                            "image task API returned {status}: {response_for_log}"
                        )));
                    }
                    let body =
                        match timeout(retry_budget.remaining_duration(), response.text()).await {
                            Ok(body) => body?,
                            Err(_) => {
                                return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
                            }
                        };
                    write_optional_ai_http_trace(
                        &self.trace_dir,
                        AiHttpTrace {
                            trace_id,
                            operation: "image_task_poll",
                            context: self.trace_context.as_ref(),
                            method: "GET",
                            endpoint: &endpoint,
                            model: Some(&self.model),
                            attempt: http_attempt,
                            max_attempts,
                            elapsed_ms,
                            status: Some(status.as_u16()),
                            retry: false,
                            retry_after_ms: 0,
                            max_tokens: None,
                            request_body: Some(&request_body),
                            response_body: Some(&body),
                            error: None,
                        },
                    );
                    final_response = Some((body, elapsed_ms, http_attempt, trace_id));
                    break;
                }
                final_response
                    .expect("image task poll retry loop always returns a response or error")
            };
            let (body, elapsed_ms, http_attempt, trace_id) = response;
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let response_for_log = full_response_for_log(&body);
                warn!(
                    trace_id = %trace_id,
                    task_id = %task_id,
                    response = %response_for_log,
                    "image task poll JSON parsing failed"
                );
                AiError::InvalidResponse(format!(
                    "invalid image task JSON: {error}; response={response_for_log}"
                ))
            })?;
            info!(
                trace_id = %trace_id,
                task_id = %task_id,
                attempt,
                http_attempt,
                status = %apimart_task_status(&response),
                elapsed_ms,
                "image task poll completed"
            );

            if let Some(url) = apimart_completed_image_url(&response)? {
                info!(task_id = %task_id, "image task completed; downloading image");
                return self.download_image(url).await;
            }

            if Instant::now() >= deadline {
                return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
            }
            if !sleep_for_retry_budget(
                &retry_budget,
                Duration::from_secs(self.config.poll_interval_seconds.max(1)),
            )
            .await
            {
                return Err(AiError::ImageTaskTimeout(self.config.timeout_seconds));
            }
        }
    }

    async fn image_bytes_from_data(&self, image: ImageData) -> Result<Vec<u8>, AiError> {
        if let Some(b64) = image.b64_json {
            general_purpose::STANDARD.decode(b64).map_err(AiError::from)
        } else if let Some(url) = image.url {
            self.download_image(url).await
        } else {
            Err(AiError::InvalidResponse(
                "image response has neither b64_json nor url".into(),
            ))
        }
    }

    async fn download_image(&self, url: String) -> Result<Vec<u8>, AiError> {
        let source = redacted_url_source(&url);
        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        let retry_budget = RetryBudget::new();
        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(source = %source, attempt, max_attempts, "image download started");
            let response = match self.client.get(&url).send().await {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts
                        && should_retry_http_transport_error(&error)
                        && retry_budget.allows(http_retry_delay_ms(attempt));
                    let retry_after_ms = if retry {
                        http_retry_delay_ms(attempt)
                    } else {
                        0
                    };
                    warn!(
                        source = %source,
                        attempt,
                        max_attempts,
                        retry,
                        retry_after_ms,
                        elapsed_ms = started.elapsed().as_millis(),
                        error = %error,
                        "image download transport failed"
                    );
                    if retry {
                        notify_retry(
                            &self.retry_notifier,
                            "image download",
                            attempt,
                            max_attempts,
                            retry_after_ms,
                            retry_reason_from_transport_error(&error),
                        )
                        .await;
                        sleep(http_retry_delay(attempt)).await;
                        continue;
                    }
                    return Err(AiError::Http(error));
                }
            };
            let status = response.status();
            let server_retry_after_ms = retry_after_ms_from_headers(response.headers(), Utc::now());
            let elapsed_ms = started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry_candidate = attempt < max_attempts
                    && should_retry_http_failure(status, &snippet)
                    && retry_budget.allows(http_retry_delay_ms_for_status(
                        status,
                        attempt,
                        server_retry_after_ms,
                    ));
                let retry_delay_ms = if retry_candidate {
                    http_retry_delay_ms_for_status(status, attempt, server_retry_after_ms)
                } else {
                    0
                };
                let retry_after_ms = if retry_candidate {
                    wait_before_status_retry(
                        "image download",
                        status,
                        attempt,
                        max_attempts,
                        &retry_budget,
                        retry_delay_ms,
                    )
                    .await
                    .unwrap_or(0)
                } else {
                    0
                };
                let retry = retry_after_ms > 0;
                warn!(
                    source = %source,
                    status = %status,
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    elapsed_ms,
                    response = %response_for_log,
                    "image download failed"
                );
                if retry {
                    notify_retry(
                        &self.retry_notifier,
                        "image download",
                        attempt,
                        max_attempts,
                        retry_after_ms,
                        retry_reason_from_status(status, &snippet),
                    )
                    .await;
                    continue;
                }
                return Err(AiError::InvalidResponse(format!(
                    "image download returned {status}: {response_for_log}"
                )));
            }
            let bytes = response.bytes().await?.to_vec();
            info!(
                source = %source,
                elapsed_ms,
                attempt,
                max_attempts,
                bytes = bytes.len(),
                "image download completed"
            );
            return Ok(bytes);
        }

        unreachable!("image download retry loop always returns")
    }

    fn write_image(
        &self,
        output_dir: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<ImageArtifact, AiError> {
        fs::create_dir_all(output_dir.as_ref())?;
        let filename = format!("summary-{}.png", Uuid::new_v4());
        let path = output_dir.as_ref().join(filename);
        fs::write(&path, bytes)?;

        let artifact = ImageArtifact {
            path: path.to_string_lossy().to_string(),
            mime_type: "image/png".to_string(),
            size_bytes: bytes.len() as u64,
            sha256: sha256_hex(bytes),
        };
        info!(
            path = %artifact.path,
            size_bytes = artifact.size_bytes,
            "summary image written"
        );
        Ok(artifact)
    }
}

async fn send_chat_completion_request(
    request: reqwest::RequestBuilder,
    first_event_timeout: Option<Duration>,
) -> Result<reqwest::Response, ChatCompletionRequestError> {
    match first_event_timeout {
        Some(timeout_duration) => match timeout(timeout_duration, request.send()).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(ChatCompletionRequestError::Http(error)),
            Err(_) => Err(ChatCompletionRequestError::FirstEventTimeout(
                timeout_duration.as_secs(),
            )),
        },
        None => request
            .send()
            .await
            .map_err(ChatCompletionRequestError::Http),
    }
}

struct StreamedChatCompletion {
    raw_body: String,
    response: Value,
}

struct SseChatCompletionAccumulator {
    raw_body: Vec<u8>,
    pending: Vec<u8>,
    content: String,
    finish_reason: Option<String>,
    received_data: bool,
    completed: bool,
}

impl SseChatCompletionAccumulator {
    fn new() -> Self {
        Self {
            raw_body: Vec::new(),
            pending: Vec::new(),
            content: String::new(),
            finish_reason: None,
            received_data: false,
            completed: false,
        }
    }

    fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), ChatCompletionStreamError> {
        self.raw_body.extend_from_slice(chunk);
        self.pending.extend_from_slice(chunk);

        while let Some(frame_end) = sse_frame_end(&self.pending) {
            let frame = self.pending.drain(..frame_end).collect::<Vec<_>>();
            self.process_frame(&frame)?;
            if self.completed {
                break;
            }
        }
        Ok(())
    }

    fn process_frame(&mut self, frame: &[u8]) -> Result<(), ChatCompletionStreamError> {
        let frame = std::str::from_utf8(frame).map_err(|error| {
            ChatCompletionStreamError::InvalidEvent(format!("frame is not UTF-8: {error}"))
        })?;
        let data = frame
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(|line| line.strip_prefix(' ').unwrap_or(line))
            .collect::<Vec<_>>();
        if data.is_empty() {
            return Ok(());
        }

        self.received_data = true;
        let data = data.join("\n");
        if data.trim() == "[DONE]" {
            self.completed = true;
            return Ok(());
        }

        let event: Value = serde_json::from_str(&data).map_err(|error| {
            ChatCompletionStreamError::InvalidEvent(format!("invalid JSON data payload: {error}"))
        })?;
        if let Some(content) = content_value_to_text(event.pointer("/choices/0/delta/content")) {
            self.content.push_str(&content);
        }
        if let Some(finish_reason) = event
            .pointer("/choices/0/finish_reason")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            self.finish_reason = Some(finish_reason.to_string());
        }
        Ok(())
    }

    fn into_streamed_response(self) -> StreamedChatCompletion {
        StreamedChatCompletion {
            raw_body: String::from_utf8_lossy(&self.raw_body).into_owned(),
            response: json!({
                "choices": [{
                    "message": { "content": self.content },
                    "finish_reason": self.finish_reason.unwrap_or_else(|| "stop".to_string()),
                }]
            }),
        }
    }
}

async fn collect_streamed_chat_completion(
    mut response: reqwest::Response,
    first_event_timeout: Duration,
    idle_timeout: Duration,
) -> Result<StreamedChatCompletion, ChatCompletionStreamError> {
    let mut accumulator = SseChatCompletionAccumulator::new();
    loop {
        let wait_timeout = if accumulator.received_data {
            idle_timeout
        } else {
            first_event_timeout
        };
        let chunk = match timeout(wait_timeout, response.chunk()).await {
            Ok(Ok(Some(chunk))) => chunk,
            Ok(Ok(None)) => return Err(ChatCompletionStreamError::EndedWithoutDone),
            Ok(Err(error)) => return Err(ChatCompletionStreamError::Read(error)),
            Err(_) if accumulator.received_data => {
                return Err(ChatCompletionStreamError::IdleTimeout(
                    idle_timeout.as_secs(),
                ))
            }
            Err(_) => {
                return Err(ChatCompletionStreamError::FirstEventTimeout(
                    first_event_timeout.as_secs(),
                ))
            }
        };
        accumulator.push_chunk(&chunk)?;
        if accumulator.completed {
            return Ok(accumulator.into_streamed_response());
        }
    }
}

fn response_is_sse(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type.trim().eq_ignore_ascii_case("text/event-stream")
            })
        })
}

fn sse_frame_end(buffer: &[u8]) -> Option<usize> {
    let lf_end = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| index + 2);
    let crlf_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4);
    match (lf_end, crlf_end) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    }
}

fn http_client(timeout_seconds: u64, proxy: &ProxyConfig) -> Result<reqwest::Client, AiError> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(timeout_seconds));
    if proxy.enabled {
        if let Some(url) = proxy.https.as_ref().or(proxy.http.as_ref()) {
            builder = builder.proxy(reqwest::Proxy::all(url)?);
        }
    }
    Ok(builder.build()?)
}

fn env_var(name: &str, purpose: &'static str) -> Result<String, AiError> {
    env::var(name).map_err(|_| AiError::MissingEnv {
        name: safe_env_name_for_error(name),
        purpose,
    })
}

fn config_value_or_env(
    configured: Option<&str>,
    env_name: &str,
    purpose: &'static str,
) -> Result<String, AiError> {
    if let Some(value) = configured.map(str::trim).filter(|value| !value.is_empty()) {
        Ok(value.to_string())
    } else if let Some(value) = direct_value_in_env_field(env_name, purpose) {
        warn!(
            purpose,
            "configuration *_env field appears to contain a direct value; using it without logging the value"
        );
        Ok(value)
    } else {
        env_var(env_name, purpose)
    }
}

fn missing_api_key(env_name: &str) -> AiError {
    AiError::MissingApiKey {
        env_var: safe_env_name_for_error(env_name),
    }
}

fn direct_value_in_env_field(value: &str, _purpose: &'static str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || is_safe_env_var_name(trimmed) {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn safe_env_name_for_error(name: &str) -> String {
    let trimmed = name.trim();
    if is_safe_env_var_name(trimmed) {
        trimmed.to_string()
    } else {
        "<redacted>".to_string()
    }
}

fn is_safe_env_var_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

#[derive(Debug, Deserialize)]
struct ImageResponse {
    data: Vec<ImageData>,
}

#[derive(Debug, Deserialize)]
struct ImageData {
    b64_json: Option<String>,
    url: Option<String>,
}

fn direct_image_data(response: &Value) -> Option<ImageData> {
    serde_json::from_value::<ImageResponse>(response.clone())
        .ok()?
        .data
        .into_iter()
        .find(|image| image.b64_json.is_some() || image.url.is_some())
}

fn apimart_task_id(response: &Value) -> Option<String> {
    data_entries(response)
        .into_iter()
        .find_map(|entry| string_at(entry, "task_id").or_else(|| string_at(entry, "id")))
}

fn apimart_completed_image_url(response: &Value) -> Result<Option<String>, AiError> {
    if let Some(code) = response.get("code").and_then(Value::as_i64) {
        if code != 200 {
            return Err(AiError::InvalidResponse(format!(
                "image task API returned code {code}: {}",
                error_message(response)
            )));
        }
    }

    for entry in data_entries(response) {
        let status = string_at(entry, "status").unwrap_or_default();
        match status.as_str() {
            "completed" => {
                if let Some(url) = entry
                    .pointer("/result/images/0/url/0")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        entry
                            .pointer("/result/images/0/url")
                            .and_then(Value::as_str)
                    })
                {
                    return Ok(Some(url.to_string()));
                }
                return Err(AiError::InvalidResponse(
                    "completed image task is missing result.images[0].url".into(),
                ));
            }
            "failed" => return Err(AiError::ImageTaskFailed(error_message(entry))),
            "pending" | "queued" | "submitted" | "processing" | "running" | "in_progress" | "" => {}
            other => {
                return Err(AiError::InvalidResponse(format!(
                    "unknown image task status: {other}"
                )))
            }
        }
    }

    Ok(None)
}

fn data_entries(value: &Value) -> Vec<&Value> {
    match value.get("data") {
        Some(Value::Array(items)) => items.iter().collect(),
        Some(data @ Value::Object(_)) => vec![data],
        _ => vec![value],
    }
}

fn string_at(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn error_message(value: &Value) -> String {
    value
        .pointer("/error/message")
        .or_else(|| value.pointer("/data/error/message"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn chat_completions_endpoint(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/chat/completions") {
        base.to_string()
    } else {
        format!("{base}/chat/completions")
    }
}

fn image_generations_endpoint(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if base.ends_with("/images/generations") {
        base.to_string()
    } else {
        format!("{base}/images/generations")
    }
}

fn api_root_url(base_url: &str) -> String {
    base_url
        .trim_end_matches('/')
        .strip_suffix("/images/generations")
        .or_else(|| {
            base_url
                .trim_end_matches('/')
                .strip_suffix("/chat/completions")
        })
        .unwrap_or_else(|| base_url.trim_end_matches('/'))
        .to_string()
}

fn extract_chat_completion_content(response: &Value) -> Option<String> {
    let choice = response.pointer("/choices/0")?;
    non_empty_content_value_to_text(choice.pointer("/message/content"))
        .or_else(|| non_empty_content_value_to_text(choice.pointer("/text")))
        .map(|content| content.trim().to_string())
}

fn non_empty_content_value_to_text(value: Option<&Value>) -> Option<String> {
    content_value_to_text(value).filter(|content| !content.trim().is_empty())
}

fn content_value_to_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(|item| match item {
                    Value::String(text) => Some(text.clone()),
                    Value::Object(_) => item
                        .get("text")
                        .or_else(|| item.get("content"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    _ => None,
                })
                .filter(|text| !text.trim().is_empty())
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join("\n"))
        }
        _ => None,
    }
}

fn chat_completion_finish_reason(response: &Value) -> String {
    response
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string()
}

fn should_retry_empty_length_completion_without_thinking(
    payload: &Value,
    response: &Value,
) -> bool {
    extract_chat_completion_content(response).is_none()
        && chat_completion_finish_reason(response).eq_ignore_ascii_case("length")
        && chat_completion_thinking_enabled(payload)
}

fn chat_completion_thinking_enabled(payload: &Value) -> bool {
    payload
        .pointer("/thinking/type")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("enabled"))
        || payload
            .get("enable_thinking")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

fn disable_chat_completion_thinking(payload: &mut Value) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    if object.contains_key("thinking") {
        object.insert("thinking".into(), json!({ "type": "disabled" }));
    }
    if object.contains_key("enable_thinking") {
        object.insert("enable_thinking".into(), Value::Bool(false));
    }
    object.remove("reasoning_effort");
}

fn apimart_task_status(value: &Value) -> String {
    data_entries(value)
        .into_iter()
        .find_map(|entry| string_at(entry, "status"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn redacted_url_source(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .host_str()
                .map(|host| format!("{}://{}", parsed.scheme(), host))
        })
        .unwrap_or_else(|| "<unparseable-url>".to_string())
}

fn redact_endpoint_for_trace(endpoint: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(endpoint) else {
        return redact_trace_text(endpoint);
    };

    let _ = url.set_username("");
    let _ = url.set_password(None);
    if let Some(query) = url.query().map(ToOwned::to_owned) {
        let redacted = redact_trace_text(&query);
        url.set_query(Some(&redacted));
    }
    if let Some(fragment) = url.fragment().map(ToOwned::to_owned) {
        let redacted = redact_trace_text(&fragment);
        url.set_fragment(Some(&redacted));
    }
    redact_trace_text(url.as_str())
}

fn full_response_for_log(input: &str) -> String {
    redact_trace_text(input)
}

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let redacted = redact_trace_text(input);
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if redacted.chars().count() > max_chars {
        output.push_str("...");
    }
    output
}

fn redact_json_for_trace(value: &Value) -> Value {
    redact_json_value_for_trace(value, None, None)
}

fn redact_json_value_for_trace(
    value: &Value,
    key: Option<&str>,
    parent_key: Option<&str>,
) -> Value {
    if key.map(is_sensitive_json_key).unwrap_or(false) {
        return Value::String("<redacted-secret>".to_string());
    }

    match value {
        Value::String(text) => {
            if let Some(metadata) = media_data_url_trace_metadata(text) {
                metadata
            } else if let Some(metadata) = base64_field_trace_metadata(key, parent_key, text) {
                metadata
            } else {
                Value::String(redact_trace_text(text))
            }
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| redact_json_value_for_trace(item, None, key))
                .collect(),
        ),
        Value::Object(map) => {
            let mut redacted = Map::new();
            let object_key = key;
            for (key, value) in map {
                redacted.insert(
                    key.clone(),
                    redact_json_value_for_trace(value, Some(key.as_str()), object_key),
                );
            }
            Value::Object(redacted)
        }
        _ => value.clone(),
    }
}

fn media_data_url_trace_metadata(value: &str) -> Option<Value> {
    let (header, body) = value.strip_prefix("data:")?.split_once(',')?;
    let media_type = header.split(';').next()?.trim();
    if !(media_type.starts_with("image/")
        || media_type.starts_with("video/")
        || media_type.starts_with("audio/"))
    {
        return None;
    }
    Some(encoded_media_trace_metadata(
        media_type,
        body,
        header.contains(";base64"),
    ))
}

fn base64_field_trace_metadata(
    key: Option<&str>,
    parent_key: Option<&str>,
    value: &str,
) -> Option<Value> {
    let key = key.unwrap_or_default().to_ascii_lowercase();
    let parent_key = parent_key.unwrap_or_default().to_ascii_lowercase();
    let normalized_key = key.replace(['-', '_'], "");
    let normalized_parent = parent_key.replace(['-', '_'], "");
    let media_type = if normalized_parent == "audio" || key.contains("audio") {
        "audio/*"
    } else if normalized_parent == "image" || key.contains("image") {
        "image/*"
    } else if normalized_parent == "video" || key.contains("video") {
        "video/*"
    } else if normalized_key.contains("base64") || normalized_key.contains("encoded") {
        "application/octet-stream"
    } else {
        return None;
    };

    if normalized_parent == "audio" && normalized_key == "data"
        || normalized_key.contains("base64")
        || normalized_key.contains("encoded")
    {
        Some(encoded_media_trace_metadata(media_type, value, true))
    } else {
        None
    }
}

fn encoded_media_trace_metadata(media_type: &str, encoded: &str, is_base64: bool) -> Value {
    let decoded = if is_base64 {
        general_purpose::STANDARD
            .decode(encoded)
            .or_else(|_| general_purpose::URL_SAFE.decode(encoded))
            .ok()
    } else {
        None
    };
    let (decoded_size, sha256) = match decoded {
        Some(bytes) => (Some(bytes.len()), sha256_hex(&bytes)),
        None => (None, sha256_hex(encoded.as_bytes())),
    };
    let mut metadata = Map::new();
    metadata.insert("media_type".into(), json!(media_type));
    metadata.insert("encoded_length".into(), json!(encoded.len()));
    if let Some(decoded_size) = decoded_size {
        metadata.insert("decoded_size".into(), json!(decoded_size));
    }
    metadata.insert("sha256".into(), json!(sha256));
    Value::Object(metadata)
}

fn is_sensitive_json_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '_'], "");
    if normalized.ends_with("env") || normalized.ends_with("envvar") {
        return false;
    }
    [
        "apikey",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "token",
        "password",
        "passwd",
        "clientsecret",
        "secret",
        "authorization",
        "credential",
        "privatekey",
    ]
    .iter()
    .any(|marker| normalized == *marker || normalized.ends_with(marker))
}

fn redact_trace_text(input: &str) -> String {
    let assignments = redact_sensitive_assignments(input);
    let bearer_tokens = redact_bearer_tokens(&assignments);
    redact_secret_like_tokens(&bearer_tokens)
}

fn redact_sensitive_assignments(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut last = 0;
    let mut index = 0;

    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .expect("index is on a char boundary");
        if !is_assignment_key_char(ch)
            || (index > 0
                && input[..index]
                    .chars()
                    .next_back()
                    .is_some_and(is_assignment_key_char))
        {
            index += ch.len_utf8();
            continue;
        }

        let key_end = input[index..]
            .char_indices()
            .find_map(|(offset, ch)| (!is_assignment_key_char(ch)).then_some(index + offset))
            .unwrap_or(input.len());
        let key = &input[index..key_end];
        if !is_sensitive_json_key(key) {
            index = key_end;
            continue;
        }

        let mut separator = key_end;
        while separator < input.len()
            && input[separator..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_whitespace())
        {
            separator += input[separator..]
                .chars()
                .next()
                .expect("separator is on a char boundary")
                .len_utf8();
        }
        if matches!(input[separator..].chars().next(), Some('"' | '\'')) {
            let quote = input[separator..]
                .chars()
                .next()
                .expect("quote was checked");
            separator += quote.len_utf8();
            while separator < input.len()
                && input[separator..]
                    .chars()
                    .next()
                    .is_some_and(|ch| ch.is_ascii_whitespace())
            {
                separator += input[separator..]
                    .chars()
                    .next()
                    .expect("separator is on a char boundary")
                    .len_utf8();
            }
        }
        if !matches!(input[separator..].chars().next(), Some(':' | '=')) {
            index = key_end;
            continue;
        }

        let mut value_start = separator + 1;
        while value_start < input.len()
            && input[value_start..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_whitespace())
        {
            value_start += input[value_start..]
                .chars()
                .next()
                .expect("value start is on a char boundary")
                .len_utf8();
        }
        if value_start == input.len() {
            index = value_start;
            continue;
        }

        let quoted = matches!(input[value_start..].chars().next(), Some('"' | '\''));
        let (content_start, content_end) = if quoted {
            let quote = input[value_start..]
                .chars()
                .next()
                .expect("quote was checked");
            let content_start = value_start + quote.len_utf8();
            let content_end = input[content_start..]
                .find(quote)
                .map(|offset| content_start + offset)
                .unwrap_or(input.len());
            (content_start, content_end)
        } else {
            let allow_spaces = key.eq_ignore_ascii_case("authorization");
            let content_end = input[value_start..]
                .char_indices()
                .find_map(|(offset, ch)| {
                    (is_assignment_value_delimiter(ch, allow_spaces))
                        .then_some(value_start + offset)
                })
                .unwrap_or(input.len());
            (value_start, content_end)
        };
        if content_start == content_end {
            index = content_end;
            continue;
        }

        let value = &input[content_start..content_end];
        let replacement = if value
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("bearer"))
            && value[6..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_whitespace())
        {
            "Bearer <redacted-secret>"
        } else {
            "<redacted-secret>"
        };
        output.push_str(&input[last..content_start]);
        output.push_str(replacement);
        last = content_end;
        index = content_end;
    }

    output.push_str(&input[last..]);
    output
}

fn is_assignment_key_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')
}

fn is_assignment_value_delimiter(ch: char, allow_spaces: bool) -> bool {
    (!allow_spaces && ch.is_ascii_whitespace()) || matches!(ch, ',' | ';' | '}' | ']' | '&' | '#')
}

fn redact_bearer_tokens(input: &str) -> String {
    let lowercase = input.to_ascii_lowercase();
    let mut output = String::with_capacity(input.len());
    let mut last = 0;
    let mut search_from = 0;

    while let Some(relative) = lowercase[search_from..].find("bearer") {
        let start = search_from + relative;
        let end = start + "bearer".len();
        let boundary_before = start == 0
            || !input[..start]
                .chars()
                .next_back()
                .is_some_and(|ch| ch.is_ascii_alphanumeric());
        let boundary_after = end == input.len()
            || !input[end..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_alphanumeric());
        if !boundary_before || !boundary_after {
            search_from = end;
            continue;
        }

        let mut token_start = end;
        while token_start < input.len()
            && input[token_start..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_whitespace())
        {
            token_start += input[token_start..]
                .chars()
                .next()
                .expect("token start is on a char boundary")
                .len_utf8();
        }
        let token_end = input[token_start..]
            .char_indices()
            .find_map(|(offset, ch)| {
                matches!(ch, ',' | ';' | '}' | ']' | '&' | '#' | '"' | '\'')
                    .then_some(token_start + offset)
            })
            .unwrap_or(input.len());
        let token = input[token_start..token_end].trim_end();
        if token.len() < 8 || token.starts_with("<redacted") {
            search_from = end;
            continue;
        }

        output.push_str(&input[last..start]);
        output.push_str("Bearer <redacted-secret>");
        last = token_end - (input[token_start..token_end].len() - token.len());
        search_from = token_end;
    }

    output.push_str(&input[last..]);
    output
}

fn redact_secret_like_tokens(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while let Some(relative) = input[index..].find("sk-") {
        let start = index + relative;
        output.push_str(&input[index..start]);
        let mut end = start + 3;
        for (offset, ch) in input[end..].char_indices() {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                end = start + 3 + offset + ch.len_utf8();
            } else {
                break;
            }
        }
        if end - start >= 12 {
            output.push_str("<redacted-secret>");
        } else {
            output.push_str(&input[start..end]);
        }
        index = end;
    }
    output.push_str(&input[index..]);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image_client(config: ImageGenConfig) -> OpenAiImageClient {
        OpenAiImageClient {
            config,
            client: reqwest::Client::new(),
            key_pool: Arc::new(ApiKeyPool::from_keys(vec!["test-key".to_string()], 0)),
            base_url: "https://api.apimart.ai/v1".into(),
            model: "gpt-image-2".into(),
            retry_notifier: None,
            trace_dir: None,
            trace_context: None,
        }
    }

    #[test]
    fn trace_redaction_removes_secret_like_values() {
        let payload = json!({
            "api_key": "sk-test-secret-value",
            "refresh_token": "refresh-secret-value",
            "id_token": "id-secret-value",
            "credential": "credential-secret-value",
            "private_key": "private-key-secret-value",
            "api_key_env": "LLM_API_KEY",
            "headers": {
                "Authorization": "Bearer sk-auth-secret-value",
                "x-safe": "plain"
            },
            "messages": [
                {
                    "role": "user",
                    "content": "token sk-inline-secret-value should not leak"
                }
            ]
        });

        let redacted = redact_json_for_trace(&payload);
        assert_eq!(redacted["api_key"], "<redacted-secret>");
        assert_eq!(redacted["refresh_token"], "<redacted-secret>");
        assert_eq!(redacted["id_token"], "<redacted-secret>");
        assert_eq!(redacted["credential"], "<redacted-secret>");
        assert_eq!(redacted["private_key"], "<redacted-secret>");
        assert_eq!(redacted["api_key_env"], "LLM_API_KEY");
        assert_eq!(redacted["headers"]["Authorization"], "<redacted-secret>");
        assert_eq!(redacted["headers"]["x-safe"], "plain");
        assert!(!redacted["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("sk-inline-secret-value"));
    }

    #[test]
    fn trace_endpoint_removes_url_credentials_but_keeps_safe_parts() {
        let endpoint = "https://trace-user:trace-password@example.com/v1/tasks?model=vision&api_key=query-secret#token=fragment-secret";

        let redacted = redact_endpoint_for_trace(endpoint);

        assert!(!redacted.contains("trace-user"));
        assert!(!redacted.contains("trace-password"));
        assert!(!redacted.contains("query-secret"));
        assert!(!redacted.contains("fragment-secret"));
        assert!(redacted.contains("https://example.com/v1/tasks"));
        assert!(redacted.contains("model=vision"));
        assert!(redacted.contains("api_key=<redacted-secret>"));
        assert!(redacted.contains("token=<redacted-secret>"));
    }

    #[test]
    fn trace_text_redacts_common_assignments_without_erasing_normal_text() {
        let text = "Authorization: Bearer bearer-secret-value, api_key=api-secret-value; token=token-secret-value password: pass-secret secret='secret-value'; token_count=3; ordinary content stays";

        let redacted = redact_trace_text(text);

        assert!(!redacted.contains("bearer-secret-value"));
        assert!(!redacted.contains("api-secret-value"));
        assert!(!redacted.contains("token-secret-value"));
        assert!(!redacted.contains("pass-secret"));
        assert!(!redacted.contains("secret-value"));
        assert!(redacted.contains("token_count=3"));
        assert!(redacted.contains("ordinary content stays"));
    }

    #[test]
    fn trace_redaction_replaces_image_and_video_data_urls_with_metadata() {
        let payload = json!({
            "image": "data:image/png;base64,AAAA",
            "video": "data:video/mp4,raw-video-data",
            "base64": "c2Vuc2l0aXZlLWJhc2U2NC1iaW5hcnk=",
        });

        let redacted = redact_json_for_trace(&payload);
        assert_eq!(redacted["image"]["media_type"], "image/png");
        assert_eq!(redacted["image"]["encoded_length"], 4);
        assert_eq!(redacted["video"]["media_type"], "video/mp4");
        assert_eq!(redacted["video"]["encoded_length"], 14);
        assert!(redacted["image"].get("sha256").is_some());
        assert!(redacted["video"].get("sha256").is_some());
        assert_eq!(redacted["base64"]["media_type"], "application/octet-stream");
        assert!(!serde_json::to_string(&redacted)
            .unwrap()
            .contains("c2Vuc2l0aXZlLWJhc2U2NC1iaW5hcnk="));
    }

    #[test]
    fn multipart_voice_trace_contains_audio_metadata_without_base64() {
        let client = OpenAiAudioTranscriptionClient {
            config: voice_transcription_config(),
            client: reqwest::Client::new(),
            key_pool: Arc::new(ApiKeyPool::from_keys(vec!["test-key".to_string()], 0)),
            base_url: "https://api.example.com/v1".into(),
            model: "voice-model".into(),
            trace_dir: None,
            trace_context: None,
        };
        let bytes = b"audio-secret-binary";
        let encoded = general_purpose::STANDARD.encode(bytes);
        let trace = voice_transcription_multipart_trace_payload(
            &client,
            "voice.mp3",
            Some("audio/mpeg"),
            bytes,
        );
        let serialized = serde_json::to_string(&trace).unwrap();

        assert!(!serialized.contains(&encoded));
        assert_eq!(trace["file"]["media_type"], "audio/mpeg");
        assert_eq!(trace["file"]["encoded_length"], encoded.len());
        assert_eq!(trace["file"]["decoded_size"], bytes.len());
        assert_eq!(trace["file"]["mime"], "audio/mpeg");
        assert_eq!(trace["file"]["size_bytes"], bytes.len());
        assert_eq!(trace["file"]["sha256"], sha256_hex(bytes));
    }

    #[test]
    fn stepfun_asr_trace_redacts_audio_data_without_changing_http_payload() {
        let config = voice_transcription_config();
        let bytes = b"stepfun-audio-secret";
        let payload =
            stepfun_asr_payload("stepaudio-2.5-asr", &config, Path::new("voice.mp3"), bytes)
                .unwrap();
        let encoded = payload["audio"]["data"].as_str().unwrap();
        let redacted = redact_json_for_trace(&payload);
        let serialized = serde_json::to_string(&redacted).unwrap();

        assert_eq!(encoded, general_purpose::STANDARD.encode(bytes));
        assert!(!serialized.contains(encoded));
        assert_eq!(redacted["audio"]["data"]["media_type"], "audio/*");
        assert_eq!(redacted["audio"]["data"]["decoded_size"], bytes.len());
        assert_eq!(redacted["audio"]["data"]["sha256"], sha256_hex(bytes));
    }

    fn image_caption_config() -> ImageCaptionConfig {
        ImageCaptionConfig {
            enabled: true,
            provider: "openai_compatible".into(),
            api_key: None,
            api_key_env: "IMAGE_CAPTION_API_KEY".into(),
            api_keys: Vec::new(),
            api_keys_env: "IMAGE_CAPTION_API_KEYS".into(),
            base_url: None,
            base_url_env: "IMAGE_CAPTION_BASE_URL".into(),
            model: None,
            model_env: "IMAGE_CAPTION_MODEL".into(),
            timeout_seconds: 120,
            retry_5xx_attempts: 5,
            max_output_tokens: 500,
            temperature: 0.1,
            system_prompt: "describe".into(),
            user_prompt: "caption".into(),
            max_images_per_summary: 20,
            max_concurrent_requests: 4,
            max_concurrent_per_key: 0,
            request_body_overrides: Default::default(),
        }
    }

    fn voice_transcription_config() -> VoiceTranscriptionConfig {
        VoiceTranscriptionConfig {
            enabled: true,
            provider: "openai_compatible".into(),
            api_key: None,
            api_key_env: "VOICE_TRANSCRIPTION_API_KEY".into(),
            api_keys: Vec::new(),
            api_keys_env: "VOICE_TRANSCRIPTION_API_KEYS".into(),
            base_url: None,
            base_url_env: "VOICE_TRANSCRIPTION_BASE_URL".into(),
            model: None,
            model_env: "VOICE_TRANSCRIPTION_MODEL".into(),
            timeout_seconds: 120,
            retry_5xx_attempts: 5,
            language: "zh".into(),
            prompt: String::new(),
            response_format: "json".into(),
            transcode_to_mp3: true,
            ffmpeg_executable: "ffmpeg".into(),
            mp3_bitrate: "64k".into(),
            max_voices_per_summary: 20,
            max_concurrent_requests: 2,
            max_concurrent_per_key: 0,
            request_body_overrides: Default::default(),
        }
    }

    fn image_config() -> ImageGenConfig {
        ImageGenConfig {
            enabled: true,
            provider: "openai".into(),
            api_key: None,
            api_key_env: "IMAGE_API_KEY".into(),
            api_keys: Vec::new(),
            api_keys_env: "IMAGE_API_KEYS".into(),
            base_url: None,
            base_url_env: "IMAGE_BASE_URL".into(),
            model: None,
            model_env: "IMAGE_MODEL".into(),
            size: "16:9".into(),
            quality: None,
            resolution: Some("2k".into()),
            official_fallback: false,
            poll_initial_delay_seconds: 10,
            poll_interval_seconds: 5,
            timeout_seconds: 300,
            retry_5xx_attempts: 5,
            max_concurrent_per_key: 0,
            prompt_template: None,
        }
    }

    #[test]
    fn image_payload_uses_resolution_for_apimart_gpt_image_2() {
        let client = image_client(image_config());

        let payload = client.image_generation_payload("画一张群聊总结海报");

        assert_eq!(payload["model"], "gpt-image-2");
        assert_eq!(payload["prompt"], "画一张群聊总结海报");
        assert_eq!(payload["n"], 1);
        assert_eq!(payload["size"], "16:9");
        assert_eq!(payload["resolution"], "2k");
        assert!(payload.get("quality").is_none());
    }

    #[test]
    fn image_caption_payload_uses_multimodal_content() {
        let payload = image_caption_payload(
            "vision-model",
            "system",
            "describe image",
            "data:image/png;base64,abc",
            0.1,
            500,
        );

        assert_eq!(payload["model"], "vision-model");
        assert_eq!(payload["messages"][1]["content"][0]["type"], "text");
        assert_eq!(payload["messages"][1]["content"][1]["type"], "image_url");
        assert_eq!(
            payload["messages"][1]["content"][1]["image_url"]["url"],
            "data:image/png;base64,abc"
        );
    }

    #[test]
    fn video_caption_payload_uses_video_url_content() {
        let payload = video_caption_payload(
            "step-3.7-flash",
            "system",
            "describe video",
            "data:video/mp4;base64,AAAA",
            0.1,
            800,
        );

        assert_eq!(payload["model"], "step-3.7-flash");
        assert_eq!(payload["messages"][1]["content"][0]["type"], "text");
        assert_eq!(payload["messages"][1]["content"][1]["type"], "video_url");
        assert_eq!(
            payload["messages"][1]["content"][1]["video_url"]["url"],
            "data:video/mp4;base64,AAAA"
        );
        assert_eq!(payload["max_tokens"], 800);
    }

    #[test]
    fn image_caption_client_can_use_persistent_config_values() {
        let mut config = image_caption_config();
        config.api_key = Some("persisted-key".into());
        config.base_url = Some("https://api.example.com/v1".into());
        config.model = Some("vision-model".into());
        let proxy = ProxyConfig {
            enabled: false,
            http: None,
            https: None,
        };

        let client = OpenAiVisionCaptionClient::new(config, &proxy).unwrap();

        assert_eq!(client.key_pool.len(), 1);
        assert_eq!(client.key_pool.keys(), vec!["persisted-key"]);
        assert_eq!(client.base_url, "https://api.example.com/v1");
        assert_eq!(client.model, "vision-model");
    }

    #[test]
    fn voice_transcription_helpers_accept_openai_style_outputs() {
        assert_eq!(
            audio_transcriptions_endpoint("https://api.example.com/v1"),
            "https://api.example.com/v1/audio/transcriptions"
        );
        assert_eq!(
            audio_transcriptions_endpoint("https://api.example.com/v1/audio/transcriptions"),
            "https://api.example.com/v1/audio/transcriptions"
        );
        assert_eq!(
            extract_transcription_text(r#"{"text":"你好世界"}"#, "json").unwrap(),
            "你好世界"
        );
        assert_eq!(
            extract_transcription_text("plain text", "text").unwrap(),
            "plain text"
        );
    }

    #[test]
    fn stepfun_asr_payload_uses_documented_json_shape() {
        let mut config = voice_transcription_config();
        config.provider = "stepfun".into();
        config.language = "zh".into();
        config.request_body_overrides = toml::from_str(
            r#"
[audio.input.transcription]
hotwords = ["群聊", "总结"]
enable_timestamp = true
"#,
        )
        .unwrap();

        let payload =
            stepfun_asr_payload("stepaudio-2.5-asr", &config, Path::new("voice.mp3"), b"abc")
                .unwrap();

        assert_eq!(
            stepfun_asr_endpoint("https://api.stepfun.com/v1"),
            "https://api.stepfun.com/v1/audio/asr/sse"
        );
        assert_eq!(payload["audio"]["data"], json!("YWJj"));
        assert_eq!(
            payload["audio"]["input"]["transcription"]["model"],
            json!("stepaudio-2.5-asr")
        );
        assert_eq!(
            payload["audio"]["input"]["transcription"]["language"],
            json!("zh")
        );
        assert_eq!(
            payload["audio"]["input"]["transcription"]["hotwords"],
            json!(["群聊", "总结"])
        );
        assert_eq!(
            payload["audio"]["input"]["transcription"]["enable_timestamp"],
            json!(true)
        );
        assert_eq!(payload["audio"]["input"]["format"]["type"], json!("mp3"));
    }

    #[test]
    fn stepfun_asr_sse_text_prefers_done_and_can_use_delta() {
        let sse = r#"data: {"type":"transcript.text.delta","delta":"识别的"}

data: {"type":"transcript.text.done","text":"识别的完整文字内容"}
"#;
        assert_eq!(extract_stepfun_asr_text(sse).unwrap(), "识别的完整文字内容");

        let delta_only = r#"data: {"type":"transcript.text.delta","delta":"识别的"}

data: {"type":"transcript.text.delta","delta":"文字"}
"#;
        assert_eq!(extract_stepfun_asr_text(delta_only).unwrap(), "识别的文字");
    }

    #[test]
    fn voice_transcription_uses_mp3_mime_type() {
        assert_eq!(
            audio_mime_type(Path::new(r"C:\temp\voice.mp3")),
            Some("audio/mpeg")
        );
        assert_eq!(audio_mime_type(Path::new("voice.silk")), None);
    }

    #[test]
    fn voice_transcription_client_can_use_persistent_config_values() {
        let mut config = voice_transcription_config();
        config.api_key = Some("persisted-voice-key".into());
        config.base_url = Some("https://voice.example.com/v1".into());
        config.model = Some("whisper-model".into());
        let proxy = ProxyConfig {
            enabled: false,
            http: None,
            https: None,
        };

        let client = OpenAiAudioTranscriptionClient::new(config, &proxy).unwrap();

        assert_eq!(client.key_pool.len(), 1);
        assert_eq!(client.key_pool.keys(), vec!["persisted-voice-key"]);
        assert_eq!(client.base_url, "https://voice.example.com/v1");
        assert_eq!(client.model, "whisper-model");
    }

    #[test]
    fn image_mime_type_detects_png() {
        assert_eq!(
            image_mime_type(None, &[0x89, 0x50, 0x4e, 0x47]),
            Some("image/png")
        );
    }

    #[test]
    fn image_data_url_uses_supported_rfc2397_media_types() {
        let jpeg = image_data_url_from_bytes(None, &[0xff, 0xd8, 0xff, 0x00]).unwrap();
        assert!(jpeg.starts_with("data:image/jpeg;base64,"));

        let png = image_data_url_from_bytes(None, &[0x89, 0x50, 0x4e, 0x47]).unwrap();
        assert!(png.starts_with("data:image/png;base64,"));

        let webp = image_data_url_from_bytes(None, b"RIFF\x00\x00\x00\x00WEBPVP8 \x00\x00\x00\x00")
            .unwrap();
        assert!(webp.starts_with("data:image/webp;base64,"));
    }

    #[test]
    fn image_data_url_accepts_static_gif_and_marks_animated_gif_unsupported() {
        let static_gif =
            b"GIF89a\x01\x00\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";
        let animated_gif = b"GIF89a\x01\x00\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x00;";

        let static_data_url = image_data_url_from_bytes(None, static_gif).unwrap();
        assert!(static_data_url.starts_with("data:image/gif;base64,"));
        assert_eq!(image_mime_type(None, animated_gif), None);
    }

    #[test]
    fn image_data_url_converts_unsupported_decodable_images_to_jpeg() {
        let image = image::RgbImage::from_pixel(1, 1, image::Rgb([255, 0, 0]));
        let mut cursor = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgb8(image)
            .write_to(&mut cursor, image::ImageFormat::Bmp)
            .unwrap();
        let bmp = cursor.into_inner();

        assert_eq!(
            image_mime_type(Some(std::path::Path::new("image.bmp")), &bmp),
            None
        );
        let data_url =
            image_data_url_from_bytes(Some(std::path::Path::new("image.bmp")), &bmp).unwrap();
        assert!(data_url.starts_with("data:image/jpeg;base64,"));
    }

    #[test]
    fn image_data_url_rejects_invalid_unsupported_images() {
        assert!(
            image_data_url_from_bytes(Some(std::path::Path::new("image.bmp")), b"BM\x00\x00")
                .is_err()
        );
        assert!(image_data_url_from_bytes(Some(std::path::Path::new("image.gif")), b"").is_err());
    }

    #[test]
    fn image_data_url_validation_requires_supported_base64_image_type() {
        assert!(validate_image_data_url("data:image/jpeg;base64,abc").is_ok());
        assert!(validate_image_data_url("data:image/png,abc").is_err());
        assert!(validate_image_data_url("data:image/bmp;base64,abc").is_err());
    }

    #[test]
    fn video_data_url_from_bytes_accepts_mp4_magic() {
        let data = video_data_url_from_bytes(
            Some(std::path::Path::new("clip.mp4")),
            b"\x00\x00\x00\x18ftypmp42",
            128,
        )
        .unwrap();

        assert!(data.starts_with("data:video/mp4;base64,"));
    }

    #[test]
    fn video_data_url_from_bytes_rejects_oversized_video() {
        let error = video_data_url_from_bytes(
            Some(std::path::Path::new("clip.mp4")),
            b"\x00\x00\x00\x18ftypmp42",
            4,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("too large"));
    }

    #[test]
    fn image_payload_keeps_quality_for_openai_style_requests() {
        let mut config = image_config();
        config.resolution = None;
        config.quality = Some("high".into());
        let client = image_client(config);

        let payload = client.image_generation_payload("poster");

        assert_eq!(payload["quality"], "high");
        assert!(payload.get("resolution").is_none());
    }

    #[test]
    fn parses_apimart_task_response_and_result_url() {
        let submitted = json!({
            "code": 200,
            "data": [{"status": "submitted", "task_id": "task_123"}]
        });
        let pending = json!({
            "code": 200,
            "data": [{"status": "pending", "task_id": "task_123"}]
        });
        let completed = json!({
            "code": 200,
            "data": {
                "id": "task_123",
                "status": "completed",
                "result": {
                    "images": [{"url": ["https://upload.apimart.ai/f/image/out.png"]}]
                }
            }
        });

        assert_eq!(apimart_task_id(&submitted).as_deref(), Some("task_123"));
        assert_eq!(apimart_completed_image_url(&pending).unwrap(), None);
        assert_eq!(
            apimart_completed_image_url(&completed).unwrap().as_deref(),
            Some("https://upload.apimart.ai/f/image/out.png")
        );
    }

    #[test]
    fn image_client_can_use_persistent_config_values() {
        let mut config = image_config();
        config.api_key = Some("persisted-key".into());
        config.base_url = Some("https://api.apimart.ai/v1".into());
        config.model = Some("gpt-image-2".into());

        let proxy = ProxyConfig {
            enabled: false,
            http: None,
            https: None,
        };
        let client = OpenAiImageClient::new(config, &proxy).unwrap();

        assert_eq!(client.key_pool.len(), 1);
        assert_eq!(client.key_pool.keys(), vec!["persisted-key"]);
        assert_eq!(client.base_url, "https://api.apimart.ai/v1");
        assert_eq!(client.model, "gpt-image-2");
    }

    #[test]
    fn env_fields_accept_direct_values_without_logging_secret_names() {
        let direct_key = "sk-test-direct-value-1234567890";
        let mut config = image_config();
        config.api_key_env = direct_key.into();
        config.base_url_env = "https://api.apimart.ai/v1".into();
        config.model_env = "gpt-image-2".into();

        let proxy = ProxyConfig {
            enabled: false,
            http: None,
            https: None,
        };
        let client = OpenAiImageClient::new(config, &proxy).unwrap();

        assert_eq!(client.key_pool.len(), 1);
        assert_eq!(client.key_pool.keys(), vec![direct_key]);
        assert_eq!(client.base_url, "https://api.apimart.ai/v1");
        assert_eq!(client.model, "gpt-image-2");
    }

    #[test]
    fn missing_api_key_error_redacts_secret_like_env_field() {
        let error = missing_api_key("sk-test-direct-value-1234567890").to_string();

        assert!(!error.contains("sk-test"));
        assert!(error.contains("<redacted>"));
    }

    #[test]
    fn log_truncation_redacts_secret_like_tokens() {
        let text = truncate_for_log("bad sk-test-direct-value-1234567890 here", 200);

        assert!(!text.contains("sk-test"));
        assert!(text.contains("<redacted-secret>"));
    }

    #[test]
    fn full_response_log_keeps_long_body_and_redacts_secret_like_tokens() {
        let body = format!(
            "bad sk-test-direct-value-1234567890 here {}",
            "x".repeat(900)
        );
        let text = full_response_for_log(&body);

        assert!(!text.contains("sk-test"));
        assert!(text.contains("<redacted-secret>"));
        assert!(text.len() > 900);
        assert!(!text.ends_with("..."));
    }

    #[test]
    fn chat_completion_content_supports_string_and_array_forms() {
        let string_response = json!({
            "choices": [{
                "message": {"content": "hello"},
                "finish_reason": "stop"
            }]
        });
        let array_response = json!({
            "choices": [{
                "message": {
                    "content": [
                        {"type": "text", "text": "hello"},
                        {"type": "text", "text": "world"}
                    ]
                },
                "finish_reason": "stop"
            }]
        });

        assert_eq!(
            extract_chat_completion_content(&string_response).as_deref(),
            Some("hello")
        );
        assert_eq!(
            extract_chat_completion_content(&array_response).as_deref(),
            Some("hello\nworld")
        );
    }

    #[test]
    fn chat_completion_content_does_not_fall_back_to_reasoning_content() {
        let response = json!({
            "choices": [{
                "message": {
                    "content": "",
                    "reasoning_content": "fallback text"
                },
                "finish_reason": "stop"
            }]
        });

        assert_eq!(extract_chat_completion_content(&response).as_deref(), None);
    }

    #[test]
    fn streamed_chat_completion_collects_content_and_discards_reasoning() {
        let mut accumulator = SseChatCompletionAccumulator::new();
        let first = concat!(
            "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"internal\"},\"finish_reason\":null}]}\r\n\r\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello \"},\"finish_reason\":null}]}\r\n\r\n"
        );
        let second = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}]}\r\n\r\n",
            "data: [DONE]\r\n\r\n"
        );

        accumulator.push_chunk(first.as_bytes()).unwrap();
        accumulator.push_chunk(second.as_bytes()).unwrap();

        assert!(accumulator.received_data);
        assert!(accumulator.completed);
        let streamed = accumulator.into_streamed_response();
        assert_eq!(
            extract_chat_completion_content(&streamed.response).as_deref(),
            Some("hello world")
        );
        assert_eq!(chat_completion_finish_reason(&streamed.response), "stop");
        assert!(streamed.raw_body.contains("reasoning_content"));
    }

    #[test]
    fn streamed_chat_completion_requires_done_event() {
        let mut accumulator = SseChatCompletionAccumulator::new();
        let partial = "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n";
        accumulator.push_chunk(partial.as_bytes()).unwrap();

        assert!(accumulator.received_data);
        assert!(!accumulator.completed);
    }

    #[test]
    fn empty_length_completion_with_thinking_enabled_uses_non_thinking_fallback() {
        let mut payload = json!({
            "model": "step-3.7-flash",
            "thinking": { "type": "enabled" },
            "reasoning_effort": "low"
        });
        let response = json!({
            "choices": [{
                "finish_reason": "length",
                "message": { "content": "", "reasoning": "still thinking" }
            }]
        });

        assert!(should_retry_empty_length_completion_without_thinking(
            &payload, &response
        ));
        disable_chat_completion_thinking(&mut payload);
        assert_eq!(payload["thinking"]["type"], "disabled");
        assert!(payload.get("reasoning_effort").is_none());
        assert!(!should_retry_empty_length_completion_without_thinking(
            &payload, &response
        ));
    }

    #[test]
    fn empty_length_completion_without_explicit_thinking_is_not_retried() {
        let payload = json!({ "model": "plain-model" });
        let response = json!({
            "choices": [{
                "finish_reason": "length",
                "message": { "content": "" }
            }]
        });

        assert!(!should_retry_empty_length_completion_without_thinking(
            &payload, &response
        ));
    }

    #[test]
    fn request_body_overrides_replace_and_extend_payload() {
        let overrides = toml::from_str(
            r#"
temperature = 0
enable_thinking = false
reasoning_effort = "none"
"#,
        )
        .unwrap();
        let mut payload = json!({
            "model": "base-model",
            "temperature": 0.3,
            "max_tokens": 2000,
        });

        apply_request_body_overrides(&mut payload, &overrides);

        assert_eq!(payload["temperature"], json!(0));
        assert_eq!(payload["enable_thinking"], json!(false));
        assert_eq!(payload["reasoning_effort"], json!("none"));
        assert_eq!(payload["max_tokens"], json!(2000));
    }

    #[test]
    fn chat_completion_payload_can_omit_max_tokens() {
        let payload = chat_completion_payload("model-a", "system prompt", "user prompt", 0.3, None);

        assert_eq!(payload["model"], json!("model-a"));
        assert!((payload["temperature"].as_f64().unwrap() - 0.3).abs() < 0.0001);
        assert!(payload.get("max_tokens").is_none());
    }

    #[test]
    fn chat_completion_payload_keeps_max_tokens_when_requested() {
        let payload =
            chat_completion_payload("model-a", "system prompt", "user prompt", 0.3, Some(2000));

        assert_eq!(payload["max_tokens"], json!(2000));
    }

    #[test]
    fn chat_completion_retry_policy_retries_transient_upstream_failures() {
        assert!(should_retry_chat_completion_failure(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            r#"{"code":"UPSTREAM_FAILED"}"#
        ));
        assert!(should_retry_chat_completion_failure(
            reqwest::StatusCode::BAD_GATEWAY,
            ""
        ));
        assert!(should_retry_chat_completion_failure(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            ""
        ));
    }

    #[test]
    fn chat_completion_retry_policy_does_not_retry_content_policy_blocks() {
        assert!(!should_retry_chat_completion_failure(
            reqwest::StatusCode::UNAVAILABLE_FOR_LEGAL_REASONS,
            r#"{"code":"UPSTREAM_REQUEST_FAILED","message":"{\"type\":\"censorship_blocked\"}"}"#
        ));
        assert!(!should_retry_chat_completion_failure(
            reqwest::StatusCode::BAD_GATEWAY,
            r#"{"code":"UPSTREAM_REQUEST_FAILED","message":"The content you provided or machine outputted is blocked."}"#
        ));
    }

    #[test]
    fn http_retry_count_defaults_to_extra_attempts() {
        assert_eq!(http_max_attempts(0), 1);
        assert_eq!(http_max_attempts(5), 6);
        assert_eq!(http_retry_delay_ms(1), 1_000);
        assert_eq!(http_retry_delay_ms(5), 16_000);
        assert_eq!(http_retry_delay_ms(6), 30_000);
        assert_eq!(http_retry_delay_ms(7), 30_000);
    }

    #[test]
    fn rate_limit_retry_uses_bounded_exponential_backoff_with_jitter() {
        let first = http_retry_delay_ms_for_status(reqwest::StatusCode::TOO_MANY_REQUESTS, 1, None);
        let later = http_retry_delay_ms_for_status(reqwest::StatusCode::TOO_MANY_REQUESTS, 5, None);
        assert!((1_000..=1_200).contains(&first));
        assert!((16_000..=19_200).contains(&later));
        let gateway = http_retry_delay_ms_for_status(reqwest::StatusCode::BAD_GATEWAY, 3, None);
        assert!((4_000..=4_800).contains(&gateway));
    }

    #[test]
    fn rate_limit_retry_honors_retry_after_without_exceeding_budget() {
        let delay =
            http_retry_delay_ms_for_status(reqwest::StatusCode::TOO_MANY_REQUESTS, 1, Some(5_000));
        assert!((5_000..=6_000).contains(&delay));
    }

    #[test]
    fn retry_budget_bounds_cumulative_wait_time() {
        let budget = RetryBudget {
            started: Instant::now() - Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS),
            deadline: None,
            retry_delay_budget: Some(Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS)),
        };

        assert!(!budget.allows(1));
    }

    #[tokio::test]
    async fn rate_limit_queue_wait_is_counted_against_retry_budget() {
        let queue = Arc::new(Mutex::new(()));
        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let holder_queue = Arc::clone(&queue);
        tokio::spawn(async move {
            let _guard = holder_queue.lock().await;
            locked_tx.send(()).unwrap();
            sleep(Duration::from_millis(30)).await;
        });
        locked_rx.await.unwrap();

        let retry_budget = RetryBudget {
            started: Instant::now() - Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS - 50),
            deadline: None,
            retry_delay_budget: Some(Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS)),
        };
        let started = Instant::now();
        let waited =
            wait_in_rate_limit_retry_queue(queue.as_ref(), "test retry", 1, 2, &retry_budget, 100)
                .await;

        assert!(waited.unwrap() < 100);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn exhausted_rate_limit_budget_returns_no_retry_plan() {
        let queue = Mutex::new(());
        let retry_budget = RetryBudget {
            started: Instant::now() - Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS),
            deadline: None,
            retry_delay_budget: Some(Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS)),
        };

        let plan =
            wait_in_rate_limit_retry_queue(&queue, "test retry", 1, 2, &retry_budget, 100).await;

        assert!(plan.is_none());
    }

    #[tokio::test]
    async fn outer_retry_deadline_bounds_backoff_sleep() {
        let budget = RetryBudget::with_deadline(Instant::now() + Duration::from_millis(25));
        let started = Instant::now();

        assert!(!sleep_for_retry_budget(&budget, Duration::from_millis(200)).await);
        assert!(started.elapsed() < Duration::from_millis(150));
        assert!(budget.deadline_exhausted());
    }

    #[test]
    fn chat_completion_retry_policy_does_not_retry_context_errors() {
        assert!(!should_retry_chat_completion_failure(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"type":"context_length_exceeded"}"#
        ));
    }

    #[test]
    fn endpoint_helpers_accept_root_or_full_endpoint_base_urls() {
        assert_eq!(
            image_generations_endpoint("https://api.apimart.ai/v1"),
            "https://api.apimart.ai/v1/images/generations"
        );
        assert_eq!(
            image_generations_endpoint("https://api.apimart.ai/v1/images/generations"),
            "https://api.apimart.ai/v1/images/generations"
        );
        assert_eq!(
            chat_completions_endpoint("https://api.example.com/v1"),
            "https://api.example.com/v1/chat/completions"
        );
        assert_eq!(
            api_root_url("https://api.apimart.ai/v1/images/generations"),
            "https://api.apimart.ai/v1"
        );
    }

    #[test]
    fn key_pool_dedupes_and_trims_keys() {
        let pool = ApiKeyPool::from_keys(
            vec![
                "sk-a".to_string(),
                "sk-a".to_string(),
                "".to_string(),
                "  sk-b  ".to_string(),
            ],
            0,
        );

        assert_eq!(pool.len(), 2);
        assert_eq!(pool.keys(), vec!["sk-a", "sk-b"]);
    }

    #[tokio::test]
    async fn key_pool_round_robin_distributes_across_keys() {
        let pool = ApiKeyPool::from_keys(
            vec!["sk-a".to_string(), "sk-b".to_string(), "sk-c".to_string()],
            1,
        );

        let first = pool.acquire().await;
        let second = pool.acquire().await;
        let third = pool.acquire().await;

        let mut indices = vec![first.key_index(), second.key_index(), third.key_index()];
        indices.sort();
        assert_eq!(indices, vec![0, 1, 2]);
        assert_eq!(first.key(), "sk-a");
        assert_eq!(second.key(), "sk-b");
        assert_eq!(third.key(), "sk-c");
    }

    #[tokio::test]
    async fn key_pool_per_key_cap_limits_concurrency() {
        let pool = ApiKeyPool::from_keys(vec!["sk-a".to_string()], 1);
        let first = pool.acquire().await;
        assert_eq!(first.key_index(), 0);

        // Second concurrent acquire on the same key must wait for the first permit.
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let waiter_pool = Arc::new(pool);
        let waiter = {
            let pool = Arc::clone(&waiter_pool);
            tokio::spawn(async move {
                let _permit = pool.acquire().await;
                release_rx.await.ok();
            })
        };
        sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        drop(first);
        release_tx.send(()).ok();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should finish after first permit released")
            .unwrap();
    }

    #[tokio::test]
    async fn key_pool_unlimited_cap_allows_all_concurrent_acquires() {
        let pool = ApiKeyPool::from_keys(vec!["sk-a".to_string(), "sk-b".to_string()], 0);

        let a = pool.acquire().await;
        let b = pool.acquire().await;
        let c = pool.acquire().await;
        assert_eq!(a.key_index(), 0);
        assert_eq!(b.key_index(), 1);
        assert_eq!(c.key_index(), 0);
    }

    #[test]
    fn shared_key_pool_reuses_pool_for_same_credentials() {
        let first = shared_key_pool(vec!["sk-a".to_string(), "sk-b".to_string()], 1);
        let second = shared_key_pool(vec!["sk-b".to_string(), "sk-a".to_string()], 1);
        let different = shared_key_pool(vec!["sk-a".to_string(), "sk-b".to_string()], 2);

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &different));
        assert_eq!(first.len(), 2);
        assert_eq!(different.len(), 2);
    }

    #[test]
    fn resolve_api_keys_prefers_explicit_list_over_single_fields() {
        let keys = resolve_api_keys(
            &["sk-list-1".to_string(), "sk-list-2".to_string()],
            Some("sk-single"),
            "LLM_API_KEYS",
            "LLM_API_KEY",
            "test key",
        )
        .unwrap();

        assert_eq!(keys, vec!["sk-list-1", "sk-list-2"]);
    }

    #[test]
    fn resolve_api_keys_splits_comma_newline_and_semicolon_separated_single_key() {
        let keys = resolve_api_keys(
            &[],
            Some("sk-a, sk-b\nsk-c;sk-d"),
            "",
            "LLM_API_KEY",
            "test key",
        )
        .unwrap();

        assert_eq!(keys, vec!["sk-a", "sk-b", "sk-c", "sk-d"]);
    }

    #[test]
    fn resolve_api_keys_accepts_direct_value_in_api_keys_env() {
        let keys =
            resolve_api_keys(&[], None, "sk-env-1,sk-env-2", "LLM_API_KEY", "test key").unwrap();

        assert_eq!(keys, vec!["sk-env-1", "sk-env-2"]);
    }

    #[test]
    fn resolve_api_keys_errors_when_no_key_resolves() {
        let error = resolve_api_keys(&[], None, "", "LLM_API_KEY", "test key").unwrap_err();

        assert!(error.to_string().contains("LLM_API_KEY"));
        assert!(!error.to_string().contains("sk-"));
    }
}
