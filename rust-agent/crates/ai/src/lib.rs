use std::{
    env, fs,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};

use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};
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
const HTTP_RATE_LIMIT_QUEUE_DELAY_MS: u64 = 60_000;

static HTTP_RATE_LIMIT_RETRY_QUEUE: OnceLock<Mutex<()>> = OnceLock::new();

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
    #[error("rate limited: {0}")]
    RateLimited(String),
    #[error("image task failed: {0}")]
    ImageTaskFailed(String),
    #[error("image task timed out after {0} seconds")]
    ImageTaskTimeout(u64),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
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

#[derive(Clone)]
pub struct OpenAiCompatibleLlm {
    config: LlmConfig,
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
    retry_notifier: Option<RetryNotifier>,
    trace_dir: Option<PathBuf>,
}

impl OpenAiCompatibleLlm {
    pub fn new(config: LlmConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let api_key = config_value_or_env(
            config.api_key.as_deref(),
            &config.api_key_env,
            "LLM API key",
        )
        .map_err(|_| missing_api_key(&config.api_key_env))?;
        let base_url = config_value_or_env(
            config.base_url.as_deref(),
            &config.base_url_env,
            "LLM base URL",
        )?;
        let model =
            config_value_or_env(config.model.as_deref(), &config.model_env, "LLM model name")?;
        let client = http_client(config.timeout_seconds, proxy)?;
        Ok(Self {
            config,
            client,
            api_key,
            base_url,
            model,
            retry_notifier: None,
            trace_dir: None,
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

        let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
        for attempt in 1..=max_attempts {
            let trace_id = Uuid::new_v4();
            let started = Instant::now();
            info!(
                trace_id = %trace_id,
                base_url = %self.base_url,
                model = %self.model,
                system_chars = system_prompt.chars().count(),
                user_chars = user_content.chars().count(),
                max_tokens = ?max_tokens,
                timeout_seconds = self.config.timeout_seconds,
                attempt,
                max_attempts,
                "LLM chat completion request started"
            );
            let response = match self
                .client
                .post(&endpoint)
                .bearer_auth(&self.api_key)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let elapsed_ms = started.elapsed().as_millis();
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
                    self.write_chat_completion_trace(ChatCompletionTrace {
                        trace_id,
                        endpoint: &endpoint,
                        attempt,
                        max_attempts,
                        elapsed_ms,
                        status: None,
                        retry,
                        retry_after_ms,
                        max_tokens,
                        request_body: &payload,
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
            let elapsed_ms = started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
                self.write_chat_completion_trace(ChatCompletionTrace {
                    trace_id,
                    endpoint: &endpoint,
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    status: Some(status.as_u16()),
                    retry,
                    retry_after_ms,
                    max_tokens,
                    request_body: &payload,
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
                    wait_before_status_retry("LLM chat completion", status, attempt, max_attempts)
                        .await;
                    continue;
                }
                let message = format!("chat completion API returned {status}: {response_for_log}");
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

            let body = response.text().await?;
            self.write_chat_completion_trace(ChatCompletionTrace {
                trace_id,
                endpoint: &endpoint,
                attempt,
                max_attempts,
                elapsed_ms,
                status: Some(status.as_u16()),
                retry: false,
                retry_after_ms: 0,
                max_tokens,
                request_body: &payload,
                response_body: Some(&body),
                error: None,
            });
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let response_for_log = full_response_for_log(&body);
                warn!(
                    trace_id = %trace_id,
                    elapsed_ms,
                    response = %response_for_log,
                    "LLM chat completion JSON parsing failed"
                );
                AiError::InvalidResponse(format!(
                    "invalid chat completion JSON: {error}; response={response_for_log}"
                ))
            })?;
            let content = extract_chat_completion_content(&response).ok_or_else(|| {
                let finish_reason = chat_completion_finish_reason(&response);
                let response_for_log = full_response_for_log(&response.to_string());
                warn!(
                    trace_id = %trace_id,
                    finish_reason = %finish_reason,
                    response = %response_for_log,
                    "LLM chat completion response is missing content"
                );
                AiError::InvalidResponse(format!(
                    "missing chat completion content (finish_reason={finish_reason}); response={response_for_log}"
                ))
            })?;
            info!(
                trace_id = %trace_id,
                output_chars = content.chars().count(),
                "LLM chat completion response parsed"
            );
            return Ok(content);
        }

        unreachable!("chat completion retry loop always returns")
    }

    fn write_chat_completion_trace(&self, trace: ChatCompletionTrace<'_>) {
        let Some(trace_dir) = &self.trace_dir else {
            return;
        };
        if let Err(error) = fs::create_dir_all(trace_dir) {
            warn!(error = %error, path = %trace_dir.display(), "failed to create AI trace directory");
            return;
        }
        let timestamp = Utc::now();
        let file_name = format!(
            "{}-llm-chat-{}-attempt-{}.json",
            timestamp.format("%Y%m%d-%H%M%S-%3f"),
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
                .map(|body| Value::String(redact_secret_like_tokens(body)))
                .unwrap_or(Value::Null),
        };
        let payload = json!({
            "trace_id": trace.trace_id.to_string(),
            "operation": "llm_chat_completion",
            "created_at_utc": timestamp.to_rfc3339(),
            "endpoint": trace.endpoint,
            "model": self.model,
            "attempt": trace.attempt,
            "max_attempts": trace.max_attempts,
            "elapsed_ms": trace.elapsed_ms,
            "status": trace.status,
            "retry": trace.retry,
            "retry_after_ms": trace.retry_after_ms,
            "max_tokens": trace.max_tokens,
            "request_body": redact_json_for_trace(trace.request_body),
            "response_body": response_body,
            "error": trace.error.map(redact_secret_like_tokens),
        });
        match serde_json::to_string_pretty(&payload) {
            Ok(text) => {
                if let Err(error) = fs::write(&path, text) {
                    warn!(error = %error, path = %path.display(), "failed to write AI trace");
                } else {
                    info!(trace_id = %trace.trace_id, path = %path.display(), "AI trace written");
                }
            }
            Err(error) => warn!(error = %error, "failed to serialize AI trace"),
        }
    }
}

struct ChatCompletionTrace<'a> {
    trace_id: Uuid,
    endpoint: &'a str,
    attempt: usize,
    max_attempts: usize,
    elapsed_ms: u128,
    status: Option<u16>,
    retry: bool,
    retry_after_ms: u64,
    max_tokens: Option<u32>,
    request_body: &'a Value,
    response_body: Option<&'a str>,
    error: Option<&'a str>,
}

pub struct OpenAiVisionCaptionClient {
    config: ImageCaptionConfig,
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
    retry_notifier: Option<RetryNotifier>,
}

impl OpenAiVisionCaptionClient {
    pub fn new(config: ImageCaptionConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let api_key = config_value_or_env(
            config.api_key.as_deref(),
            &config.api_key_env,
            "image caption API key",
        )
        .map_err(|_| missing_api_key(&config.api_key_env))?;
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
        Ok(Self {
            config,
            client,
            api_key,
            base_url,
            model,
            retry_notifier: None,
        })
    }

    pub fn with_retry_notifier(mut self, retry_notifier: RetryNotifier) -> Self {
        self.retry_notifier = Some(retry_notifier);
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
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
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
                let retry = attempt < max_attempts && should_retry_http_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
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
                    wait_before_status_retry(
                        "image caption remote image download",
                        status,
                        attempt,
                        max_attempts,
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
        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(
                base_url = %self.base_url,
                model = %self.model,
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
                .bearer_auth(&self.api_key)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
                    warn!(
                        elapsed_ms = started.elapsed().as_millis(),
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
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
                warn!(
                    status = %status,
                    elapsed_ms = started.elapsed().as_millis(),
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
                    wait_before_status_retry(
                        "image caption request",
                        status,
                        attempt,
                        max_attempts,
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
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let response_for_log = full_response_for_log(&body);
                warn!(
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
                    finish_reason = %finish_reason,
                    response = %response_for_log,
                    "image caption response is missing content"
                );
                AiError::InvalidResponse(format!(
                    "missing image caption content (finish_reason={finish_reason}); response={response_for_log}"
                ))
            })?;
            info!(
                output_chars = content.chars().count(),
                "image caption response parsed"
            );
            return Ok(content);
        }

        unreachable!("image caption retry loop always returns")
    }
}

pub struct OpenAiAudioTranscriptionClient {
    config: VoiceTranscriptionConfig,
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
}

pub struct OpenAiVideoCaptionClient {
    config: VideoCaptionConfig,
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
}

impl OpenAiVideoCaptionClient {
    pub fn new(config: VideoCaptionConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let api_key = config_value_or_env(
            config.api_key.as_deref(),
            &config.api_key_env,
            "video caption API key",
        )
        .map_err(|_| missing_api_key(&config.api_key_env))?;
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
        Ok(Self {
            config,
            client,
            api_key,
            base_url,
            model,
        })
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
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
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
                let retry = attempt < max_attempts && should_retry_http_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
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
                    wait_before_status_retry(
                        "video caption remote video download",
                        status,
                        attempt,
                        max_attempts,
                    )
                    .await;
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
        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(
                base_url = %self.base_url,
                model = %self.model,
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
                .bearer_auth(&self.api_key)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
                    warn!(
                        elapsed_ms = started.elapsed().as_millis(),
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
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
                warn!(
                    status = %status,
                    elapsed_ms = started.elapsed().as_millis(),
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "video caption request failed"
                );
                if retry {
                    wait_before_status_retry(
                        "video caption request",
                        status,
                        attempt,
                        max_attempts,
                    )
                    .await;
                    continue;
                }
                let message = format!("video caption API returned {status}: {response_for_log}");
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(AiError::RateLimited(message));
                }
                return Err(AiError::InvalidResponse(message));
            }

            let body = response.text().await?;
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let response_for_log = full_response_for_log(&body);
                warn!(
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
                    finish_reason = %finish_reason,
                    response = %response_for_log,
                    "video caption response is missing content"
                );
                AiError::InvalidResponse(format!(
                    "missing video caption content (finish_reason={finish_reason}); response={response_for_log}"
                ))
            })?;
            info!(
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
        let api_key = config_value_or_env(
            config.api_key.as_deref(),
            &config.api_key_env,
            "voice transcription API key",
        )
        .map_err(|_| missing_api_key(&config.api_key_env))?;
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
        Ok(Self {
            config,
            client,
            api_key,
            base_url,
            model,
        })
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

        for attempt in 1..=max_attempts {
            let started = Instant::now();
            let mut file_part =
                reqwest::multipart::Part::bytes(bytes.clone()).file_name(file_name.clone());
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

            info!(
                base_url = %self.base_url,
                model = %self.model,
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
                .bearer_auth(&self.api_key)
                .multipart(form)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
                    warn!(
                        elapsed_ms = started.elapsed().as_millis(),
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
            let body = response.text().await?;
            if !status.is_success() {
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
                warn!(
                    status = %status,
                    elapsed_ms = started.elapsed().as_millis(),
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "voice transcription request failed"
                );
                if retry {
                    wait_before_status_retry(
                        "voice transcription request",
                        status,
                        attempt,
                        max_attempts,
                    )
                    .await;
                    continue;
                }
                let message =
                    format!("voice transcription API returned {status}: {response_for_log}");
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(AiError::RateLimited(message));
                }
                return Err(AiError::InvalidResponse(message));
            }

            let text = extract_transcription_text(&body, &self.config.response_format)?;
            info!(
                output_chars = text.chars().count(),
                elapsed_ms = started.elapsed().as_millis(),
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

        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(
                base_url = %self.base_url,
                model = %self.model,
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
                .bearer_auth(&self.api_key)
                .header(ACCEPT, "text/event-stream")
                .header(CONTENT_TYPE, "application/json")
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
                    warn!(
                        elapsed_ms = started.elapsed().as_millis(),
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
            let body = response.text().await?;
            if !status.is_success() {
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry = attempt < max_attempts
                    && should_retry_chat_completion_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
                warn!(
                    status = %status,
                    elapsed_ms = started.elapsed().as_millis(),
                    attempt,
                    max_attempts,
                    retry,
                    retry_after_ms,
                    response = %response_for_log,
                    "StepFun voice transcription request failed"
                );
                if retry {
                    wait_before_status_retry(
                        "StepFun voice transcription request",
                        status,
                        attempt,
                        max_attempts,
                    )
                    .await;
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

            let text = extract_stepfun_asr_text(&body)?;
            info!(
                output_chars = text.chars().count(),
                elapsed_ms = started.elapsed().as_millis(),
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

fn http_retry_delay_ms_for_status(status: reqwest::StatusCode, attempt: usize) -> u64 {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        HTTP_RATE_LIMIT_QUEUE_DELAY_MS
    } else {
        http_retry_delay_ms(attempt)
    }
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
) {
    let delay_ms = http_retry_delay_ms_for_status(status, attempt);
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let queue = HTTP_RATE_LIMIT_RETRY_QUEUE.get_or_init(|| Mutex::new(()));
        info!(
            operation,
            attempt,
            max_attempts,
            wait_ms = delay_ms,
            "AI request rate limited; waiting in serial retry queue"
        );
        let _guard = queue.lock().await;
        sleep(Duration::from_millis(delay_ms)).await;
    } else {
        sleep(Duration::from_millis(delay_ms)).await;
    }
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

pub struct OpenAiImageClient {
    config: ImageGenConfig,
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
    retry_notifier: Option<RetryNotifier>,
}

impl OpenAiImageClient {
    pub fn new(config: ImageGenConfig, proxy: &ProxyConfig) -> Result<Self, AiError> {
        let api_key = config_value_or_env(
            config.api_key.as_deref(),
            &config.api_key_env,
            "image API key",
        )
        .map_err(|_| missing_api_key(&config.api_key_env))?;
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
        Ok(Self {
            config,
            client,
            api_key,
            base_url,
            model,
            retry_notifier: None,
        })
    }

    pub fn with_retry_notifier(mut self, retry_notifier: RetryNotifier) -> Self {
        self.retry_notifier = Some(retry_notifier);
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
        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(
                base_url = %self.base_url,
                model = %self.model,
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
                .bearer_auth(&self.api_key)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
                    warn!(
                        elapsed_ms = started.elapsed().as_millis(),
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
            let elapsed_ms = started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry = attempt < max_attempts && should_retry_http_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
                warn!(
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
                    wait_before_status_retry(
                        "image generation request",
                        status,
                        attempt,
                        max_attempts,
                    )
                    .await;
                    continue;
                }
                return Err(AiError::InvalidResponse(format!(
                    "image generation API returned {status}: {response_for_log}"
                )));
            }
            info!(
                status = %status,
                elapsed_ms,
                attempt,
                max_attempts,
                "image generation HTTP request completed"
            );

            let response = response.json::<Value>().await?;
            let bytes = self.image_bytes_from_generation_response(response).await?;
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
    ) -> Result<Vec<u8>, AiError> {
        if let Some(image) = direct_image_data(&response) {
            info!("image generation returned direct image data");
            return self.image_bytes_from_data(image).await;
        }
        if let Some(task_id) = apimart_task_id(&response) {
            info!(task_id = %task_id, "image generation task accepted; polling started");
            return self.poll_apimart_task(&task_id).await;
        }
        Err(AiError::InvalidResponse(format!(
            "image response has neither direct image data nor task_id: {response}"
        )))
    }

    async fn poll_apimart_task(&self, task_id: &str) -> Result<Vec<u8>, AiError> {
        let deadline = Instant::now() + Duration::from_secs(self.config.timeout_seconds.max(1));
        info!(
            task_id = %task_id,
            initial_delay_seconds = self.config.poll_initial_delay_seconds.min(self.config.timeout_seconds),
            poll_interval_seconds = self.config.poll_interval_seconds.max(1),
            "image task initial poll delay started"
        );
        sleep(Duration::from_secs(
            self.config
                .poll_initial_delay_seconds
                .min(self.config.timeout_seconds),
        ))
        .await;

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let max_attempts = http_max_attempts(self.config.retry_5xx_attempts);
            let response = {
                let mut final_response = None;
                for http_attempt in 1..=max_attempts {
                    let poll_started = Instant::now();
                    let response = match self
                        .client
                        .get(format!(
                            "{}/tasks/{}",
                            api_root_url(&self.base_url),
                            task_id
                        ))
                        .bearer_auth(&self.api_key)
                        .send()
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            let retry = http_attempt < max_attempts
                                && should_retry_http_transport_error(&error);
                            let retry_after_ms = retry
                                .then(|| http_retry_delay_ms(http_attempt))
                                .unwrap_or(0);
                            warn!(
                                task_id = %task_id,
                                attempt,
                                http_attempt,
                                max_attempts,
                                retry,
                                retry_after_ms,
                                elapsed_ms = poll_started.elapsed().as_millis(),
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
                                sleep(http_retry_delay(http_attempt)).await;
                                continue;
                            }
                            return Err(AiError::Http(error));
                        }
                    };
                    let status = response.status();
                    let elapsed_ms = poll_started.elapsed().as_millis();
                    if !status.is_success() {
                        let body = response.text().await.unwrap_or_else(|error| {
                            format!("failed to read error response body: {error}")
                        });
                        let snippet = truncate_for_log(&body, 500);
                        let response_for_log = full_response_for_log(&body);
                        let retry = http_attempt < max_attempts
                            && should_retry_http_failure(status, &snippet);
                        let retry_after_ms = retry
                            .then(|| http_retry_delay_ms_for_status(status, http_attempt))
                            .unwrap_or(0);
                        warn!(
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
                            wait_before_status_retry(
                                "image task poll",
                                status,
                                http_attempt,
                                max_attempts,
                            )
                            .await;
                            continue;
                        }
                        return Err(AiError::InvalidResponse(format!(
                            "image task API returned {status}: {response_for_log}"
                        )));
                    }
                    final_response = Some((response, elapsed_ms, http_attempt));
                    break;
                }
                final_response
                    .expect("image task poll retry loop always returns a response or error")
            };
            let (response, elapsed_ms, http_attempt) = response;
            let response = response.json::<Value>().await?;
            info!(
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
            sleep(Duration::from_secs(
                self.config.poll_interval_seconds.max(1),
            ))
            .await;
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
        for attempt in 1..=max_attempts {
            let started = Instant::now();
            info!(source = %source, attempt, max_attempts, "image download started");
            let response = match self.client.get(&url).send().await {
                Ok(response) => response,
                Err(error) => {
                    let retry = attempt < max_attempts && should_retry_http_transport_error(&error);
                    let retry_after_ms = retry.then(|| http_retry_delay_ms(attempt)).unwrap_or(0);
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
            let elapsed_ms = started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                let response_for_log = full_response_for_log(&body);
                let retry = attempt < max_attempts && should_retry_http_failure(status, &snippet);
                let retry_after_ms = retry
                    .then(|| http_retry_delay_ms_for_status(status, attempt))
                    .unwrap_or(0);
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
                    wait_before_status_retry("image download", status, attempt, max_attempts).await;
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

        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let sha256 = format!("{:x}", hasher.finalize());

        let artifact = ImageArtifact {
            path: path.to_string_lossy().to_string(),
            mime_type: "image/png".to_string(),
            size_bytes: bytes.len() as u64,
            sha256,
        };
        info!(
            path = %artifact.path,
            size_bytes = artifact.size_bytes,
            "summary image written"
        );
        Ok(artifact)
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

fn full_response_for_log(input: &str) -> String {
    redact_secret_like_tokens(input)
}

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let redacted = redact_secret_like_tokens(input);
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if redacted.chars().count() > max_chars {
        output.push_str("...");
    }
    output
}

fn redact_json_for_trace(value: &Value) -> Value {
    redact_json_value_for_trace(value, None)
}

fn redact_json_value_for_trace(value: &Value, key: Option<&str>) -> Value {
    if key.map(is_sensitive_json_key).unwrap_or(false) {
        return Value::String("<redacted-secret>".to_string());
    }

    match value {
        Value::String(text) => Value::String(redact_secret_like_tokens(text)),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| redact_json_value_for_trace(item, None))
                .collect(),
        ),
        Value::Object(map) => {
            let mut redacted = Map::new();
            for (key, value) in map {
                redacted.insert(
                    key.clone(),
                    redact_json_value_for_trace(value, Some(key.as_str())),
                );
            }
            Value::Object(redacted)
        }
        _ => value.clone(),
    }
}

fn is_sensitive_json_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '_'], "");
    normalized.contains("apikey")
        || normalized.contains("authorization")
        || normalized.contains("accesstoken")
        || normalized == "token"
        || normalized.contains("secret")
        || normalized.contains("password")
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
            api_key: "test-key".into(),
            base_url: "https://api.apimart.ai/v1".into(),
            model: "gpt-image-2".into(),
            retry_notifier: None,
        }
    }

    #[test]
    fn trace_redaction_removes_secret_like_values() {
        let payload = json!({
            "api_key": "sk-test-secret-value",
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
        assert_eq!(redacted["headers"]["Authorization"], "<redacted-secret>");
        assert_eq!(redacted["headers"]["x-safe"], "plain");
        assert!(!redacted["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("sk-inline-secret-value"));
    }

    fn image_caption_config() -> ImageCaptionConfig {
        ImageCaptionConfig {
            enabled: true,
            provider: "openai_compatible".into(),
            api_key: None,
            api_key_env: "IMAGE_CAPTION_API_KEY".into(),
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
            request_body_overrides: Default::default(),
        }
    }

    fn voice_transcription_config() -> VoiceTranscriptionConfig {
        VoiceTranscriptionConfig {
            enabled: true,
            provider: "openai_compatible".into(),
            api_key: None,
            api_key_env: "VOICE_TRANSCRIPTION_API_KEY".into(),
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
            request_body_overrides: Default::default(),
        }
    }

    fn image_config() -> ImageGenConfig {
        ImageGenConfig {
            enabled: true,
            provider: "openai".into(),
            api_key: None,
            api_key_env: "IMAGE_API_KEY".into(),
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

        assert_eq!(client.api_key, "persisted-key");
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

        assert_eq!(client.api_key, "persisted-voice-key");
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

        assert_eq!(client.api_key, "persisted-key");
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

        assert_eq!(client.api_key, direct_key);
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
    fn rate_limit_retry_uses_serial_queue_delay() {
        assert_eq!(
            http_retry_delay_ms_for_status(reqwest::StatusCode::TOO_MANY_REQUESTS, 1),
            60_000
        );
        assert_eq!(
            http_retry_delay_ms_for_status(reqwest::StatusCode::TOO_MANY_REQUESTS, 5),
            60_000
        );
        assert_eq!(
            http_retry_delay_ms_for_status(reqwest::StatusCode::BAD_GATEWAY, 3),
            4_000
        );
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
}
