use std::{env, fs, path::Path, time::Duration};

use base64::{engine::general_purpose, Engine};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::time::{sleep, Instant};
use tracing::{info, warn};
use uuid::Uuid;
use wechat_summary_core::{
    config::{ImageGenConfig, LlmConfig, ProxyConfig},
    ImageArtifact,
};

const CHAT_COMPLETION_MAX_ATTEMPTS: usize = 3;
const CHAT_COMPLETION_RETRY_DELAYS_MS: [u64; 2] = [1_000, 3_000];

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

#[derive(Clone)]
pub struct OpenAiCompatibleLlm {
    config: LlmConfig,
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
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
        })
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

        for attempt in 1..=CHAT_COMPLETION_MAX_ATTEMPTS {
            let started = Instant::now();
            info!(
                base_url = %self.base_url,
                model = %self.model,
                system_chars = system_prompt.chars().count(),
                user_chars = user_content.chars().count(),
                max_tokens = ?max_tokens,
                timeout_seconds = self.config.timeout_seconds,
                attempt,
                max_attempts = CHAT_COMPLETION_MAX_ATTEMPTS,
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
                    let retry = attempt < CHAT_COMPLETION_MAX_ATTEMPTS
                        && should_retry_chat_completion_transport_error(&error);
                    warn!(
                        elapsed_ms,
                        attempt,
                        max_attempts = CHAT_COMPLETION_MAX_ATTEMPTS,
                        retry,
                        error = %error,
                        "LLM chat completion transport failed"
                    );
                    if retry {
                        sleep(chat_completion_retry_delay(attempt)).await;
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
                let retry = attempt < CHAT_COMPLETION_MAX_ATTEMPTS
                    && should_retry_chat_completion_failure(status, &snippet);
                warn!(
                    status = %status,
                    elapsed_ms,
                    attempt,
                    max_attempts = CHAT_COMPLETION_MAX_ATTEMPTS,
                    retry,
                    response = %snippet,
                    "LLM chat completion request failed"
                );
                if retry {
                    sleep(chat_completion_retry_delay(attempt)).await;
                    continue;
                }
                let message = format!("chat completion API returned {status}: {snippet}");
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    return Err(AiError::RateLimited(message));
                }
                return Err(AiError::InvalidResponse(message));
            }
            info!(
                status = %status,
                elapsed_ms,
                attempt,
                max_attempts = CHAT_COMPLETION_MAX_ATTEMPTS,
                "LLM chat completion HTTP request completed"
            );

            let body = response.text().await?;
            let response = serde_json::from_str::<Value>(&body).map_err(|error| {
                let snippet = truncate_for_log(&body, 500);
                warn!(
                    elapsed_ms,
                    response = %snippet,
                    "LLM chat completion JSON parsing failed"
                );
                AiError::InvalidResponse(format!("invalid chat completion JSON: {error}"))
            })?;
            let content = extract_chat_completion_content(&response).ok_or_else(|| {
                let finish_reason = chat_completion_finish_reason(&response);
                let snippet = truncate_for_log(&response.to_string(), 500);
                warn!(
                    finish_reason = %finish_reason,
                    response = %snippet,
                    "LLM chat completion response is missing content"
                );
                AiError::InvalidResponse(format!(
                    "missing chat completion content (finish_reason={finish_reason})"
                ))
            })?;
            info!(
                output_chars = content.chars().count(),
                "LLM chat completion response parsed"
            );
            return Ok(content);
        }

        unreachable!("chat completion retry loop always returns")
    }
}

fn should_retry_chat_completion_failure(status: reqwest::StatusCode, body: &str) -> bool {
    status.is_server_error()
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || body.contains("UPSTREAM_FAILED")
        || body.contains("UPSTREAM_REQUEST_FAILED")
}

fn should_retry_chat_completion_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect()
}

fn chat_completion_retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(
        CHAT_COMPLETION_RETRY_DELAYS_MS
            .get(attempt.saturating_sub(1))
            .copied()
            .unwrap_or_else(|| *CHAT_COMPLETION_RETRY_DELAYS_MS.last().unwrap_or(&1_000)),
    )
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
        payload_object.insert(key, value);
    }
}

pub struct OpenAiImageClient {
    config: ImageGenConfig,
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
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
        })
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

        let started = Instant::now();
        info!(
            base_url = %self.base_url,
            model = %self.model,
            prompt_chars = prompt.chars().count(),
            size = %self.config.size,
            quality = ?self.config.quality,
            resolution = ?self.config.resolution,
            timeout_seconds = self.config.timeout_seconds,
            "image generation request started"
        );
        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.api_key)
            .json(&payload)
            .send()
            .await?;
        let status = response.status();
        let elapsed_ms = started.elapsed().as_millis();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
            let snippet = truncate_for_log(&body, 500);
            warn!(
                status = %status,
                elapsed_ms,
                response = %snippet,
                "image generation request failed"
            );
            return Err(AiError::InvalidResponse(format!(
                "image generation API returned {status}: {snippet}"
            )));
        }
        info!(
            status = %status,
            elapsed_ms,
            "image generation HTTP request completed"
        );

        let response = response.json::<Value>().await?;

        let bytes = self.image_bytes_from_generation_response(response).await?;
        self.write_image(output_dir, &bytes)
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
            let poll_started = Instant::now();
            let response = self
                .client
                .get(format!(
                    "{}/tasks/{}",
                    api_root_url(&self.base_url),
                    task_id
                ))
                .bearer_auth(&self.api_key)
                .send()
                .await?;
            let status = response.status();
            let elapsed_ms = poll_started.elapsed().as_millis();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
                let snippet = truncate_for_log(&body, 500);
                warn!(
                    task_id = %task_id,
                    attempt,
                    status = %status,
                    elapsed_ms,
                    response = %snippet,
                    "image task poll failed"
                );
                return Err(AiError::InvalidResponse(format!(
                    "image task API returned {status}: {snippet}"
                )));
            }
            let response = response.json::<Value>().await?;
            info!(
                task_id = %task_id,
                attempt,
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
        let started = Instant::now();
        info!(source = %source, "image download started");
        let response = self.client.get(url).send().await?;
        let status = response.status();
        let elapsed_ms = started.elapsed().as_millis();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|error| format!("failed to read error response body: {error}"));
            let snippet = truncate_for_log(&body, 500);
            warn!(
                source = %source,
                status = %status,
                elapsed_ms,
                response = %snippet,
                "image download failed"
            );
            return Err(AiError::InvalidResponse(format!(
                "image download returned {status}: {snippet}"
            )));
        }
        let bytes = response.bytes().await?.to_vec();
        info!(
            source = %source,
            elapsed_ms,
            bytes = bytes.len(),
            "image download completed"
        );
        Ok(bytes)
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
        .or_else(|| non_empty_content_value_to_text(choice.pointer("/message/reasoning_content")))
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

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let redacted = redact_secret_like_tokens(input);
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if redacted.chars().count() > max_chars {
        output.push_str("...");
    }
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
            api_key: "test-key".into(),
            base_url: "https://api.apimart.ai/v1".into(),
            model: "gpt-image-2".into(),
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
    fn chat_completion_content_can_fall_back_to_reasoning_content() {
        let response = json!({
            "choices": [{
                "message": {
                    "content": "",
                    "reasoning_content": "fallback text"
                },
                "finish_reason": "stop"
            }]
        });

        assert_eq!(
            extract_chat_completion_content(&response).as_deref(),
            Some("fallback text")
        );
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
