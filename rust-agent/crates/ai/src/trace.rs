//! AI HTTP trace persistence and secret redaction helpers.
//!
//! Moved verbatim from lib.rs: every function here is a leaf utility used by
//! the provider clients to keep credentials out of trace files and logs.

use std::fs;
use std::path::{Path, PathBuf};

use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use serde_json::{json, Map, Value};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{sha256_hex, AiTraceContext};

pub(crate) struct AiHttpTrace<'a> {
    pub(crate) trace_id: Uuid,
    pub(crate) operation: &'static str,
    pub(crate) context: Option<&'a AiTraceContext>,
    pub(crate) method: &'static str,
    pub(crate) endpoint: &'a str,
    pub(crate) model: Option<&'a str>,
    pub(crate) attempt: usize,
    pub(crate) max_attempts: usize,
    pub(crate) elapsed_ms: u128,
    pub(crate) status: Option<u16>,
    pub(crate) retry: bool,
    pub(crate) retry_after_ms: u64,
    pub(crate) max_tokens: Option<u32>,
    pub(crate) request_body: Option<&'a Value>,
    pub(crate) response_body: Option<&'a str>,
    pub(crate) error: Option<&'a str>,
}

pub(crate) fn write_ai_http_trace(trace_dir: &Path, trace: AiHttpTrace<'_>) {
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

pub(crate) fn write_optional_ai_http_trace(trace_dir: &Option<PathBuf>, trace: AiHttpTrace<'_>) {
    if let Some(trace_dir) = trace_dir {
        write_ai_http_trace(trace_dir, trace);
    }
}

pub(crate) fn redacted_url_source(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .host_str()
                .map(|host| format!("{}://{}", parsed.scheme(), host))
        })
        .unwrap_or_else(|| "<unparseable-url>".to_string())
}

pub(crate) fn redact_endpoint_for_trace(endpoint: &str) -> String {
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

pub(crate) fn full_response_for_log(input: &str) -> String {
    redact_trace_text(input)
}

pub(crate) fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let redacted = redact_trace_text(input);
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if redacted.chars().count() > max_chars {
        output.push_str("...");
    }
    output
}

pub(crate) fn redact_json_for_trace(value: &Value) -> Value {
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

pub(crate) fn is_sensitive_json_key(key: &str) -> bool {
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

pub(crate) fn redact_trace_text(input: &str) -> String {
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

pub(crate) fn redact_secret_like_tokens(input: &str) -> String {
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
