//! Runtime diagnostics: log-file writer with size-based rotation, the
//! tracing env-filter setup, retry-notice formatting, and secret redaction
//! for operator-facing messages.
//!
//! Moved verbatim from main.rs.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use tracing::info;
use tracing_subscriber::{fmt::MakeWriter, EnvFilter};
use wechat_summary_ai::{AiError, AiRetryNotice, RetryNotifier};
use wechat_summary_core::AgentConfig;

pub(crate) fn runtime_env_filter(log_level: &str) -> EnvFilter {
    let trimmed = log_level.trim();
    let level = if trimmed.is_empty() { "info" } else { trimmed };
    let filter = if level.contains(',') || level.eq_ignore_ascii_case("debug") {
        level.to_string()
    } else {
        format!("{level},wx4py_client=warn")
    };
    EnvFilter::new(filter)
}

pub(crate) fn append_runtime_log(config: &AgentConfig, message: &str) {
    let output_dir = std::path::Path::new(&config.runtime.output_dir);
    if fs::create_dir_all(output_dir).is_err() {
        return;
    }
    let path = output_dir.join("wechat-summary-app.log");
    enforce_runtime_log_limit(&path, config.runtime.max_log_mb);
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{} {}", Utc::now().to_rfc3339(), message);
    }
}

pub(crate) fn enforce_runtime_log_limit(path: &Path, max_log_mb: u64) {
    if max_log_mb == 0 {
        return;
    }
    let max_bytes = max_log_mb.saturating_mul(1024).saturating_mul(1024);
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if metadata.len() < max_bytes {
        return;
    }

    // Rotate instead of truncating so recent history survives: current -> .log.1
    // (previous .log.1 is dropped).
    let rotated = path.with_extension("log.1");
    let _ = fs::remove_file(&rotated);
    if fs::rename(path, &rotated).is_ok() {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(
                file,
                "{} log rotated because size reached {} bytes (limit={}MB)",
                Utc::now().to_rfc3339(),
                metadata.len(),
                max_log_mb
            );
        }
    }
}

pub(crate) fn retry_log_notifier(config: &AgentConfig, room_id: String) -> RetryNotifier {
    let config = config.clone();
    Arc::new(move |notice: AiRetryNotice| {
        let config = config.clone();
        let room_id = room_id.clone();
        Box::pin(async move {
            let reason = retry_notice_reason(&notice.reason, 160);
            info!(
                room_id = %room_id,
                operation = notice.operation,
                attempt = notice.attempt,
                max_attempts = notice.max_attempts,
                retry_after_ms = notice.retry_after_ms,
                reason = %reason,
                "AI request retry scheduled"
            );
            append_runtime_log(&config, &format_retry_log_entry(&room_id, &notice));
        })
    })
}

pub(crate) fn format_retry_log_entry(room_id: &str, notice: &AiRetryNotice) -> String {
    format!(
        "ai retry scheduled room={} operation={} retry={}/{} wait_ms={} reason={}",
        room_id,
        notice.operation,
        retry_notice_retry_index(notice),
        retry_notice_max_retries(notice),
        notice.retry_after_ms,
        retry_notice_reason(&notice.reason, 160)
    )
}

pub(crate) fn retry_notice_max_retries(notice: &AiRetryNotice) -> usize {
    notice.max_attempts.saturating_sub(1).max(1)
}

pub(crate) fn retry_notice_retry_index(notice: &AiRetryNotice) -> usize {
    notice.attempt.min(retry_notice_max_retries(notice)).max(1)
}

pub(crate) fn retry_notice_reason(reason: &str, max_chars: usize) -> String {
    let compact = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    let redacted = redact_secret_like_tokens(&compact);
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if redacted.chars().count() > max_chars {
        output.push_str("...");
    }
    if output.is_empty() {
        "unknown".to_string()
    } else {
        output
    }
}

pub(crate) fn compact_ai_error_for_runtime(error: &AiError) -> String {
    retry_notice_reason(&error.to_string(), 700)
}

pub(crate) fn compact_error_for_runtime(error_message: &str, max_chars: usize) -> String {
    let compact = error_message
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let redacted = redact_secret_like_tokens(&compact);
    let char_count = redacted.chars().count();
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if char_count > max_chars {
        output.push_str("...");
    }
    if output.is_empty() {
        "unknown".to_string()
    } else {
        output
    }
}

pub(crate) fn format_failure_message_for_chat(label: &str, error_message: &str) -> String {
    let reason = compact_error_for_chat(error_message, 700);
    format!("{label}：{reason}")
}

pub(crate) fn compact_error_for_chat(error_message: &str, max_chars: usize) -> String {
    let compact = error_message
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let redacted = redact_secret_like_tokens(&compact);
    let char_count = redacted.chars().count();
    let mut output = redacted.chars().take(max_chars).collect::<String>();
    if char_count > max_chars {
        output.push_str("...（完整错误见终端/日志）");
    }
    if output.is_empty() {
        "unknown".to_string()
    } else {
        output
    }
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

#[derive(Clone)]
pub(crate) struct RuntimeTraceWriter {
    path: PathBuf,
    max_log_mb: u64,
}

impl RuntimeTraceWriter {
    pub(crate) fn new(config: &AgentConfig) -> Self {
        let output_dir = std::path::Path::new(&config.runtime.output_dir);
        let _ = fs::create_dir_all(output_dir);
        Self {
            path: output_dir.join("wechat-summary-app.log"),
            max_log_mb: config.runtime.max_log_mb,
        }
    }
}

impl<'a> MakeWriter<'a> for RuntimeTraceWriter {
    type Writer = RuntimeTraceGuard;

    fn make_writer(&'a self) -> Self::Writer {
        enforce_runtime_log_limit(&self.path, self.max_log_mb);
        RuntimeTraceGuard {
            file: OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .ok(),
        }
    }
}

pub(crate) struct RuntimeTraceGuard {
    file: Option<fs::File>,
}

impl Write for RuntimeTraceGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = io::stdout().write_all(buf);
        if let Some(file) = &mut self.file {
            let _ = file.write_all(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let _ = io::stdout().flush();
        if let Some(file) = &mut self.file {
            let _ = file.flush();
        }
        Ok(())
    }
}

pub(crate) fn append_startup_error(config_path: &str, message: &str) {
    if let Ok(config) = AgentConfig::from_path(config_path) {
        append_runtime_log(&config, message);
        return;
    }

    let output_dir = std::path::Path::new("runtime").join("rust-output");
    if fs::create_dir_all(&output_dir).is_err() {
        return;
    }
    let path = output_dir.join("wechat-summary-app.log");
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{} {}", Utc::now().to_rfc3339(), message);
    }
}
