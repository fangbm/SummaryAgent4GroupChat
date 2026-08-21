//! Shared HTTP retry policy: delay computation, jitter, Retry-After parsing,
//! the serial rate-limit queue, and the total retry-delay budget.
//!
//! Moved verbatim from lib.rs.

use std::time::Duration;

use chrono::Utc;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};
use tracing::info;

use crate::{AiRetryNotice, RetryNotifier};

pub(crate) const HTTP_RETRY_INITIAL_DELAY_MS: u64 = 1_000;
pub(crate) const HTTP_RETRY_MAX_DELAY_MS: u64 = 30_000;
pub(crate) const HTTP_RETRY_TOTAL_DELAY_BUDGET_MS: u64 = 180_000;

pub(crate) static HTTP_RATE_LIMIT_RETRY_QUEUE: std::sync::OnceLock<Mutex<()>> =
    std::sync::OnceLock::new();

#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryBudget {
    pub(crate) started: Instant,
    pub(crate) deadline: Option<Instant>,
    pub(crate) retry_delay_budget: Option<Duration>,
}

impl RetryBudget {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            deadline: None,
            retry_delay_budget: Some(Duration::from_millis(HTTP_RETRY_TOTAL_DELAY_BUDGET_MS)),
        }
    }

    pub(crate) fn with_deadline(deadline: Instant) -> Self {
        Self {
            started: Instant::now(),
            deadline: Some(deadline),
            retry_delay_budget: None,
        }
    }

    pub(crate) fn allows(&self, delay_ms: u64) -> bool {
        delay_ms <= self.remaining_ms()
    }

    pub(crate) fn remaining_ms(&self) -> u64 {
        self.remaining_duration().as_millis() as u64
    }

    pub(crate) fn remaining_duration(&self) -> Duration {
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

    pub(crate) fn deadline_exhausted(&self) -> bool {
        self.deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(false)
    }
}

pub(crate) fn should_retry_chat_completion_failure(
    status: reqwest::StatusCode,
    body: &str,
) -> bool {
    should_retry_http_failure(status, body)
}

pub(crate) fn should_retry_http_failure(status: reqwest::StatusCode, body: &str) -> bool {
    if is_content_policy_block(status, body) {
        return false;
    }
    status.is_server_error()
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || body.contains("UPSTREAM_FAILED")
        || body.contains("UPSTREAM_REQUEST_FAILED")
}

pub(crate) fn is_content_policy_block(status: reqwest::StatusCode, body: &str) -> bool {
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

pub(crate) fn should_retry_http_transport_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect()
}

pub(crate) fn http_max_attempts(retry_attempts: usize) -> usize {
    retry_attempts.saturating_add(1)
}

pub(crate) fn http_retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(http_retry_delay_ms(attempt))
}

pub(crate) fn http_retry_delay_ms_for_status(
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

pub(crate) fn deterministic_retry_jitter_ms(base_ms: u64, attempt: usize) -> u64 {
    let window = (base_ms / 5).min(5_000);
    if window == 0 {
        return 0;
    }
    let seed = (attempt as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    seed % (window + 1)
}

pub(crate) fn retry_after_ms_from_headers(
    headers: &HeaderMap,
    now: chrono::DateTime<Utc>,
) -> Option<u64> {
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

pub(crate) async fn sleep_for_retry_budget(retry_budget: &RetryBudget, delay: Duration) -> bool {
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

pub(crate) fn http_retry_delay_ms(attempt: usize) -> u64 {
    let mut delay = HTTP_RETRY_INITIAL_DELAY_MS;
    for _ in 1..attempt {
        delay = delay.saturating_mul(2).min(HTTP_RETRY_MAX_DELAY_MS);
        if delay == HTTP_RETRY_MAX_DELAY_MS {
            break;
        }
    }
    delay
}

pub(crate) async fn wait_before_status_retry(
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

pub(crate) async fn wait_in_rate_limit_retry_queue(
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

pub(crate) async fn notify_retry(
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

pub(crate) fn retry_reason_from_transport_error(error: &reqwest::Error) -> String {
    crate::trace::truncate_for_log(&error.to_string(), 300)
}

pub(crate) fn retry_reason_from_status(status: reqwest::StatusCode, snippet: &str) -> String {
    crate::trace::truncate_for_log(&format!("{status}: {snippet}"), 300)
}
