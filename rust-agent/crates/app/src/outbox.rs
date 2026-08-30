//! Reliable platform delivery: enqueue first, then send with a bounded retry window.

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use wechat_summary_core::{models::ImageArtifact, AgentConfig, PrivacyFilter};
use wechat_summary_storage::{DeliveryRecord, DeliveryState, SqliteStateStore};

use crate::{format_error_chain, platform::PlatformWorker, OperationalTask};

const DELIVERY_LEASE_SECONDS: i64 = 10 * 60;

pub(crate) async fn deliver_text(
    config: &AgentConfig,
    task: &OperationalTask,
    client: &PlatformWorker,
    room_id: &str,
    text: &str,
) -> Result<()> {
    let payload = redact_payload(config, text);
    let delivery = task
        .store
        .enqueue_delivery(Some(&task.id), room_id, "text", &payload)
        .context("enqueueing summary text delivery")?;
    if !task.store.claim_delivery(
        &delivery.id,
        delivery.attempts.saturating_add(1),
        DELIVERY_LEASE_SECONDS,
    )? {
        return Ok(());
    }
    match client.send_text(room_id, &payload).await {
        Ok(()) => task.store.update_delivery(
            &delivery.id,
            DeliveryState::Delivered,
            1,
            Utc::now(),
            None,
        )?,
        Err(error) => {
            schedule_failure(config, &task.store, &delivery, &error)?;
            return Err(error).context("sending outboxed summary text");
        }
    }
    Ok(())
}

pub(crate) async fn deliver_image(
    config: &AgentConfig,
    task: &OperationalTask,
    client: &PlatformWorker,
    room_id: &str,
    artifact: &ImageArtifact,
) -> Result<()> {
    let delivery = task
        .store
        .enqueue_delivery(Some(&task.id), room_id, "image", &artifact.path)
        .context("enqueueing summary image delivery")?;
    if !task.store.claim_delivery(
        &delivery.id,
        delivery.attempts.saturating_add(1),
        DELIVERY_LEASE_SECONDS,
    )? {
        return Ok(());
    }
    match client.send_image(room_id, &artifact.path).await {
        Ok(()) => task.store.update_delivery(
            &delivery.id,
            DeliveryState::Delivered,
            1,
            Utc::now(),
            None,
        )?,
        Err(error) => {
            schedule_failure(config, &task.store, &delivery, &error)?;
            return Err(error).context("sending outboxed summary image");
        }
    }
    Ok(())
}

pub(crate) async fn drain(
    config: &AgentConfig,
    store: &SqliteStateStore,
    client: &PlatformWorker,
) -> Result<()> {
    for delivery in store.due_deliveries(8)? {
        let attempts = delivery.attempts.saturating_add(1);
        if !store.claim_delivery(&delivery.id, attempts, DELIVERY_LEASE_SECONDS)? {
            continue;
        }
        let result = match delivery.kind.as_str() {
            "text" => client.send_text(&delivery.room_id, &delivery.payload).await,
            "image" => {
                client
                    .send_image(&delivery.room_id, &delivery.payload)
                    .await
            }
            _ => Err(anyhow::anyhow!(
                "unsupported outbox delivery kind {}",
                delivery.kind
            )),
        };
        match result {
            Ok(()) => store.update_delivery(
                &delivery.id,
                DeliveryState::Delivered,
                attempts,
                Utc::now(),
                None,
            )?,
            Err(error) => schedule_failure(config, store, &delivery, &error)?,
        }
    }
    Ok(())
}

fn schedule_failure(
    config: &AgentConfig,
    store: &SqliteStateStore,
    delivery: &DeliveryRecord,
    error: &anyhow::Error,
) -> Result<()> {
    let message = format_error_chain(error);
    let now = Utc::now();
    if may_have_reached_platform(&message) {
        store.update_delivery(
            &delivery.id,
            DeliveryState::Uncertain,
            delivery.attempts.saturating_add(1),
            now,
            Some(&message),
        )?;
        return Ok(());
    }
    if now - delivery.created_at
        >= Duration::seconds(config.operations.outbox_retry_window_seconds.max(1))
    {
        store.update_delivery(
            &delivery.id,
            DeliveryState::Failed,
            delivery.attempts.saturating_add(1),
            now,
            Some(&message),
        )?;
        return Ok(());
    }
    let delay = (5_i64 * (1_i64 << delivery.attempts.min(6))).min(300);
    store.update_delivery(
        &delivery.id,
        DeliveryState::Pending,
        delivery.attempts.saturating_add(1),
        now + Duration::seconds(delay),
        Some(&message),
    )?;
    Ok(())
}

fn may_have_reached_platform(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("ack") || error.contains("timed out") || error.contains("timeout")
}

fn redact_payload(config: &AgentConfig, value: &str) -> String {
    let mut privacy = config.privacy.clone();
    privacy.redact_enabled = true;
    PrivacyFilter::new(privacy).apply(value)
}
