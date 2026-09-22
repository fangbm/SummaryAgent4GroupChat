//! Shared AI client tracing and request context helpers.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use wechat_summary_ai::{AiTraceContext, OpenAiCompatibleLlm};
use wechat_summary_core::AgentConfig;

pub(crate) fn configure_llm_tracing(
    llm: OpenAiCompatibleLlm,
    config: &AgentConfig,
) -> Result<OpenAiCompatibleLlm> {
    Ok(match ai_trace_dir(config)? {
        Some(trace_dir) => llm.with_trace_dir(trace_dir),
        None => llm,
    })
}

pub(crate) fn ai_trace_dir(config: &AgentConfig) -> Result<Option<PathBuf>> {
    if !config.runtime.ai_trace_enabled {
        return Ok(None);
    }
    let trace_dir = if config.runtime.ai_trace_dir.trim().is_empty() {
        Path::new(&config.runtime.output_dir).join("ai-traces")
    } else {
        PathBuf::from(config.runtime.ai_trace_dir.trim())
    };
    fs::create_dir_all(&trace_dir)
        .with_context(|| format!("creating AI trace directory {}", trace_dir.display()))?;
    Ok(Some(trace_dir))
}

pub(crate) fn ai_trace_context(room_id: &str, stage: &str) -> AiTraceContext {
    AiTraceContext {
        room_id: Some(room_id.to_string()),
        stage: Some(stage.to_string()),
        ..Default::default()
    }
}

pub(crate) fn ai_trace_context_for_chunk(
    room_id: &str,
    stage: &str,
    chunk_index: usize,
    chunk_total: usize,
) -> AiTraceContext {
    AiTraceContext {
        room_id: Some(room_id.to_string()),
        stage: Some(stage.to_string()),
        chunk_index: Some(chunk_index),
        chunk_total: Some(chunk_total),
        ..Default::default()
    }
}

pub(crate) fn ai_trace_context_for_item(
    room_id: &str,
    stage: &str,
    item_index: usize,
    item_total: usize,
) -> AiTraceContext {
    AiTraceContext {
        room_id: Some(room_id.to_string()),
        stage: Some(stage.to_string()),
        item_index: Some(item_index),
        item_total: Some(item_total),
        ..Default::default()
    }
}
