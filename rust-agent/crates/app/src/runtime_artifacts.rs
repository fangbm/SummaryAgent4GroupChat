//! Retention rules for generated runtime artifacts.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use wechat_summary_core::AgentConfig;

pub(crate) fn cleanup(config: &AgentConfig) {
    const MAX_ARTIFACT_BYTES: u64 = 1024 * 1024 * 1024;
    let cutoff = SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(
            u64::from(config.runtime.cleanup_after_days.max(1)) * 24 * 60 * 60,
        ))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let files = known(config);
    for (path, _, modified) in &files {
        if *modified < cutoff {
            let _ = fs::remove_file(path);
        }
    }
    let mut remaining = files
        .into_iter()
        .filter(|(path, _, _)| path.exists())
        .collect::<Vec<_>>();
    let mut total = remaining.iter().map(|(_, size, _)| *size).sum::<u64>();
    if total > MAX_ARTIFACT_BYTES {
        remaining.sort_by_key(|(_, _, modified)| *modified);
        for (path, size, _) in remaining {
            if total <= MAX_ARTIFACT_BYTES {
                break;
            }
            if fs::remove_file(path).is_ok() {
                total = total.saturating_sub(size);
            }
        }
    }
}

#[allow(clippy::type_complexity)]
fn known(config: &AgentConfig) -> Vec<(PathBuf, u64, SystemTime)> {
    let output_dir = PathBuf::from(&config.runtime.output_dir);
    let trace_dir = if config.runtime.ai_trace_dir.trim().is_empty() {
        output_dir.join("ai-traces")
    } else {
        PathBuf::from(config.runtime.ai_trace_dir.trim())
    };
    let long_text_dir = if config.wx4py.long_text_file_dir.trim().is_empty() {
        output_dir.join("long-text")
    } else {
        PathBuf::from(config.wx4py.long_text_file_dir.trim())
    };
    let specs: [(PathBuf, fn(&Path) -> bool); 4] = [
        (output_dir.clone(), is_generated_image),
        (trace_dir, is_ai_trace),
        (output_dir.join("voice-mp3"), is_voice_mp3),
        (long_text_dir, is_long_text),
    ];
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    for (dir, predicate) in specs {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() || !predicate(&path) || !seen.insert(path.clone()) {
                continue;
            }
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            files.push((
                path,
                metadata.len(),
                metadata.modified().unwrap_or(SystemTime::now()),
            ));
        }
    }
    files
}

fn name(path: &Path) -> &str {
    path.file_name().and_then(|name| name.to_str()).unwrap_or_default()
}

fn is_generated_image(path: &Path) -> bool {
    let name = name(path);
    name.starts_with("summary-") && name.ends_with(".png")
}

fn is_ai_trace(path: &Path) -> bool {
    let name = name(path);
    name.ends_with(".json") && name.contains("-attempt-")
}

fn is_voice_mp3(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("mp3"))
}

fn is_long_text(path: &Path) -> bool {
    let name = name(path);
    name.starts_with("summary-") && name.ends_with(".txt")
}
