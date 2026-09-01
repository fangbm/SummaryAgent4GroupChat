//! Retention rules for generated runtime artifacts.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use wechat_summary_core::{models::ImageArtifact, AgentConfig};

static RANDOM_IMAGE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Pick one still-usable NovelAI image from the dedicated runtime output pool.
/// Only the manual NovelAI command writes `summary-*.png` files below
/// `<output_dir>/nai`; summary-image generation keeps using the root output
/// directory selected by `[image_gen]`.
pub(crate) fn random_generated_image(config: &AgentConfig) -> Result<Option<ImageArtifact>> {
    let output_dir = Path::new(&config.runtime.output_dir).join("nai");
    if let Some(artifact) = random_generated_image_from_dir(&output_dir)? {
        return Ok(Some(artifact));
    }

    // The legacy launcher could start the agent with an arbitrary working
    // directory. Keep existing local NAI artifacts usable after that upgrade.
    let fallback_dir = installed_runtime_output_dir().join("nai");
    if fallback_dir != output_dir {
        return random_generated_image_from_dir(&fallback_dir);
    }
    Ok(None)
}

fn random_generated_image_from_dir(output_dir: &Path) -> Result<Option<ImageArtifact>> {
    let entries = match fs::read_dir(output_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("reading generated image directory {}", output_dir.display())
            })
        }
    };
    let mut candidates = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let mime_type = replayable_image_mime_type(&path)?;
            if !path.is_file() {
                return None;
            }
            let metadata = entry.metadata().ok()?;
            (metadata.len() > 0).then_some((path, metadata.len(), mime_type))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        tracing::info!(directory = %output_dir.display(), candidate_count = 0, "manual random image lookup completed");
        return Ok(None);
    }
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // The monotonic counter ensures simultaneous room events do not all select
    // the same artifact merely because they share a clock tick.
    let sequence = RANDOM_IMAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let index = ((now ^ u128::from(sequence) ^ u128::from(std::process::id()))
        % candidates.len() as u128) as usize;
    let (path, size_bytes, mime_type) = candidates.swap_remove(index);
    tracing::info!(
        directory = %output_dir.display(),
        candidate_count = candidates.len() + 1,
        size_bytes,
        "manual random image selected"
    );
    Ok(Some(ImageArtifact {
        path: path.to_string_lossy().into_owned(),
        mime_type: mime_type.into(),
        size_bytes,
        // Delivery does not consume the digest. Avoid re-reading a potentially
        // large image merely to replay an already written artifact.
        sha256: String::new(),
    }))
}

fn installed_runtime_output_dir() -> PathBuf {
    let Ok(executable) = std::env::current_exe() else {
        return PathBuf::from("runtime/rust-output");
    };
    let Some(executable_dir) = executable.parent() else {
        return PathBuf::from("runtime/rust-output");
    };
    let root = executable_dir
        .file_name()
        .is_some_and(|name| name.eq_ignore_ascii_case("bin"))
        .then(|| executable_dir.parent())
        .flatten()
        .unwrap_or(executable_dir);
    root.join("runtime/rust-output")
}

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
    let specs: [(PathBuf, fn(&Path) -> bool); 5] = [
        (output_dir.clone(), is_generated_image),
        (output_dir.join("nai"), is_generated_image),
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
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
}

fn is_generated_image(path: &Path) -> bool {
    let name = name(path);
    name.starts_with("summary-") && name.ends_with(".png")
}

fn replayable_image_mime_type(path: &Path) -> Option<&'static str> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())?
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_generated_image_uses_any_nonempty_supported_image_in_nai_dir() {
        let directory = std::env::temp_dir().join(format!(
            "summary-agent-random-image-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&directory).unwrap();
        let nai = directory.join("nai");
        fs::create_dir_all(&nai).unwrap();
        fs::write(nai.join("summary-a.png"), b"png-a").unwrap();
        fs::write(nai.join("custom.jpg"), b"jpg").unwrap();
        fs::write(nai.join("empty.webp"), []).unwrap();
        fs::write(nai.join("notes.txt"), b"not-an-image").unwrap();
        fs::write(directory.join("root.png"), b"not-nai").unwrap();

        let artifact = random_generated_image_from_dir(&nai).unwrap().unwrap();
        assert!(artifact.path.ends_with("summary-a.png") || artifact.path.ends_with("custom.jpg"));
        assert!(artifact.path.contains("nai"));
        assert!(matches!(
            artifact.mime_type.as_str(),
            "image/png" | "image/jpeg"
        ));
        let _ = fs::remove_dir_all(directory);
    }
}
