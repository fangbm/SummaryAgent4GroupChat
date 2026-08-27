use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

use anyhow::{bail, Context, Result};
use wechat_summary_ai::AiError;
use wechat_summary_core::AgentConfig;

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x08000000;

#[derive(Debug, Clone)]
pub(crate) struct VoiceTranscriptionAudioPrep {
    pub(crate) transcode_to_mp3: bool,
    pub(crate) ffmpeg_executable: String,
    pub(crate) mp3_bitrate: String,
    pub(crate) cache_dir: PathBuf,
}

impl VoiceTranscriptionAudioPrep {
    pub(crate) fn from_config(config: &AgentConfig) -> Self {
        let ffmpeg_executable = config.voice_transcription.ffmpeg_executable.trim();
        let mp3_bitrate = config.voice_transcription.mp3_bitrate.trim();
        Self {
            transcode_to_mp3: config.voice_transcription.transcode_to_mp3,
            ffmpeg_executable: if ffmpeg_executable.is_empty() {
                "ffmpeg".to_string()
            } else {
                ffmpeg_executable.to_string()
            },
            mp3_bitrate: if mp3_bitrate.is_empty() {
                "64k".to_string()
            } else {
                mp3_bitrate.to_string()
            },
            cache_dir: Path::new(&config.runtime.output_dir).join("voice-mp3"),
        }
    }
}

pub(crate) async fn prepare_voice_transcription_audio(
    audio_prep: Arc<VoiceTranscriptionAudioPrep>,
    source: String,
) -> std::result::Result<String, AiError> {
    if !audio_prep.transcode_to_mp3 {
        return Ok(source);
    }

    let source_path = PathBuf::from(source);
    tokio::task::spawn_blocking(move || transcode_voice_source_to_mp3(&audio_prep, &source_path))
        .await
        .map_err(|error| {
            AiError::InvalidResponse(format!("voice mp3 transcoding task failed: {error}"))
        })?
        .map_err(|error| {
            AiError::InvalidResponse(format!("voice mp3 transcoding failed: {error:#}"))
        })
}

pub(crate) fn transcode_voice_source_to_mp3(
    audio_prep: &VoiceTranscriptionAudioPrep,
    source_path: &Path,
) -> Result<String> {
    if !source_path.is_file() {
        bail!("voice source file not found: {}", source_path.display());
    }

    let output_path = audio_prep
        .cache_dir
        .join(format!("{}.mp3", voice_transcode_cache_key(source_path)));
    if usable_cached_file(&output_path) {
        return Ok(output_path.to_string_lossy().into_owned());
    }

    fs::create_dir_all(&audio_prep.cache_dir).with_context(|| {
        format!(
            "creating voice mp3 cache {}",
            audio_prep.cache_dir.display()
        )
    })?;

    if is_mp3_audio_file(source_path) {
        fs::copy(source_path, &output_path).with_context(|| {
            format!(
                "copying already-mp3 voice {} to {}",
                source_path.display(),
                output_path.display()
            )
        })?;
        return Ok(output_path.to_string_lossy().into_owned());
    }

    let temp_path = output_path.with_extension(format!(
        "tmp-{}.mp3",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let mut command = Command::new(&audio_prep.ffmpeg_executable);
    command.arg("-hide_banner").arg("-nostdin").arg("-y");
    if let Some(format) = audio_input_format_hint(source_path) {
        command.arg("-f").arg(format);
    }
    command
        .arg("-i")
        .arg(source_path)
        .arg("-vn")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("16000")
        .arg("-codec:a")
        .arg("libmp3lame")
        .arg("-b:a")
        .arg(&audio_prep.mp3_bitrate)
        .arg(&temp_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);

    let output = command.output().with_context(|| {
        format!(
            "starting ffmpeg executable '{}'; set voice_transcription.ffmpeg_executable to the full path if needed",
            audio_prep.ffmpeg_executable
        )
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let detail = first_non_empty_line(&stderr)
            .or_else(|| first_non_empty_line(&stdout))
            .unwrap_or_else(|| "ffmpeg exited without output".to_string());
        let _ = fs::remove_file(&temp_path);
        bail!(
            "ffmpeg exited with {}; source={}; detail={}",
            output.status,
            source_path.display(),
            detail
        );
    }
    if !usable_cached_file(&temp_path) {
        bail!(
            "ffmpeg succeeded but did not create {}",
            temp_path.display()
        );
    }
    if output_path.exists() {
        let _ = fs::remove_file(&temp_path);
    } else {
        fs::rename(&temp_path, &output_path).with_context(|| {
            format!(
                "moving transcoded voice {} to {}",
                temp_path.display(),
                output_path.display()
            )
        })?;
    }
    Ok(output_path.to_string_lossy().into_owned())
}

fn voice_transcode_cache_key(source_path: &Path) -> String {
    let metadata = source_path.metadata().ok();
    let len = metadata
        .as_ref()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let modified = metadata
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let input = format!("{}:{len}:{modified}:mp3", source_path.display());
    format!("{:x}", md5::compute(input.as_bytes()))
}

fn usable_cached_file(path: &Path) -> bool {
    path.metadata()
        .map(|metadata| metadata.is_file() && metadata.len() > 0)
        .unwrap_or(false)
}

fn is_mp3_audio_file(path: &Path) -> bool {
    if path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case("mp3"))
        .unwrap_or(false)
    {
        return true;
    }

    let mut header = [0u8; 16];
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let Ok(read) = file.read(&mut header) else {
        return false;
    };
    let bytes = &header[..read];
    bytes.starts_with(b"ID3")
        || bytes.len() >= 2 && bytes[0] == 0xff && matches!(bytes[1], 0xfb | 0xf3 | 0xf2)
}

pub(crate) fn audio_input_format_hint(path: &Path) -> Option<&'static str> {
    let mut header = [0u8; 16];
    let Ok(mut file) = fs::File::open(path) else {
        return None;
    };
    let Ok(read) = file.read(&mut header) else {
        return None;
    };
    let bytes = &header[..read];
    if bytes.starts_with(b"#!SILK") || bytes.starts_with(b"\x02#!SILK") {
        Some("silk")
    } else if bytes.starts_with(b"#!AMR") {
        Some("amr")
    } else {
        None
    }
}

fn first_non_empty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.chars().take(300).collect())
}
