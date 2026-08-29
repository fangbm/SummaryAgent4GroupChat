//! Media enrichment boundary for summary history.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::task::JoinSet;
use tracing::{info, warn};
use wechat_summary_ai::{
    AiError, OpenAiAudioTranscriptionClient, OpenAiVideoCaptionClient, OpenAiVisionCaptionClient,
};
use wechat_summary_core::AgentConfig;

use crate::{
    ai_runtime::{ai_trace_context_for_item, ai_trace_dir},
    media_rules,
    platform::PlatformHistoryMessage,
    runtime_log::append_runtime_log,
};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MediaEnrichment {
    pub(crate) images: usize,
    pub(crate) videos: usize,
    pub(crate) voices: usize,
}

impl MediaEnrichment {
    pub(crate) fn total(self) -> usize {
        self.images + self.videos + self.voices
    }
}

pub(crate) async fn enrich_history(
    config: &AgentConfig,
    room_id: &str,
    history: &mut [PlatformHistoryMessage],
) -> Result<MediaEnrichment> {
    Ok(MediaEnrichment {
        images: apply_image_captions(config, room_id, history).await?,
        videos: apply_video_captions(config, room_id, history).await?,
        voices: apply_voice_transcriptions(config, room_id, history).await?,
    })
}

pub(crate) async fn apply_image_captions(
    config: &AgentConfig,
    room_id: &str,
    history: &mut [PlatformHistoryMessage],
) -> Result<usize> {
    if !config.image_caption.enabled || config.image_caption.max_images_per_summary == 0 {
        return Ok(0);
    }
    let captioner =
        match OpenAiVisionCaptionClient::new(config.image_caption.clone(), &config.proxy) {
            Ok(client) => match ai_trace_dir(config)? {
                Some(trace_dir) => client.with_trace_dir(trace_dir),
                None => client,
            },
            Err(error) => {
                let error = error.to_string();
                warn!(
                    room_id = %room_id,
                    error = %error,
                    "image caption client initialization failed; continuing without captions"
                );
                append_runtime_log(
                    config,
                    &format!("image caption init failed room={} error={}", room_id, error),
                );
                return Ok(0);
            }
        };
    let captioner = Arc::new(captioner);
    let max_concurrent = config.image_caption.max_concurrent_requests.max(1);

    let selection = select_candidates(
        history,
        config.image_caption.max_images_per_summary,
        media_rules::is_image_type,
        media_rules::image_source,
    );
    for error in &selection.decode_errors {
        append_runtime_log(
            config,
            &format!(
                "image caption skipped room={} reason=decode_failed error={}",
                room_id, error
            ),
        );
    }
    let candidates: Vec<_> = selection
        .candidates
        .into_iter()
        .map(|candidate| (candidate.history_index, candidate.attempted, candidate.source))
        .collect();

    let attempted = candidates.len();
    if attempted == 0 {
        return Ok(0);
    }

    append_runtime_log(
        config,
        &format!(
            "image caption batch started room={} attempted={} max_concurrent={}",
            room_id, attempted, max_concurrent
        ),
    );

    let mut inserted = 0usize;
    let mut next_candidate = 0usize;
    let mut join_set = JoinSet::new();
    while next_candidate < candidates.len() && join_set.len() < max_concurrent {
        spawn_image_caption_task(
            &mut join_set,
            Arc::clone(&captioner),
            &candidates[next_candidate],
            attempted,
            room_id,
        );
        next_candidate += 1;
    }

    while let Some(joined) = join_set.join_next().await {
        let ImageCaptionTaskResult {
            history_index,
            attempted,
            result,
        } = joined.context("joining image caption request task")?;
        match result {
            Ok(caption) => {
                let caption = caption.trim();
                if !caption.is_empty() {
                    history[history_index].content = format!(
                        "{}（图片转述：{}）",
                        history[history_index].content.trim(),
                        caption
                    );
                    inserted += 1;
                    info!(
                        room_id = %room_id,
                        inserted,
                        attempted,
                        "image caption inserted into history"
                    );
                }
            }
            Err(error) => {
                let error = error.to_string();
                warn!(
                    room_id = %room_id,
                    attempted,
                    error = %error,
                    "image caption failed; keeping image placeholder"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "image caption failed room={} attempted={} error={}",
                        room_id, attempted, error
                    ),
                );
                if media_rules::is_auth_error(&error) {
                    warn!(
                        room_id = %room_id,
                        attempted,
                        "image caption stopped after authentication failure"
                    );
                    append_runtime_log(
                        config,
                        &format!(
                            "image caption stopped room={} reason=authentication_failed attempted={}",
                            room_id, attempted
                        ),
                    );
                    break;
                }
            }
        }

        while next_candidate < candidates.len() && join_set.len() < max_concurrent {
            spawn_image_caption_task(
                &mut join_set,
                Arc::clone(&captioner),
                &candidates[next_candidate],
                attempted,
                room_id,
            );
            next_candidate += 1;
        }
    }

    append_runtime_log(
        config,
        &format!(
            "image caption completed room={} attempted={} inserted={}",
            room_id, attempted, inserted
        ),
    );
    Ok(inserted)
}

struct ImageCaptionTaskResult {
    history_index: usize,
    attempted: usize,
    result: std::result::Result<String, AiError>,
}

fn spawn_image_caption_task(
    join_set: &mut JoinSet<ImageCaptionTaskResult>,
    captioner: Arc<OpenAiVisionCaptionClient>,
    candidate: &(usize, usize, String),
    item_total: usize,
    room_id: &str,
) {
    let (history_index, attempted, source) = (candidate.0, candidate.1, candidate.2.clone());
    let room_id = room_id.to_string();
    join_set.spawn(async move {
        let captioner = captioner
            .as_ref()
            .clone()
            .with_trace_context(ai_trace_context_for_item(
                &room_id,
                "image caption",
                attempted,
                item_total,
            ));
        ImageCaptionTaskResult {
            history_index,
            attempted,
            result: captioner.caption_image(&source).await,
        }
    });
}

pub(crate) async fn apply_video_captions(
    config: &AgentConfig,
    room_id: &str,
    history: &mut [PlatformHistoryMessage],
) -> Result<usize> {
    if !config.video_caption.enabled || config.video_caption.max_videos_per_summary == 0 {
        return Ok(0);
    }
    let captioner = match OpenAiVideoCaptionClient::new(config.video_caption.clone(), &config.proxy)
    {
        Ok(client) => match ai_trace_dir(config)? {
            Some(trace_dir) => client.with_trace_dir(trace_dir),
            None => client,
        },
        Err(error) => {
            let error = error.to_string();
            warn!(
                room_id = %room_id,
                error = %error,
                "video caption client initialization failed; continuing without captions"
            );
            append_runtime_log(
                config,
                &format!("video caption init failed room={} error={}", room_id, error),
            );
            return Ok(0);
        }
    };
    let captioner = Arc::new(captioner);
    let max_concurrent = config.video_caption.max_concurrent_requests.max(1);

    let selection = select_candidates(
        history,
        config.video_caption.max_videos_per_summary,
        media_rules::is_video_type,
        media_rules::video_source,
    );
    for error in &selection.decode_errors {
        append_runtime_log(
            config,
            &format!(
                "video caption skipped room={} reason=decode_failed error={}",
                room_id, error
            ),
        );
    }
    let candidates: Vec<_> = selection
        .candidates
        .into_iter()
        .map(|candidate| (candidate.history_index, candidate.attempted, candidate.source))
        .collect();

    let attempted = candidates.len();
    if attempted == 0 {
        return Ok(0);
    }

    append_runtime_log(
        config,
        &format!(
            "video caption batch started room={} attempted={} max_concurrent={}",
            room_id, attempted, max_concurrent
        ),
    );

    let mut inserted = 0usize;
    let mut next_candidate = 0usize;
    let mut join_set = JoinSet::new();
    while next_candidate < candidates.len() && join_set.len() < max_concurrent {
        spawn_video_caption_task(
            &mut join_set,
            Arc::clone(&captioner),
            &candidates[next_candidate],
            attempted,
            room_id,
        );
        next_candidate += 1;
    }

    while let Some(joined) = join_set.join_next().await {
        let VideoCaptionTaskResult {
            history_index,
            attempted,
            result,
        } = joined.context("joining video caption request task")?;
        match result {
            Ok(caption) => {
                let caption = caption.trim();
                if !caption.is_empty() {
                    history[history_index].content = format!(
                        "{}（视频转述：{}）",
                        history[history_index].content.trim(),
                        caption
                    );
                    inserted += 1;
                    info!(
                        room_id = %room_id,
                        inserted,
                        attempted,
                        "video caption inserted into history"
                    );
                }
            }
            Err(error) => {
                let error = error.to_string();
                warn!(
                    room_id = %room_id,
                    attempted,
                    error = %error,
                    "video caption failed; keeping video placeholder"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "video caption failed room={} attempted={} error={}",
                        room_id, attempted, error
                    ),
                );
                if media_rules::is_auth_error(&error) {
                    warn!(
                        room_id = %room_id,
                        attempted,
                        "video caption stopped after authentication failure"
                    );
                    append_runtime_log(
                        config,
                        &format!(
                            "video caption stopped room={} reason=authentication_failed attempted={}",
                            room_id, attempted
                        ),
                    );
                    break;
                }
            }
        }

        while next_candidate < candidates.len() && join_set.len() < max_concurrent {
            spawn_video_caption_task(
                &mut join_set,
                Arc::clone(&captioner),
                &candidates[next_candidate],
                attempted,
                room_id,
            );
            next_candidate += 1;
        }
    }

    append_runtime_log(
        config,
        &format!(
            "video caption completed room={} attempted={} inserted={}",
            room_id, attempted, inserted
        ),
    );
    Ok(inserted)
}

struct VideoCaptionTaskResult {
    history_index: usize,
    attempted: usize,
    result: std::result::Result<String, AiError>,
}

fn spawn_video_caption_task(
    join_set: &mut JoinSet<VideoCaptionTaskResult>,
    captioner: Arc<OpenAiVideoCaptionClient>,
    candidate: &(usize, usize, String),
    item_total: usize,
    room_id: &str,
) {
    let (history_index, attempted, source) = (candidate.0, candidate.1, candidate.2.clone());
    let room_id = room_id.to_string();
    join_set.spawn(async move {
        let captioner = captioner
            .as_ref()
            .clone()
            .with_trace_context(ai_trace_context_for_item(
                &room_id,
                "video caption",
                attempted,
                item_total,
            ));
        VideoCaptionTaskResult {
            history_index,
            attempted,
            result: captioner.caption_video(&source).await,
        }
    });
}

pub(crate) async fn apply_voice_transcriptions(
    config: &AgentConfig,
    room_id: &str,
    history: &mut [PlatformHistoryMessage],
) -> Result<usize> {
    if !config.voice_transcription.enabled || config.voice_transcription.max_voices_per_summary == 0
    {
        return Ok(0);
    }
    let transcriber = match OpenAiAudioTranscriptionClient::new(
        config.voice_transcription.clone(),
        &config.proxy,
    ) {
        Ok(client) => match ai_trace_dir(config)? {
            Some(trace_dir) => client.with_trace_dir(trace_dir),
            None => client,
        },
        Err(error) => {
            let error = error.to_string();
            warn!(
                room_id = %room_id,
                error = %error,
                "voice transcription client initialization failed; continuing without transcriptions"
            );
            append_runtime_log(
                config,
                &format!(
                    "voice transcription init failed room={} error={}",
                    room_id, error
                ),
            );
            return Ok(0);
        }
    };
    let transcriber = Arc::new(transcriber);
    let max_concurrent = config.voice_transcription.max_concurrent_requests.max(1);
    let audio_prep = Arc::new(crate::media_audio::VoiceTranscriptionAudioPrep::from_config(config));

    let selection = select_candidates(
        history,
        config.voice_transcription.max_voices_per_summary,
        media_rules::is_voice_type,
        media_rules::voice_source,
    );
    for error in &selection.decode_errors {
        append_runtime_log(
            config,
            &format!(
                "voice transcription skipped room={} reason=decode_failed error={}",
                room_id, error
            ),
        );
    }
    let candidates: Vec<_> = selection
        .candidates
        .into_iter()
        .map(|candidate| (candidate.history_index, candidate.attempted, candidate.source))
        .collect();

    let attempted = candidates.len();
    if attempted == 0 {
        return Ok(0);
    }

    append_runtime_log(
        config,
        &format!(
            "voice transcription batch started room={} attempted={} max_concurrent={} transcode_to_mp3={}",
            room_id, attempted, max_concurrent, audio_prep.transcode_to_mp3
        ),
    );

    let mut inserted = 0usize;
    let mut next_candidate = 0usize;
    let mut join_set = JoinSet::new();
    while next_candidate < candidates.len() && join_set.len() < max_concurrent {
        spawn_voice_transcription_task(
            &mut join_set,
            Arc::clone(&transcriber),
            Arc::clone(&audio_prep),
            &candidates[next_candidate],
            attempted,
            room_id,
        );
        next_candidate += 1;
    }

    while let Some(joined) = join_set.join_next().await {
        let VoiceTranscriptionTaskResult {
            history_index,
            attempted,
            result,
        } = joined.context("joining voice transcription request task")?;
        match result {
            Ok(transcription) => {
                let transcription = transcription.trim();
                if !transcription.is_empty() {
                    history[history_index].content = format!(
                        "{}（语音转写：{}）",
                        history[history_index].content.trim(),
                        transcription
                    );
                    inserted += 1;
                    info!(
                        room_id = %room_id,
                        inserted,
                        attempted,
                        "voice transcription inserted into history"
                    );
                }
            }
            Err(error) => {
                let error = error.to_string();
                warn!(
                    room_id = %room_id,
                    attempted,
                    error = %error,
                    "voice transcription failed; keeping voice placeholder"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "voice transcription failed room={} attempted={} error={}",
                        room_id, attempted, error
                    ),
                );
                if media_rules::is_auth_error(&error) {
                    warn!(
                        room_id = %room_id,
                        attempted,
                        "voice transcription stopped after authentication failure"
                    );
                    append_runtime_log(
                        config,
                        &format!(
                            "voice transcription stopped room={} reason=authentication_failed attempted={}",
                            room_id, attempted
                        ),
                    );
                    break;
                }
            }
        }

        while next_candidate < candidates.len() && join_set.len() < max_concurrent {
            spawn_voice_transcription_task(
                &mut join_set,
                Arc::clone(&transcriber),
                Arc::clone(&audio_prep),
                &candidates[next_candidate],
                attempted,
                room_id,
            );
            next_candidate += 1;
        }
    }

    append_runtime_log(
        config,
        &format!(
            "voice transcription completed room={} attempted={} inserted={}",
            room_id, attempted, inserted
        ),
    );
    Ok(inserted)
}

struct VoiceTranscriptionTaskResult {
    history_index: usize,
    attempted: usize,
    result: std::result::Result<String, AiError>,
}

fn spawn_voice_transcription_task(
    join_set: &mut JoinSet<VoiceTranscriptionTaskResult>,
    transcriber: Arc<OpenAiAudioTranscriptionClient>,
    audio_prep: Arc<crate::media_audio::VoiceTranscriptionAudioPrep>,
    candidate: &(usize, usize, String),
    item_total: usize,
    room_id: &str,
) {
    let (history_index, attempted, source) = (candidate.0, candidate.1, candidate.2.clone());
    let room_id = room_id.to_string();
    join_set.spawn(async move {
        let transcriber = transcriber
            .as_ref()
            .clone()
            .with_trace_context(ai_trace_context_for_item(
                &room_id,
                "voice transcription",
                attempted,
                item_total,
            ));
        let result = match crate::media_audio::prepare_voice_transcription_audio(audio_prep, source).await {
            Ok(source) => transcriber.transcribe_audio(&source).await,
            Err(error) => Err(error),
        };
        VoiceTranscriptionTaskResult {
            history_index,
            attempted,
            result,
        }
    });
}

#[derive(Debug, Clone)]
pub(crate) struct MediaCandidate {
    pub(crate) history_index: usize,
    pub(crate) attempted: usize,
    pub(crate) source: String,
}

#[derive(Debug, Default)]
pub(crate) struct CandidateSelection {
    pub(crate) candidates: Vec<MediaCandidate>,
    pub(crate) decode_errors: Vec<String>,
}

pub(crate) fn select_candidates(
    history: &[PlatformHistoryMessage],
    max_items: usize,
    matches_type: fn(&str) -> bool,
    source_for: fn(&PlatformHistoryMessage) -> Option<String>,
) -> CandidateSelection {
    let mut selection = CandidateSelection::default();
    for (history_index, message) in history.iter().enumerate() {
        if selection.candidates.len() >= max_items {
            break;
        }
        if !matches_type(&message.msg_type) {
            continue;
        }
        let Some(source) = source_for(message) else {
            if let Some(error) = message.media_decode_error.as_deref() {
                selection.decode_errors.push(error.to_string());
            }
            continue;
        };
        selection.candidates.push(MediaCandidate {
            history_index,
            attempted: selection.candidates.len() + 1,
            source,
        });
    }
    selection
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::media_rules;

    fn history_message(
        msg_type: &str,
        media_path: Option<&str>,
        decode_error: Option<&str>,
    ) -> PlatformHistoryMessage {
        PlatformHistoryMessage {
            stable_id: None,
            timestamp: Utc::now(),
            sender_id: "sender".to_string(),
            sender_name: None,
            content: "[media]".to_string(),
            msg_type: msg_type.to_string(),
            media_path: media_path.map(ToOwned::to_owned),
            thumbnail_path: None,
            decoded_media_path: None,
            media_decode_error: decode_error.map(ToOwned::to_owned),
            is_self: false,
        }
    }

    #[test]
    fn candidate_selection_preserves_order_and_reports_decode_errors() {
        let history = vec![
            history_message("text", None, None),
            history_message("image", None, Some("missing decoded image")),
            history_message("image", Some("https://example.test/a.png"), None),
            history_message("image", Some("https://example.test/b.png"), None),
        ];

        let selection = select_candidates(
            &history,
            1,
            media_rules::is_image_type,
            media_rules::image_source,
        );

        assert_eq!(selection.decode_errors, vec!["missing decoded image"]);
        assert_eq!(selection.candidates.len(), 1);
        assert_eq!(selection.candidates[0].history_index, 2);
        assert_eq!(selection.candidates[0].attempted, 1);
        assert_eq!(selection.candidates[0].source, "https://example.test/a.png");
    }
}
