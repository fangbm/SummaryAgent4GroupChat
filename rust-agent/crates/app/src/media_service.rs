//! Media enrichment boundary for summary history.

use anyhow::Result;
use wechat_summary_core::AgentConfig;

use crate::{
    apply_image_captions, apply_video_captions, apply_voice_transcriptions,
    platform::PlatformHistoryMessage,
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
