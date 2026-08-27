//! Media enrichment boundary for summary history.

use anyhow::Result;
use wechat_summary_core::AgentConfig;

use crate::{apply_image_captions, apply_video_captions, apply_voice_transcriptions, platform::PlatformHistoryMessage};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MediaEnrichment {
    pub(crate) images: usize,
    pub(crate) videos: usize,
    pub(crate) voices: usize,
}

impl MediaEnrichment {
    pub(crate) fn total(self) -> usize { self.images + self.videos + self.voices }
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
