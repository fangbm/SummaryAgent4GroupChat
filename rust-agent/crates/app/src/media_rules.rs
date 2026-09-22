//! Media candidate selection shared by image, video, and voice enrichment.

use crate::platform::PlatformHistoryMessage;

pub(crate) fn is_auth_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("401")
        || lower.contains("unauthorized")
        || lower.contains("invalid_platform_key")
        || lower.contains("missing or invalid platform key")
}

pub(crate) fn is_image_type(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "image" | "img" | "3" | "图片"
    )
}
pub(crate) fn is_video_type(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "video" | "视频" | "43"
    )
}
pub(crate) fn is_voice_type(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "voice" | "语音" | "34"
    )
}

pub(crate) fn image_source(message: &PlatformHistoryMessage) -> Option<String> {
    decoded_source(message).or_else(|| remote_or_data_source(message))
}
pub(crate) fn video_source(message: &PlatformHistoryMessage) -> Option<String> {
    decoded_source(message).or_else(|| non_empty_media_path(message))
}
pub(crate) fn voice_source(message: &PlatformHistoryMessage) -> Option<String> {
    decoded_source(message).or_else(|| non_remote_media_path(message))
}

fn decoded_source(message: &PlatformHistoryMessage) -> Option<String> {
    message
        .decoded_media_path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .map(ToOwned::to_owned)
}
fn non_empty_media_path(message: &PlatformHistoryMessage) -> Option<String> {
    message
        .media_path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .map(ToOwned::to_owned)
}
fn remote_or_data_source(message: &PlatformHistoryMessage) -> Option<String> {
    message
        .media_path
        .as_deref()
        .filter(|path| {
            let value = path.trim();
            value.starts_with("http://")
                || value.starts_with("https://")
                || value.starts_with("data:")
        })
        .map(ToOwned::to_owned)
}
fn non_remote_media_path(message: &PlatformHistoryMessage) -> Option<String> {
    message
        .media_path
        .as_deref()
        .filter(|path| {
            let value = path.trim();
            !value.is_empty()
                && !value.starts_with("http://")
                && !value.starts_with("https://")
                && !value.starts_with("data:")
        })
        .map(ToOwned::to_owned)
}
