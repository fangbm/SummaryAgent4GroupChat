use std::time::Duration as StdDuration;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use wechat_summary_core::{
    config::{ListenConfig, PlatformKindConfig},
    models::IncomingMessage,
    AgentConfig,
};
use wx4py_client::{Wx4pyClient, Wx4pyEvent, Wx4pyHistoryMessage};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PlatformKind {
    Wx4py,
}

impl PlatformKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Wx4py => "wx4py",
        }
    }
}

pub enum PlatformClient {
    Wx4py(Wx4pyPlatform),
}

pub struct Wx4pyPlatform {
    client: Wx4pyClient,
}

impl PlatformClient {
    pub fn start(config: &AgentConfig) -> Result<Self> {
        match config.platform.kind {
            PlatformKindConfig::Wx4py => Ok(Self::Wx4py(Wx4pyPlatform {
                client: Wx4pyClient::start(&config.wx4py, &config.listen, &config.wx_cli)?,
            })),
            PlatformKindConfig::Discord => {
                bail!("platform 'discord' is not implemented yet; add a Discord PlatformClient adapter")
            }
        }
    }

    pub fn kind(&self) -> PlatformKind {
        match self {
            Self::Wx4py(_) => PlatformKind::Wx4py,
        }
    }

    pub fn configured_rooms(&self, config: &AgentConfig) -> Vec<String> {
        match self {
            Self::Wx4py(_) => wx4py_rooms(config),
        }
    }

    pub fn next_event_timeout(&self, timeout: StdDuration) -> Result<Option<PlatformEvent>> {
        match self {
            Self::Wx4py(platform) => platform.next_event_timeout(timeout),
        }
    }

    pub async fn send_text(&self, room_id: &str, text: &str) -> Result<()> {
        match self {
            Self::Wx4py(platform) => platform
                .client
                .send_text(room_id, text)
                .await
                .map_err(Into::into),
        }
    }

    pub async fn send_image(&self, room_id: &str, image_path: &str) -> Result<()> {
        match self {
            Self::Wx4py(platform) => platform
                .client
                .send_image(room_id, image_path)
                .await
                .map_err(Into::into),
        }
    }

    pub async fn query_text_messages(
        &self,
        room_id: &str,
        room_name: Option<&str>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<PlatformHistoryMessage>> {
        match self {
            Self::Wx4py(platform) => platform
                .client
                .query_text_messages(room_id, room_name, since, until, limit)
                .await
                .map(|messages| messages.into_iter().map(Into::into).collect())
                .map_err(Into::into),
        }
    }
}

impl Wx4pyPlatform {
    fn next_event_timeout(&self, timeout: StdDuration) -> Result<Option<PlatformEvent>> {
        Ok(self
            .client
            .next_event_timeout(timeout)?
            .and_then(PlatformEvent::from_wx4py))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PlatformEvent {
    pub room_id: String,
    pub room_name: Option<String>,
    pub sender_id: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub msg_type: String,
    pub timestamp: DateTime<Utc>,
    pub is_self: bool,
}

impl PlatformEvent {
    fn from_wx4py(event: Wx4pyEvent) -> Option<Self> {
        let timestamp = event.timestamp()?;
        Some(Self {
            room_id: event.room_id,
            room_name: event.room_name,
            sender_id: event.sender_id.unwrap_or_else(|| "unknown".to_string()),
            sender_name: event.sender_name,
            content: event.content,
            msg_type: "text".to_string(),
            timestamp,
            is_self: false,
        })
    }
}

impl From<PlatformEvent> for IncomingMessage {
    fn from(event: PlatformEvent) -> Self {
        Self {
            room_id: event.room_id,
            room_name: event.room_name,
            sender_id: event.sender_id,
            sender_name: event.sender_name,
            content: event.content,
            msg_type: event.msg_type,
            timestamp: event.timestamp,
            is_self: event.is_self,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PlatformHistoryMessage {
    pub timestamp: DateTime<Utc>,
    pub sender_id: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub msg_type: String,
    pub is_self: bool,
}

impl From<Wx4pyHistoryMessage> for PlatformHistoryMessage {
    fn from(message: Wx4pyHistoryMessage) -> Self {
        Self {
            timestamp: message.timestamp,
            sender_id: message.sender_id,
            sender_name: message.sender_name,
            content: message.content,
            msg_type: message.msg_type,
            is_self: message.is_self,
        }
    }
}

fn wx4py_rooms(config: &AgentConfig) -> Vec<String> {
    listen_groups(&config.wx4py.groups, &config.listen)
}

fn listen_groups(platform_rooms: &[String], listen: &ListenConfig) -> Vec<String> {
    let rooms = if platform_rooms.is_empty() {
        &listen.whitelist_rooms
    } else {
        platform_rooms
    };

    rooms
        .iter()
        .filter(|room| !room.trim().is_empty())
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_groups_falls_back_to_whitelist() {
        let listen = ListenConfig {
            triggers: vec!["/总结".into()],
            match_mode: Default::default(),
            whitelist_rooms: vec!["room-a".into()],
            blacklist_users: Vec::new(),
            content_types: vec!["text".into()],
            ignore_self: true,
        };

        assert_eq!(listen_groups(&[], &listen), vec!["room-a".to_string()]);
    }

    #[test]
    fn listen_groups_prefers_platform_rooms() {
        let listen = ListenConfig {
            triggers: vec!["/总结".into()],
            match_mode: Default::default(),
            whitelist_rooms: vec!["fallback-room".into()],
            blacklist_users: Vec::new(),
            content_types: vec!["text".into()],
            ignore_self: true,
        };

        assert_eq!(
            listen_groups(&["platform-room".to_string()], &listen),
            vec!["platform-room".to_string()]
        );
    }

    #[test]
    fn listen_groups_drops_blank_rooms() {
        let listen = ListenConfig {
            triggers: vec!["/总结".into()],
            match_mode: Default::default(),
            whitelist_rooms: vec![" ".into(), "fallback-room".into()],
            blacklist_users: Vec::new(),
            content_types: vec!["text".into()],
            ignore_self: true,
        };

        assert_eq!(
            listen_groups(&[], &listen),
            vec!["fallback-room".to_string()]
        );
    }
}
