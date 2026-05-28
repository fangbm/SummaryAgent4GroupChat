use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct IncomingMessage {
    pub room_id: String,
    pub room_name: Option<String>,
    pub sender_id: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub msg_type: String,
    pub timestamp: DateTime<Utc>,
    pub is_self: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct ChatMessage {
    pub timestamp: DateTime<Utc>,
    pub sender_id: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub msg_type: String,
}

impl ChatMessage {
    pub fn display_sender(&self) -> &str {
        self.sender_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(&self.sender_id)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct UserStat {
    pub user: String,
    pub count: usize,
    pub percentage: f64,
    pub frequency_per_hour: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub struct ImageArtifact {
    pub path: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub sha256: String,
}
