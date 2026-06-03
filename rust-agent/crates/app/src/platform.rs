use std::{
    env,
    sync::{
        mpsc::{self, Receiver, RecvTimeoutError},
        Arc,
    },
    time::Duration as StdDuration,
};

use anyhow::{bail, Context as AnyhowContext, Result};
use chrono::{DateTime, Utc};
use serenity::{
    all::{
        Attachment, ChannelId, CreateAttachment, CreateMessage, GatewayIntents, GetMessages,
        Message, MessageId, UserId,
    },
    async_trait,
    client::{Client, Context as SerenityContext, EventHandler},
    http::Http,
};
use wechat_summary_core::{
    config::{ListenConfig, PlatformKindConfig},
    models::IncomingMessage,
    AgentConfig,
};
use wx4py_client::{Wx4pyClient, Wx4pyEvent, Wx4pyHistoryMessage, Wx4pySender};

const DISCORD_TEXT_LIMIT: usize = 1_900;

pub enum PlatformClient {
    Wx4py(Wx4pyPlatform),
    Discord(DiscordPlatform),
}

#[derive(Clone)]
pub enum PlatformSender {
    Wx4py(Wx4pySender),
    Discord(DiscordSender),
}

#[derive(Clone)]
pub struct DiscordSender {
    http: Arc<Http>,
}

pub struct Wx4pyPlatform {
    client: Wx4pyClient,
}

pub struct DiscordPlatform {
    http: Arc<Http>,
    receiver: Receiver<DiscordInbound>,
    bot_user_id: UserId,
    _gateway_task: tokio::task::JoinHandle<()>,
}

enum DiscordInbound {
    Event(PlatformEvent),
    Error(String),
}

impl PlatformClient {
    pub async fn start(config: &AgentConfig) -> Result<Self> {
        match config.platform.kind {
            PlatformKindConfig::Wx4py => Ok(Self::Wx4py(Wx4pyPlatform {
                client: Wx4pyClient::start(&config.wx4py, &config.listen, &config.wx_cli)?,
            })),
            PlatformKindConfig::Discord => Ok(Self::Discord(DiscordPlatform::start(config).await?)),
        }
    }

    pub fn kind(&self) -> PlatformKindConfig {
        match self {
            Self::Wx4py(_) => PlatformKindConfig::Wx4py,
            Self::Discord(_) => PlatformKindConfig::Discord,
        }
    }

    pub fn supports(&self, kind: PlatformKindConfig) -> bool {
        self.kind() == kind
    }

    pub fn configured_rooms(&self, config: &AgentConfig) -> Vec<String> {
        match self {
            Self::Wx4py(_) => wx4py_rooms(config),
            Self::Discord(_) => discord_rooms(config),
        }
    }

    pub fn next_event_timeout(&self, timeout: StdDuration) -> Result<Option<PlatformEvent>> {
        match self {
            Self::Wx4py(platform) => platform.next_event_timeout(timeout),
            Self::Discord(platform) => platform.next_event_timeout(timeout),
        }
    }

    pub async fn send_text(&self, room_id: &str, text: &str) -> Result<()> {
        self.sender().send_text(room_id, text).await
    }

    pub async fn send_image(&self, room_id: &str, image_path: &str) -> Result<()> {
        self.sender().send_image(room_id, image_path).await
    }

    pub fn sender(&self) -> PlatformSender {
        match self {
            Self::Wx4py(platform) => PlatformSender::Wx4py(platform.client.sender()),
            Self::Discord(platform) => PlatformSender::Discord(DiscordSender {
                http: Arc::clone(&platform.http),
            }),
        }
    }

    pub async fn query_text_messages(
        &self,
        room_id: &str,
        room_name: Option<&str>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u32,
        media_decode_limit: Option<usize>,
    ) -> Result<Vec<PlatformHistoryMessage>> {
        match self {
            Self::Wx4py(platform) => platform
                .client
                .query_text_messages(room_id, room_name, since, until, limit, media_decode_limit)
                .await
                .map(|messages| messages.into_iter().map(Into::into).collect())
                .map_err(Into::into),
            Self::Discord(platform) => {
                platform
                    .query_text_messages(room_id, since, until, limit)
                    .await
            }
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

impl DiscordPlatform {
    async fn start(config: &AgentConfig) -> Result<Self> {
        let token = discord_token(config)?;
        let (sender, receiver) = mpsc::channel();
        let handler = DiscordHandler {
            sender: sender.clone(),
        };
        let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;
        let mut client = Client::builder(&token, intents)
            .event_handler(handler)
            .await
            .context("building Discord gateway client")?;
        let http = client.http.clone();
        let current_user = http
            .get_current_user()
            .await
            .context("fetching Discord bot user")?;
        let bot_user_id = current_user.id;
        let gateway_task = tokio::spawn(async move {
            if let Err(error) = client.start().await {
                let _ = sender.send(DiscordInbound::Error(format!(
                    "Discord gateway stopped: {error}"
                )));
            }
        });

        Ok(Self {
            http,
            receiver,
            bot_user_id,
            _gateway_task: gateway_task,
        })
    }

    fn next_event_timeout(&self, timeout: StdDuration) -> Result<Option<PlatformEvent>> {
        match self.receiver.recv_timeout(timeout) {
            Ok(DiscordInbound::Event(event)) => Ok(Some(event)),
            Ok(DiscordInbound::Error(error)) => bail!(error),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => bail!("Discord gateway event channel closed"),
        }
    }

    async fn query_text_messages(
        &self,
        room_id: &str,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<PlatformHistoryMessage>> {
        let channel_id = parse_discord_channel_id(room_id)?;
        let mut before: Option<MessageId> = None;
        let mut collected = Vec::new();
        let max_messages = limit as usize;

        while collected.len() < max_messages {
            let remaining = max_messages.saturating_sub(collected.len());
            if remaining == 0 {
                break;
            }

            let batch_limit = remaining.min(100) as u8;
            let mut request = GetMessages::new().limit(batch_limit);
            if let Some(before_id) = before {
                request = request.before(before_id);
            }

            let messages = channel_id
                .messages(&self.http, request)
                .await
                .with_context(|| format!("fetching Discord messages from channel {room_id}"))?;
            if messages.is_empty() {
                break;
            }

            let mut reached_before_range = false;
            for message in &messages {
                let timestamp = message.timestamp.to_utc();
                if timestamp > until {
                    continue;
                }
                if timestamp < since {
                    reached_before_range = true;
                    continue;
                }

                let content = discord_message_content(message);
                if content.trim().is_empty() {
                    continue;
                }
                let image_url = discord_image_attachment_url(message);

                collected.push(PlatformHistoryMessage {
                    timestamp,
                    sender_id: message.author.id.to_string(),
                    sender_name: Some(message.author.name.clone()),
                    content,
                    msg_type: discord_message_msg_type(message).to_string(),
                    media_path: image_url,
                    decoded_media_path: None,
                    media_decode_error: None,
                    thumbnail_path: None,
                    is_self: message.author.id == self.bot_user_id,
                });
            }

            before = messages.last().map(|message| message.id);
            if reached_before_range || messages.len() < batch_limit as usize {
                break;
            }
        }

        collected.sort_by_key(|message| message.timestamp);
        Ok(collected)
    }
}

impl PlatformSender {
    pub async fn send_text(&self, room_id: &str, text: &str) -> Result<()> {
        match self {
            Self::Wx4py(sender) => sender.send_text(room_id, text).await.map_err(Into::into),
            Self::Discord(sender) => sender.send_text(room_id, text).await,
        }
    }

    pub async fn send_image(&self, room_id: &str, image_path: &str) -> Result<()> {
        match self {
            Self::Wx4py(sender) => sender
                .send_image(room_id, image_path)
                .await
                .map_err(Into::into),
            Self::Discord(sender) => sender.send_image(room_id, image_path).await,
        }
    }
}

impl DiscordSender {
    async fn send_text(&self, room_id: &str, text: &str) -> Result<()> {
        let channel_id = parse_discord_channel_id(room_id)?;
        for chunk in discord_text_chunks(text) {
            channel_id
                .send_message(&self.http, CreateMessage::new().content(chunk))
                .await
                .with_context(|| format!("sending Discord text to channel {room_id}"))?;
        }
        Ok(())
    }

    async fn send_image(&self, room_id: &str, image_path: &str) -> Result<()> {
        let channel_id = parse_discord_channel_id(room_id)?;
        let attachment = CreateAttachment::path(image_path)
            .await
            .with_context(|| format!("loading image attachment {image_path}"))?;
        channel_id
            .send_files(&self.http, [attachment], CreateMessage::new())
            .await
            .with_context(|| format!("sending Discord image to channel {room_id}"))?;
        Ok(())
    }
}

struct DiscordHandler {
    sender: mpsc::Sender<DiscordInbound>,
}

#[async_trait]
impl EventHandler for DiscordHandler {
    async fn message(&self, ctx: SerenityContext, message: Message) {
        if message.author.bot {
            return;
        }

        let content = discord_message_content(&message);
        if content.trim().is_empty() {
            return;
        }
        let msg_type = discord_message_msg_type(&message).to_string();

        let room_name = message.channel_id.name(&ctx.http).await.ok();
        let event = PlatformEvent {
            room_id: message.channel_id.to_string(),
            room_name,
            sender_id: message.author.id.to_string(),
            sender_name: Some(message.author.name),
            content,
            msg_type,
            timestamp: message.timestamp.to_utc(),
            is_self: false,
        };
        let _ = self.sender.send(DiscordInbound::Event(event));
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
    pub media_path: Option<String>,
    pub thumbnail_path: Option<String>,
    pub decoded_media_path: Option<String>,
    pub media_decode_error: Option<String>,
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
            media_path: message.media_path,
            thumbnail_path: message.thumbnail_path,
            decoded_media_path: message.decoded_media_path,
            media_decode_error: message.media_decode_error,
            is_self: message.is_self,
        }
    }
}

fn discord_token(config: &AgentConfig) -> Result<String> {
    if let Some(token) = config
        .discord
        .token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        return Ok(token.to_string());
    }

    env::var(&config.discord.token_env).with_context(|| {
        format!(
            "missing Discord bot token; set [discord].token or environment variable {}",
            config.discord.token_env
        )
    })
}

fn parse_discord_channel_id(room_id: &str) -> Result<ChannelId> {
    let id = room_id.trim().parse::<u64>().with_context(|| {
        format!("Discord channel id must be a numeric snowflake, got {room_id:?}")
    })?;
    Ok(ChannelId::new(id))
}

fn discord_message_content(message: &Message) -> String {
    let content = message.content.trim();
    let attachment_tags = message
        .attachments
        .iter()
        .map(discord_attachment_tag)
        .collect::<Vec<_>>();

    match (content.is_empty(), attachment_tags.is_empty()) {
        (true, true) => String::new(),
        (false, true) => content.to_string(),
        (true, false) => attachment_tags.join(" "),
        (false, false) => format!("{content}\n{}", attachment_tags.join(" ")),
    }
}

fn discord_message_msg_type(message: &Message) -> &'static str {
    if message.attachments.iter().any(is_discord_image_attachment) {
        "image"
    } else {
        "text"
    }
}

fn discord_image_attachment_url(message: &Message) -> Option<String> {
    message
        .attachments
        .iter()
        .find(|attachment| is_discord_image_attachment(attachment))
        .map(|attachment| attachment.url.clone())
}

fn discord_attachment_tag(attachment: &Attachment) -> String {
    if is_discord_image_attachment(attachment) {
        format!("[图片:{} {}]", attachment.filename, attachment.url)
    } else {
        format!("[附件:{}]", attachment.filename)
    }
}

fn is_discord_image_attachment(attachment: &Attachment) -> bool {
    is_discord_image_attachment_parts(
        &attachment.filename,
        attachment.content_type.as_deref(),
        attachment.width,
        attachment.height,
    )
}

fn is_discord_image_attachment_parts(
    filename: &str,
    content_type: Option<&str>,
    width: Option<u32>,
    height: Option<u32>,
) -> bool {
    content_type.is_some_and(|content_type| content_type.to_ascii_lowercase().starts_with("image/"))
        || width.is_some()
        || height.is_some()
        || has_image_extension(filename)
}

fn has_image_extension(filename: &str) -> bool {
    matches!(
        filename
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" | "avif"
    )
}

fn discord_text_chunks(text: &str) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_len = 0usize;
    for ch in text.chars() {
        if current_len >= DISCORD_TEXT_LIMIT {
            chunks.push(current);
            current = String::new();
            current_len = 0;
        }
        current.push(ch);
        current_len += 1;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn wx4py_rooms(config: &AgentConfig) -> Vec<String> {
    listen_groups(&config.wx4py.groups, &config.listen)
}

fn discord_rooms(config: &AgentConfig) -> Vec<String> {
    listen_groups(&config.discord.channels, &config.listen)
        .into_iter()
        .filter(|room| room.trim().parse::<u64>().is_ok())
        .collect()
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

    #[test]
    fn discord_text_chunks_keep_chunks_under_limit() {
        let text = "a".repeat(DISCORD_TEXT_LIMIT * 2 + 1);
        let chunks = discord_text_chunks(&text);

        assert_eq!(chunks.len(), 3);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.chars().count() <= DISCORD_TEXT_LIMIT));
    }

    #[test]
    fn discord_image_attachment_detection_accepts_mime_dimensions_and_extension() {
        assert!(is_discord_image_attachment_parts(
            "file.bin",
            Some("image/png"),
            None,
            None
        ));
        assert!(is_discord_image_attachment_parts(
            "file.bin",
            Some("application/octet-stream"),
            Some(640),
            None
        ));
        assert!(is_discord_image_attachment_parts(
            "shot.webp",
            Some("application/octet-stream"),
            None,
            None
        ));
        assert!(!is_discord_image_attachment_parts(
            "logs.zip",
            Some("application/zip"),
            None,
            None
        ));
    }

    #[test]
    fn discord_rooms_only_keeps_channel_ids() {
        let mut config = AgentConfig::from_toml_str(
            r#"
            [platform]
            kind = "discord"

            [listen]
            triggers = ["/总结"]
            whitelist_rooms = ["general", "123456789012345678"]

            [time_range]

            [storage]
            sqlite_path = ":memory:"

            [llm]
            provider = "openai_compatible"
            api_key_env = "LLM_API_KEY"

            [image_gen]
            enabled = false
            provider = "openai"
            api_key_env = "IMAGE_API_KEY"
            size = "2:3"

            [runtime]
            output_dir = ".\\runtime\\test"
            "#,
        )
        .unwrap();

        assert_eq!(
            discord_rooms(&config),
            vec!["123456789012345678".to_string()]
        );

        config.discord.channels = vec!["not-a-channel".into(), "234567890123456789".into()];
        assert_eq!(
            discord_rooms(&config),
            vec!["234567890123456789".to_string()]
        );
    }
}
