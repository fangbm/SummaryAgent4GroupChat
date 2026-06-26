use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
};

use serde::{de, Deserialize, Deserializer};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config TOML: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    #[serde(default)]
    pub platform: PlatformConfig,
    #[serde(default)]
    pub wx4py: Wx4pyConfig,
    #[serde(default)]
    pub discord: DiscordConfig,
    pub listen: ListenConfig,
    pub time_range: TimeRangeConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub manual_summary: ManualSummaryConfig,
    #[serde(default)]
    pub scheduled_summary: ScheduledSummaryConfig,
    #[serde(default)]
    pub history: HistoryConfig,
    pub storage: StorageConfig,
    #[serde(default, alias = "wxdb")]
    pub wx_cli: WxCliConfig,
    #[serde(default)]
    pub privacy: PrivacyConfig,
    pub llm: LlmConfig,
    #[serde(default)]
    pub text_summary: TextSummaryConfig,
    #[serde(default)]
    pub image_summary: ImageSummaryConfig,
    pub image_gen: ImageGenConfig,
    #[serde(default)]
    pub image_prompt: ImagePromptConfig,
    #[serde(default)]
    pub image_caption: ImageCaptionConfig,
    #[serde(default)]
    pub video_caption: VideoCaptionConfig,
    #[serde(default)]
    pub voice_transcription: VoiceTranscriptionConfig,
    #[serde(default)]
    pub proxy: ProxyConfig,
    pub runtime: RuntimeConfig,
}

impl AgentConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    pub fn from_toml_str(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }

    pub fn history_message_limit(&self) -> usize {
        self.history
            .max_messages
            .or(self.privacy.max_messages_to_llm)
            .or_else(|| self.wx_cli.max_messages.map(|value| value as usize))
            .unwrap_or_else(default_history_max_messages)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PlatformConfig {
    #[serde(default)]
    pub kind: PlatformKindConfig,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum PlatformKindConfig {
    #[default]
    Wx4py,
    Discord,
}

impl PlatformKindConfig {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Wx4py => "wx",
            Self::Discord => "discord",
        }
    }

    pub fn parse_alias(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "wx" | "微信" | "wechat" | "weixin" | "wx4py" => Some(Self::Wx4py),
            "dc" | "discord" => Some(Self::Discord),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for PlatformKindConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse_alias(&value).ok_or_else(|| {
            de::Error::custom(format!(
                "unsupported platform kind {value:?}; expected one of wx, 微信, wechat, dc, discord"
            ))
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Wx4pyConfig {
    #[serde(default = "default_python_executable")]
    pub python_executable: String,
    #[serde(default = "default_wx4py_sidecar_script")]
    pub sidecar_script: String,
    #[serde(default = "default_wx4py_ready_timeout")]
    pub ready_timeout_seconds: u64,
    #[serde(default)]
    pub groups: Vec<String>,
}

impl Default for Wx4pyConfig {
    fn default() -> Self {
        Self {
            python_executable: default_python_executable(),
            sidecar_script: default_wx4py_sidecar_script(),
            ready_timeout_seconds: default_wx4py_ready_timeout(),
            groups: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiscordConfig {
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default = "default_discord_token_env")]
    pub token_env: String,
    #[serde(default)]
    pub channels: Vec<String>,
}

impl Default for DiscordConfig {
    fn default() -> Self {
        Self {
            token: None,
            token_env: default_discord_token_env(),
            channels: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenConfig {
    pub triggers: Vec<String>,
    #[serde(default)]
    pub match_mode: MatchMode,
    #[serde(default)]
    pub whitelist_rooms: Vec<String>,
    #[serde(default)]
    pub blacklist_users: Vec<String>,
    #[serde(default = "default_text_content_types")]
    pub content_types: Vec<String>,
    #[serde(default = "default_true")]
    pub ignore_self: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    #[default]
    Prefix,
    Contains,
    Regex,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimeRangeConfig {
    #[serde(default)]
    pub mode: TimeRangeMode,
    #[serde(default = "default_fallback_minutes")]
    pub fallback_minutes: i64,
    #[serde(default = "default_fallback_minutes")]
    pub fixed_minutes: i64,
    #[serde(default = "default_fixed_hours")]
    pub fixed_hours: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_successful_request_cooldown_seconds")]
    pub successful_request_cooldown_seconds: i64,
    #[serde(default)]
    pub successful_image_cooldown_seconds: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ManualSummaryConfig {
    #[serde(default)]
    pub image_by_default: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScheduledSummaryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_scheduled_local_hour")]
    pub local_hour: u32,
    #[serde(default)]
    pub local_minute: u32,
    #[serde(default = "default_scheduled_range_hours")]
    pub range_hours: i64,
    #[serde(default)]
    pub rooms: Vec<String>,
    #[serde(default = "default_true")]
    pub send_text: bool,
    #[serde(default = "default_true")]
    pub send_image: bool,
    #[serde(default = "default_true")]
    pub ignore_rate_limit: bool,
}

impl Default for ScheduledSummaryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            local_hour: default_scheduled_local_hour(),
            local_minute: 0,
            range_hours: default_scheduled_range_hours(),
            rooms: Vec::new(),
            send_text: true,
            send_image: true,
            ignore_rate_limit: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HistoryConfig {
    #[serde(default)]
    pub max_messages: Option<usize>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            successful_request_cooldown_seconds: default_successful_request_cooldown_seconds(),
            successful_image_cooldown_seconds: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TimeRangeMode {
    #[default]
    BetweenTriggers,
    FixedMinutes,
    FixedHours,
    Today,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub sqlite_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WxCliConfig {
    #[serde(default = "default_wx_cli_executable")]
    pub executable: String,
    #[serde(default = "default_wx_cli_export_format")]
    pub export_format: String,
    #[serde(default)]
    pub max_messages: Option<u32>,
    #[serde(default = "default_wx_cli_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_wx_cli_history_query_timeout_seconds")]
    pub history_query_timeout_seconds: u64,
    #[serde(default = "default_wx_cli_temp_dir")]
    pub temp_dir: String,
    #[serde(default)]
    pub cache_dir: String,
    #[serde(default)]
    pub group_name_map: HashMap<String, String>,
}

impl Default for WxCliConfig {
    fn default() -> Self {
        Self {
            executable: default_wx_cli_executable(),
            export_format: default_wx_cli_export_format(),
            max_messages: None,
            timeout_seconds: default_wx_cli_timeout_seconds(),
            history_query_timeout_seconds: default_wx_cli_history_query_timeout_seconds(),
            temp_dir: default_wx_cli_temp_dir(),
            cache_dir: String::new(),
            group_name_map: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PrivacyConfig {
    #[serde(default)]
    pub redact_enabled: bool,
    #[serde(default)]
    pub max_messages_to_llm: Option<usize>,
    #[serde(default = "default_max_chars")]
    pub max_chars_to_llm: usize,
    #[serde(default = "default_true")]
    pub cloud_allowed: bool,
    #[serde(default)]
    pub sensitive_rooms: Vec<String>,
}

impl Default for PrivacyConfig {
    fn default() -> Self {
        Self {
            redact_enabled: false,
            max_messages_to_llm: None,
            max_chars_to_llm: default_max_chars(),
            cloud_allowed: true,
            sensitive_rooms: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig {
    pub provider: String,
    #[serde(default)]
    pub api_key: Option<String>,
    pub api_key_env: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_llm_base_url_env")]
    pub base_url_env: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_llm_model_env")]
    pub model_env: String,
    #[serde(default = "default_llm_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "default_retry_5xx_attempts")]
    pub retry_5xx_attempts: usize,
    #[serde(default = "default_max_output_tokens")]
    pub max_output_tokens: u32,
    #[serde(default = "default_llm_max_concurrent_chunk_requests")]
    pub max_concurrent_chunk_requests: usize,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    #[serde(default)]
    pub system_prompt: String,
    #[serde(default)]
    pub request_body_overrides: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TextSummaryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_text_summary_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_text_summary_user_prompt_template")]
    pub user_prompt_template: String,
}

impl Default for TextSummaryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            system_prompt: default_text_summary_system_prompt(),
            user_prompt_template: default_text_summary_user_prompt_template(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImageSummaryConfig {
    #[serde(default = "default_image_summary_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_image_summary_user_prompt_template")]
    pub user_prompt_template: String,
}

impl Default for ImageSummaryConfig {
    fn default() -> Self {
        Self {
            system_prompt: default_image_summary_system_prompt(),
            user_prompt_template: default_image_summary_user_prompt_template(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImageGenConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub provider: String,
    #[serde(default)]
    pub api_key: Option<String>,
    pub api_key_env: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_image_base_url_env")]
    pub base_url_env: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_image_model_env")]
    pub model_env: String,
    pub size: String,
    #[serde(default)]
    pub quality: Option<String>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub official_fallback: bool,
    #[serde(default = "default_image_poll_initial_delay")]
    pub poll_initial_delay_seconds: u64,
    #[serde(default = "default_image_poll_interval")]
    pub poll_interval_seconds: u64,
    #[serde(default = "default_image_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "default_retry_5xx_attempts")]
    pub retry_5xx_attempts: usize,
    #[serde(default)]
    pub prompt_template: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImagePromptConfig {
    #[serde(default = "default_image_prompt_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_image_prompt_user_prompt_template")]
    pub user_prompt_template: String,
}

impl Default for ImagePromptConfig {
    fn default() -> Self {
        Self {
            system_prompt: default_image_prompt_system_prompt(),
            user_prompt_template: default_image_prompt_user_prompt_template(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImageCaptionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_image_caption_provider")]
    pub provider: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_image_caption_api_key_env")]
    pub api_key_env: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_image_caption_base_url_env")]
    pub base_url_env: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_image_caption_model_env")]
    pub model_env: String,
    #[serde(default = "default_llm_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "default_retry_5xx_attempts")]
    pub retry_5xx_attempts: usize,
    #[serde(default = "default_image_caption_max_output_tokens")]
    pub max_output_tokens: u32,
    #[serde(default = "default_image_caption_temperature")]
    pub temperature: f32,
    #[serde(default = "default_image_caption_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_image_caption_user_prompt")]
    pub user_prompt: String,
    #[serde(default = "default_image_caption_max_images")]
    pub max_images_per_summary: usize,
    #[serde(default = "default_image_caption_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(default)]
    pub request_body_overrides: BTreeMap<String, toml::Value>,
}

impl Default for ImageCaptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: default_image_caption_provider(),
            api_key: None,
            api_key_env: default_image_caption_api_key_env(),
            base_url: None,
            base_url_env: default_image_caption_base_url_env(),
            model: None,
            model_env: default_image_caption_model_env(),
            timeout_seconds: default_llm_timeout(),
            retry_5xx_attempts: default_retry_5xx_attempts(),
            max_output_tokens: default_image_caption_max_output_tokens(),
            temperature: default_image_caption_temperature(),
            system_prompt: default_image_caption_system_prompt(),
            user_prompt: default_image_caption_user_prompt(),
            max_images_per_summary: default_image_caption_max_images(),
            max_concurrent_requests: default_image_caption_max_concurrent_requests(),
            request_body_overrides: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct VideoCaptionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_video_caption_provider")]
    pub provider: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_video_caption_api_key_env")]
    pub api_key_env: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_video_caption_base_url_env")]
    pub base_url_env: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_video_caption_model_env")]
    pub model_env: String,
    #[serde(default = "default_video_caption_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "default_retry_5xx_attempts")]
    pub retry_5xx_attempts: usize,
    #[serde(default = "default_video_caption_max_output_tokens")]
    pub max_output_tokens: u32,
    #[serde(default = "default_image_caption_temperature")]
    pub temperature: f32,
    #[serde(default = "default_video_caption_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_video_caption_user_prompt")]
    pub user_prompt: String,
    #[serde(default = "default_video_caption_max_videos")]
    pub max_videos_per_summary: usize,
    #[serde(default = "default_video_caption_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(default = "default_video_caption_max_video_bytes")]
    pub max_video_bytes: u64,
    #[serde(default)]
    pub request_body_overrides: BTreeMap<String, toml::Value>,
}

impl Default for VideoCaptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: default_video_caption_provider(),
            api_key: None,
            api_key_env: default_video_caption_api_key_env(),
            base_url: None,
            base_url_env: default_video_caption_base_url_env(),
            model: None,
            model_env: default_video_caption_model_env(),
            timeout_seconds: default_video_caption_timeout(),
            retry_5xx_attempts: default_retry_5xx_attempts(),
            max_output_tokens: default_video_caption_max_output_tokens(),
            temperature: default_image_caption_temperature(),
            system_prompt: default_video_caption_system_prompt(),
            user_prompt: default_video_caption_user_prompt(),
            max_videos_per_summary: default_video_caption_max_videos(),
            max_concurrent_requests: default_video_caption_max_concurrent_requests(),
            max_video_bytes: default_video_caption_max_video_bytes(),
            request_body_overrides: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct VoiceTranscriptionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_voice_transcription_provider")]
    pub provider: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_voice_transcription_api_key_env")]
    pub api_key_env: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_voice_transcription_base_url_env")]
    pub base_url_env: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_voice_transcription_model_env")]
    pub model_env: String,
    #[serde(default = "default_llm_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "default_retry_5xx_attempts")]
    pub retry_5xx_attempts: usize,
    #[serde(default = "default_voice_transcription_language")]
    pub language: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default = "default_voice_transcription_response_format")]
    pub response_format: String,
    #[serde(default = "default_voice_transcription_transcode_to_mp3")]
    pub transcode_to_mp3: bool,
    #[serde(default = "default_voice_transcription_ffmpeg_executable")]
    pub ffmpeg_executable: String,
    #[serde(default = "default_voice_transcription_mp3_bitrate")]
    pub mp3_bitrate: String,
    #[serde(default = "default_voice_transcription_max_voices")]
    pub max_voices_per_summary: usize,
    #[serde(default = "default_voice_transcription_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(default)]
    pub request_body_overrides: BTreeMap<String, toml::Value>,
}

impl Default for VoiceTranscriptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: default_voice_transcription_provider(),
            api_key: None,
            api_key_env: default_voice_transcription_api_key_env(),
            base_url: None,
            base_url_env: default_voice_transcription_base_url_env(),
            model: None,
            model_env: default_voice_transcription_model_env(),
            timeout_seconds: default_llm_timeout(),
            retry_5xx_attempts: default_retry_5xx_attempts(),
            language: default_voice_transcription_language(),
            prompt: String::new(),
            response_format: default_voice_transcription_response_format(),
            transcode_to_mp3: default_voice_transcription_transcode_to_mp3(),
            ffmpeg_executable: default_voice_transcription_ffmpeg_executable(),
            mp3_bitrate: default_voice_transcription_mp3_bitrate(),
            max_voices_per_summary: default_voice_transcription_max_voices(),
            max_concurrent_requests: default_voice_transcription_max_concurrent_requests(),
            request_body_overrides: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub http: Option<String>,
    #[serde(default)]
    pub https: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    pub output_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_cleanup_days")]
    pub cleanup_after_days: u32,
    #[serde(default = "default_max_log_mb")]
    pub max_log_mb: u64,
}

fn default_true() -> bool {
    true
}

fn default_text_content_types() -> Vec<String> {
    vec!["text".to_string()]
}

fn default_python_executable() -> String {
    ".\\.venv\\Scripts\\python.exe".to_string()
}

fn default_wx4py_sidecar_script() -> String {
    "..\\scripts\\wx4py_sidecar.py".to_string()
}

fn default_wx4py_ready_timeout() -> u64 {
    60
}

fn default_discord_token_env() -> String {
    "DISCORD_BOT_TOKEN".to_string()
}

fn default_wx_cli_executable() -> String {
    "builtin".to_string()
}

fn default_wx_cli_export_format() -> String {
    "json".to_string()
}

fn default_wx_cli_timeout_seconds() -> u64 {
    20
}

fn default_wx_cli_history_query_timeout_seconds() -> u64 {
    45
}

fn default_wx_cli_temp_dir() -> String {
    ".\\runtime\\wx-exports".to_string()
}

fn default_fallback_minutes() -> i64 {
    30
}

fn default_fixed_hours() -> i64 {
    2
}

fn default_successful_request_cooldown_seconds() -> i64 {
    300
}

fn default_scheduled_local_hour() -> u32 {
    22
}

fn default_scheduled_range_hours() -> i64 {
    24
}

fn default_history_max_messages() -> usize {
    10_000
}

fn default_max_chars() -> usize {
    20_000
}

fn default_llm_timeout() -> u64 {
    120
}

fn default_retry_5xx_attempts() -> usize {
    5
}

fn default_llm_base_url_env() -> String {
    "LLM_BASE_URL".to_string()
}

fn default_llm_model_env() -> String {
    "LLM_MODEL".to_string()
}

fn default_image_base_url_env() -> String {
    "IMAGE_BASE_URL".to_string()
}

fn default_image_model_env() -> String {
    "IMAGE_MODEL".to_string()
}

fn default_image_caption_provider() -> String {
    "openai_compatible".to_string()
}

fn default_image_caption_api_key_env() -> String {
    "IMAGE_CAPTION_API_KEY".to_string()
}

fn default_image_caption_base_url_env() -> String {
    "IMAGE_CAPTION_BASE_URL".to_string()
}

fn default_image_caption_model_env() -> String {
    "IMAGE_CAPTION_MODEL".to_string()
}

fn default_voice_transcription_provider() -> String {
    "openai_compatible".to_string()
}

fn default_video_caption_provider() -> String {
    "stepfun".to_string()
}

fn default_voice_transcription_api_key_env() -> String {
    "VOICE_TRANSCRIPTION_API_KEY".to_string()
}

fn default_video_caption_api_key_env() -> String {
    "VIDEO_CAPTION_API_KEY".to_string()
}

fn default_voice_transcription_base_url_env() -> String {
    "VOICE_TRANSCRIPTION_BASE_URL".to_string()
}

fn default_video_caption_base_url_env() -> String {
    "VIDEO_CAPTION_BASE_URL".to_string()
}

fn default_voice_transcription_model_env() -> String {
    "VOICE_TRANSCRIPTION_MODEL".to_string()
}

fn default_video_caption_model_env() -> String {
    "VIDEO_CAPTION_MODEL".to_string()
}

fn default_image_timeout() -> u64 {
    300
}

fn default_image_poll_initial_delay() -> u64 {
    10
}

fn default_image_poll_interval() -> u64 {
    5
}

fn default_max_output_tokens() -> u32 {
    2_000
}

fn default_llm_max_concurrent_chunk_requests() -> usize {
    4
}

fn default_temperature() -> f32 {
    0.3
}

fn default_image_caption_max_output_tokens() -> u32 {
    500
}

fn default_video_caption_max_output_tokens() -> u32 {
    800
}

fn default_video_caption_timeout() -> u64 {
    180
}

fn default_image_caption_temperature() -> f32 {
    0.1
}

fn default_image_caption_max_images() -> usize {
    20
}

fn default_image_caption_max_concurrent_requests() -> usize {
    4
}

fn default_video_caption_max_videos() -> usize {
    5
}

fn default_video_caption_max_concurrent_requests() -> usize {
    2
}

fn default_video_caption_max_video_bytes() -> u64 {
    128 * 1024 * 1024
}

fn default_voice_transcription_language() -> String {
    "zh".to_string()
}

fn default_voice_transcription_response_format() -> String {
    "json".to_string()
}

fn default_voice_transcription_transcode_to_mp3() -> bool {
    true
}

fn default_voice_transcription_ffmpeg_executable() -> String {
    "ffmpeg".to_string()
}

fn default_voice_transcription_mp3_bitrate() -> String {
    "64k".to_string()
}

fn default_voice_transcription_max_voices() -> usize {
    20
}

fn default_voice_transcription_max_concurrent_requests() -> usize {
    2
}

fn default_text_summary_system_prompt() -> String {
    "你是一位专业的微信群聊总结助手。请基于聊天记录输出适合直接发回微信群的中文文字总结。要求：覆盖充分、准确、按时间和话题组织；不要为了简短只保留少数大类；不要输出 JSON 或 Markdown；不要编造聊天记录中没有的信息。".to_string()
}

fn default_text_summary_user_prompt_template() -> String {
    r#"请总结以下群聊记录，输出适合直接发回群里的中文总结。

输出要求：
1. 优先保证信息覆盖完整，其次再压缩篇幅；不要只写四五个宽泛主题。
2. 对 24h/48h/多日范围的聊天，尽量按时间推进和话题聚类，覆盖主要讨论、反复出现的话题、重要人物动态、结论、待办或未解决事项。
3. 每个主题写清楚“谁在聊、聊了什么、有什么结论/变化”，不要只给抽象标签。
4. 如果聊天量很大，至少输出 8-12 个较具体的主题；每个主题 2-4 句，允许比短摘要更长。
5. 对图片、语音、视频转述内容，按其所在话题自然合并；如果媒体很多，说明主要媒体内容趋势。
6. 忽略无意义刷屏、重复寒暄和机器人提示，但不要漏掉持续讨论或多人参与的话题。
7. 不要编造记录中没有的信息。

聊天记录：
{chat_input}"#
        .to_string()
}

fn default_image_summary_system_prompt() -> String {
    "你是一个微信聊天记录数据分析助手。请把聊天记录整理成适合后续生成信息图的数据报告，重点包含关键数字、热点话题、活跃用户、关键词、时间分布和可视化文案。".to_string()
}

fn default_image_summary_user_prompt_template() -> String {
    "{chat_input}".to_string()
}

fn default_image_prompt_system_prompt() -> String {
    "你是专业的信息图生图提示词工程师。请把群聊分析结果转写成适合图像生成模型的完整中文提示词，只输出提示词本身，不要输出解释。".to_string()
}

fn default_image_prompt_user_prompt_template() -> String {
    r#"图片总结 LLM 的群聊分析结果如下：
{image_summary}

原始聊天记录与统计输入如下：
{chat_input}

请生成一段用于生图模型的提示词。要求：
- 竖版手机海报，适合微信群分享
- 中文信息图，包含标题、关键数字、热点话题、活跃用户、时间分布和简短洞察
- 视觉风格现代、清晰、信息密度适中
- 不要要求展示真实手机号、邮箱、身份证、地址等隐私信息
- 只输出最终生图 prompt，不要 Markdown，不要 JSON"#
        .to_string()
}

fn default_image_caption_system_prompt() -> String {
    "你是图片转述助手。请客观描述图片中的可见内容，重点提取与群聊上下文相关的信息；不要推断不可见的私人信息，不要编造。".to_string()
}

fn default_image_caption_user_prompt() -> String {
    "请用一到三句话中文转述这张聊天图片的可见内容，适合插回聊天记录供群聊总结模型理解。".to_string()
}

fn default_video_caption_system_prompt() -> String {
    "你是视频转述助手。请客观描述视频中的可见内容、字幕、屏幕文字和主要动作，重点提取与群聊上下文相关的信息；不要推断不可见的私人信息，不要编造。".to_string()
}

fn default_video_caption_user_prompt() -> String {
    "请用三到六句话中文转述这个聊天视频的主要内容，适合插回聊天记录供群聊总结模型理解。".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_cleanup_days() -> u32 {
    7
}

fn default_max_log_mb() -> u64 {
    50
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privacy_redaction_defaults_to_off() {
        let cfg: PrivacyConfig = toml::from_str("").unwrap();
        assert!(!cfg.redact_enabled);
        assert!(cfg.cloud_allowed);
    }

    #[test]
    fn history_limit_defaults_to_page_size() {
        let cfg: HistoryConfig = toml::from_str("").unwrap();
        assert_eq!(
            cfg.max_messages
                .unwrap_or_else(default_history_max_messages),
            10_000
        );
    }

    #[test]
    fn platform_defaults_to_wx4py() {
        let cfg: PlatformConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.kind, PlatformKindConfig::Wx4py);
    }

    #[test]
    fn platform_kind_accepts_aliases_case_insensitively() {
        for value in ["wx", "WX", "微信", "wechat", "WeChat"] {
            let cfg: PlatformConfig = toml::from_str(&format!("kind = {value:?}")).unwrap();
            assert_eq!(cfg.kind, PlatformKindConfig::Wx4py);
        }

        for value in ["dc", "DC", "discord", "Discord"] {
            let cfg: PlatformConfig = toml::from_str(&format!("kind = {value:?}")).unwrap();
            assert_eq!(cfg.kind, PlatformKindConfig::Discord);
        }
    }

    #[test]
    fn rate_limit_defaults_to_five_minutes() {
        let cfg: RateLimitConfig = toml::from_str("").unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.successful_request_cooldown_seconds, 300);
        assert_eq!(cfg.successful_image_cooldown_seconds, 0);
    }

    #[test]
    fn manual_summary_defaults_to_text_only() {
        let cfg: ManualSummaryConfig = toml::from_str("").unwrap();
        assert!(!cfg.image_by_default);
    }

    #[test]
    fn manual_summary_can_enable_images_by_default() {
        let cfg: ManualSummaryConfig = toml::from_str("image_by_default = true").unwrap();
        assert!(cfg.image_by_default);
    }

    #[test]
    fn scheduled_summary_defaults_to_nightly_24h_with_text_and_image() {
        let cfg: ScheduledSummaryConfig = toml::from_str("").unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.local_hour, 22);
        assert_eq!(cfg.local_minute, 0);
        assert_eq!(cfg.range_hours, 24);
        assert!(cfg.send_text);
        assert!(cfg.send_image);
        assert!(cfg.ignore_rate_limit);
    }

    #[test]
    fn summary_and_image_prompt_configs_have_defaults() {
        let llm: LlmConfig = toml::from_str(
            r#"provider = "openai_compatible"
api_key_env = "LLM_API_KEY""#,
        )
        .unwrap();
        let text_summary: TextSummaryConfig = toml::from_str("").unwrap();
        let image_summary: ImageSummaryConfig = toml::from_str("").unwrap();
        let image_prompt: ImagePromptConfig = toml::from_str("").unwrap();
        let image_caption: ImageCaptionConfig = toml::from_str("").unwrap();
        let voice_transcription: VoiceTranscriptionConfig = toml::from_str("").unwrap();

        assert_eq!(llm.max_concurrent_chunk_requests, 4);
        assert!(text_summary.enabled);
        assert!(text_summary.system_prompt.contains("文字总结"));
        assert!(text_summary.user_prompt_template.contains("{chat_input}"));
        assert!(image_summary.user_prompt_template.contains("{chat_input}"));
        assert!(image_prompt
            .user_prompt_template
            .contains("{image_summary}"));
        assert!(image_prompt.user_prompt_template.contains("{chat_input}"));
        assert!(!image_caption.enabled);
        assert_eq!(image_caption.model_env, "IMAGE_CAPTION_MODEL");
        assert_eq!(image_caption.retry_5xx_attempts, 5);
        assert_eq!(image_caption.max_concurrent_requests, 4);
        assert!(image_caption.user_prompt.contains("转述"));
        assert!(!voice_transcription.enabled);
        assert_eq!(voice_transcription.model_env, "VOICE_TRANSCRIPTION_MODEL");
        assert_eq!(voice_transcription.max_concurrent_requests, 2);
        assert_eq!(voice_transcription.language, "zh");
        assert!(voice_transcription.transcode_to_mp3);
        assert_eq!(voice_transcription.ffmpeg_executable, "ffmpeg");
        assert_eq!(voice_transcription.mp3_bitrate, "64k");
    }

    #[test]
    fn wx4py_and_wxdb_configs_have_defaults() {
        let wx4py: Wx4pyConfig = toml::from_str("").unwrap();
        let wxdb: WxCliConfig = toml::from_str("").unwrap();
        let discord: DiscordConfig = toml::from_str("").unwrap();

        assert!(wx4py.python_executable.contains("python"));
        assert!(wx4py.sidecar_script.contains("wx4py_sidecar.py"));
        assert_eq!(discord.token_env, "DISCORD_BOT_TOKEN");
        assert!(discord.channels.is_empty());
        assert_eq!(wxdb.executable, "builtin");
        assert_eq!(wxdb.export_format, "json");
        assert_eq!(wxdb.max_messages, None);
        assert_eq!(wxdb.timeout_seconds, 20);
        assert_eq!(wxdb.history_query_timeout_seconds, 45);
        assert!(wxdb.cache_dir.is_empty());
    }

    #[test]
    fn default_agent_config_accepts_wxdb_section() {
        let cfg = AgentConfig::from_toml_str(include_str!("../../../config/agent.toml")).unwrap();
        assert_eq!(cfg.wx_cli.executable, "builtin");
        assert_eq!(cfg.history_message_limit(), 10_000);
        assert_eq!(cfg.runtime.max_log_mb, 50);
    }

    #[test]
    fn llm_config_accepts_request_body_overrides() {
        let cfg: LlmConfig = toml::from_str(
            r#"
provider = "openai_compatible"
api_key_env = "LLM_API_KEY"
base_url_env = "LLM_BASE_URL"
model_env = "LLM_MODEL"

[request_body_overrides]
enable_thinking = false
reasoning_effort = "none"
"#,
        )
        .unwrap();

        assert_eq!(cfg.retry_5xx_attempts, 5);
        assert_eq!(
            cfg.request_body_overrides
                .get("enable_thinking")
                .and_then(toml::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            cfg.request_body_overrides
                .get("reasoning_effort")
                .and_then(toml::Value::as_str),
            Some("none")
        );
    }
}
