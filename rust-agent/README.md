# Rust Chat Summary Agent

This workspace is an isolated Rust implementation path for the chat summary agent.
It does not import or modify the existing Python packages under `../src`.

## Workspace Layout

```text
crates/app/          # CLI entrypoint and runtime wiring
crates/app/src/platform.rs
                    # platform adapter boundary; wx4py and Discord implementations
crates/control/      # current-user Named Pipe control service for the WinUI shell
crates/core/         # config, models, trigger matching, formatting, privacy switches
crates/wx4py-client/# wx4py sidecar and wxdb history adapter
crates/ai/           # OpenAI-compatible LLM and image clients
crates/storage/      # SQLite state store for per-room trigger timestamps
config/agent.toml    # default local config
```

## Pipeline Switches

The runtime platform is selected through `[platform]`:

```toml
[platform]
kind = "wx"
```

The platform value is case-insensitive and accepts `wx`, `微信`, `wechat`, `dc`, and
`discord`. `wx` maps to the wx4py/WeChat adapter; `dc` and `discord` start the
Discord bot adapter.

For Discord:

```toml
[platform]
kind = "discord"

[discord]
token_env = "DISCORD_BOT_TOKEN"
channels = ["123456789012345678"]
```

Enable the bot's Message Content privileged intent in the Discord Developer Portal.
The bot also needs channel permissions to view channels, read message history, send
messages, and attach files when image summaries are enabled.

Manual summaries accept an optional target platform before the time range:

```text
/总结 [platform] [time] [img]
```

Examples:

- `/总结 1h`: summarize the platform that received the command.
- `/总结 1h 图片`: summarize the latest hour and toggle image generation for this request.
- `/总结 微信 1d`: summarize wx4py/WeChat history for the last day.
- `/总结 wx 1d img`: same as above, toggling image generation for this request.
- `/总结 WECHAT 1d`: same as `wx`, with case-insensitive parsing.
- `/总结 discord 2h`: summarize the Discord channel that received the command when
  the app is running with the Discord adapter.

`platform` accepts `wx` / `微信` / `wechat` / `dc` / `discord`, case-insensitively.
`img` accepts `图片` / `image` / `img`, case-insensitively for the English aliases.
It toggles manual image generation relative to `manual_summary.image_by_default`.

`config/agent.toml` has independent output switches:

```toml
[manual_summary]
image_by_default = false

[text_summary]
enabled = true

[image_gen]
enabled = true
```

When `manual_summary.image_by_default = false`, manual commands generate only text
unless the image argument is present. When it is `true`, manual commands generate
images by default and the image argument skips image generation for that request.
Scheduled summaries still use `scheduled_summary.send_image`.

`history.max_messages` is the per-read page size for platform history. The app
keeps reading pages until the requested time range is covered or the platform has
no older messages. LLM input is not capped by message count; it is capped only by
`privacy.max_chars_to_llm`. When long chat input exceeds that limit, the app
splits it into whole-message chunks and sends up to
`llm.max_concurrent_chunk_requests` chunk requests at the same time.

Provider-specific chat completion request fields can be added or overridden through
`llm.request_body_overrides`, for example `enable_thinking = false`.

### Multi-key concurrency

Every AI client section (`llm`, `image_gen`, `image_caption`, `video_caption`,
`voice_transcription`) supports multiple API keys to lift the single-account
concurrency ceiling. Requests are distributed round-robin across keys, and each
key can be capped with `max_concurrent_per_key` (0 = unlimited, the default).

Key resolution priority (first non-empty wins):

1. `api_keys = ["sk-1", "sk-2"]` — explicit list in `config/agent.toml`.
2. `api_key = "sk-1,sk-2"` — the single-key field also accepts a comma/newline
   separated list.
3. `api_keys_env = "LLM_API_KEYS"` — env var holding comma/newline separated
   keys (defaults: `LLM_API_KEYS`, `IMAGE_API_KEYS`, `IMAGE_CAPTION_API_KEYS`,
   `VIDEO_CAPTION_API_KEYS`, `VOICE_TRANSCRIPTION_API_KEYS`).
4. `api_key_env = "LLM_API_KEY"` — the original single-key env var.

```toml
[llm]
api_key_env = "LLM_API_KEY"
api_keys = ["sk-account-1", "sk-account-2"]
max_concurrent_per_key = 1   # each account serves at most 1 concurrent request
```

With two keys and `max_concurrent_per_key = 1` the effective concurrency ceiling
is 2 across the whole process (all rooms/tasks share the key pool), instead of 1
per single account. The per-task batch limits (`max_concurrent_chunk_requests`,
`max_concurrent_requests`) still cap each individual summary task; the key pool
adds a global per-account cap on top.

Optional image and voice preprocessing can enrich the chat input before summarizing:
`image_caption` describes decoded images, while `voice_transcription` sends decoded
voice files to an OpenAI-compatible `/audio/transcriptions` endpoint and inserts
the transcript back into the original voice-message line. By default local voice
files are converted to MP3 first through `voice_transcription.ffmpeg_executable`
so transcription providers do not need to support WeChat Silk/AMR directly.

The text and image paths use separate prompts:

- `text_summary.system_prompt`: direct text summary sent back to the group.
- `image_summary.system_prompt`: structured/image-oriented summary used only for image generation.
- `image_prompt.system_prompt`: converts the image summary into the final image model prompt.

When both switches are enabled, the app may call the LLM three times:

1. Text path: chat history -> text summary -> send text.
2. Image path step 1: chat history -> image summary.
3. Image path step 2: chat history + image summary -> image prompt -> image model.

## Development

```powershell
cargo test --workspace

$env:LLM_API_KEY="sk-..."
$env:LLM_BASE_URL="https://api.openai.com/v1"
$env:LLM_MODEL="gpt-4o-mini"
$env:IMAGE_API_KEY="sk-..."
$env:IMAGE_BASE_URL="https://api.openai.com/v1"
$env:IMAGE_MODEL="gpt-image-1.5"

cargo run -p wechat-summary-app -- --config config\agent.toml
```

The real wx4py runtime requires Windows WeChat to be logged in. Configure `[wx4py].groups`
with WeChat group display names. Install a compatible external history provider separately and
configure its command or absolute path in `[wxdb].executable`; this repository does not bundle it.

`wechat-summary-control` is a local, single-user control service used by the WinUI
application. It owns the main agent process, validates and atomically writes TOML
configuration, redacts secrets in output, and exposes only a fixed set of privileged
maintenance actions. It is not intended to be exposed over the network.
