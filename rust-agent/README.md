# Rust Chat Summary Agent

This workspace is an isolated Rust implementation path for the chat summary agent.
It does not import or modify the existing Python packages under `../src`.

## Workspace Layout

```text
crates/app/          # CLI entrypoint and runtime wiring
crates/app/src/platform.rs
                    # platform adapter boundary; wx4py and Discord implementations
crates/core/         # config, models, trigger matching, formatting, privacy switches
crates/wx4py-client/# wx4py sidecar and wx-cli history adapter
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

`history.max_messages` limits how many messages are read from any platform before
formatting. LLM input is not capped by message count; it is capped only by
`privacy.max_chars_to_llm`.

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
with WeChat group display names, and configure `[wx_cli].executable` if `wx` is not on `PATH`.
