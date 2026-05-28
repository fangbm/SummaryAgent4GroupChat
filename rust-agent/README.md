# Windows-Only Rust WeChat Summary Agent

This workspace is an isolated Rust implementation path for the Windows-only WeChat summary agent.
It does not import or modify the existing Python packages under `../src`.

## Workspace Layout

```text
crates/app/          # CLI entrypoint and runtime wiring
crates/app/src/platform.rs
                    # platform adapter boundary; wx4py is the first implementation
crates/core/         # config, models, trigger matching, formatting, privacy switches
crates/wx4py-client/# wx4py sidecar and wx-cli history adapter
crates/ai/           # OpenAI-compatible LLM and image clients
crates/storage/      # SQLite state store for per-room trigger timestamps
config/agent.toml    # Windows-only default config
```

## Pipeline Switches

The runtime platform is selected through `[platform]`:

```toml
[platform]
kind = "wx4py"
```

`wx4py` is the implemented Windows adapter. The config model also accepts `discord` as a
reserved value, but the app currently returns a clear "not implemented" startup error for it.

`config/agent.toml` has two independent output switches:

```toml
[text_summary]
enabled = true

[image_gen]
enabled = true
```

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
