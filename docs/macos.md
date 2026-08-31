# macOS Native App

`macos-ui` is the native SwiftUI client for SummaryAgent4GroupChat. It uses the
same Rust control API as the WinUI app, but connects through a current-user
Unix Domain Socket instead of a Windows Named Pipe.

## Supported Scope

- Discord receiving, history, commands, summaries, image commands, task
  persistence and Outbox use the existing Rust application path.
- The SwiftUI app can select a configuration file, launch the local control
  service, start or stop the Rust agent, show status and read the sanitized log
  tail.
- The app intentionally marks wx4py and wxdb as Windows-only. They require the
  Windows WeChat client and are never presented as runnable macOS features.

The configuration file remains compatible with the Windows application. For a
Mac host, configure `[platform].kind = "discord"`, then use the same LLM,
NovelAI and media configuration sections. The first release keeps complex TOML
editing external while the SwiftUI form pages are migrated; API keys remain
write-only at the Rust control boundary.

## Development Build

Requirements: macOS 14+, Xcode 16 or later, Swift 6, Rust 1.87 or later.

```bash
cd rust-agent
cargo build --release -p wechat-summary-app -p wechat-summary-control

cd ../macos-ui
export SUMMARY_AGENT_CONTROL_PATH="$(cd ../rust-agent && pwd)/target/release/wechat-summary-control"
swift run
```

In the app, choose the `agent.toml` to use. The Rust control service creates a
unique socket below the user's temporary directory with mode `0600`, and the
SwiftUI app passes an ephemeral per-launch token through the child process
environment. No TCP listener is opened.

## Packaging Direction

The production `.app` will bundle `wechat-summary-app` and
`wechat-summary-control` in `SummaryAgent4GroupChat.app/Contents/Resources/bin`.
The next packaging step is universal `aarch64-apple-darwin` and
`x86_64-apple-darwin` Rust builds, followed by codesigning and notarization.
CI already compiles the SwiftUI package on macOS; signing requires an Apple
Developer certificate and notarization credentials, which are intentionally not
committed to this repository.
