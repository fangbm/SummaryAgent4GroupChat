# Windows 单机 Rust + wxhook 开发文档

> 项目代号: WeChat-AI-Pipeline Rust Agent  
> 版本: v0.2  
> 日期: 2026-05-23  
> 目标平台: Windows 10/11 + Windows 微信客户端 + `miloira/wxhook`  
> 代码边界: `rust-agent/` 为独立 Rust workspace，与现有 Python 项目隔离

## 1. 目标与边界

新版实现改为 Windows 单机闭环，并将微信接入层从 WeChatFerry 替换为 `miloira/wxhook`：

1. wxhook 注入 Windows 微信客户端并暴露本地 HTTP API。
2. Rust Agent 调用 wxhook API 完成登录检查、消息 hook、SQL 查询、发送文本和发送图片。
3. Rust Agent 自己启动 TCP callback listener，接收 wxhook 同步过来的群消息。
4. 现有 Python 项目保留为历史/兼容实现；Rust 代码全部放入 `rust-agent/`，不依赖 `src/` 下 Python 包。
5. 隐私脱敏为可选项，默认关闭。启用后才会对 wxid、手机号、邮箱等内容做脱敏。

废弃内容：

- Linux Bot / `wx-bot-cli`
- Linux 与 Windows 之间的 WebSocket IPC
- Windows 到 Linux 的 HTTP 图片下载
- WeChatFerry / `wcferry`
- NNG / Protobuf / `wcf.proto` 直连协议

## 2. 架构

```text
+------------------------ Windows Host ------------------------+
|  WeChat PC                                                   |
|     | wxhook.dll / start-wechat.exe                          |
|     v                                                        |
|  wxhook local API                                            |
|     | HTTP: http://127.0.0.1:19001+                          |
|     | callback: TCP 127.0.0.1:18999                          |
|     v                                                        |
|  rust-agent                                                  |
|   - wxhook-client: HTTP API + TCP callback adapter           |
|   - core: trigger, formatter, privacy, time range            |
|   - storage: SQLite room state                               |
|   - ai: LLM + image generation                               |
|   - app: runtime wiring                                      |
+--------------------------------------------------------------+
```

端到端流程：

```text
微信群消息 -> wxhook hookSyncMsg -> Rust TCP callback listener
  -> 触发词匹配 -> SQLite 读取上次触发时间
  -> wxhook execSql 查询历史
  -> 格式化原文 + 用户发言统计 -> 可选隐私脱敏
  -> 可选 LLM 文字总结
  -> 可选回发文字总结
  -> 可选 LLM 图片总结
  -> 可选 LLM 生成生图 prompt
  -> 可选 Image API 生图
  -> wxhook sendTextMsg / sendImagesMsg 回发同群
```

## 3. Rust Workspace

```text
rust-agent/
├── Cargo.toml
├── config/agent.toml
└── crates/
    ├── app/            # CLI 入口和运行时编排
    ├── core/           # 配置、模型、触发、统计、隐私开关、时间范围
    ├── wxhook-client/  # wxhook HTTP API 和 TCP callback 适配
    ├── ai/             # OpenAI-compatible LLM / Image API
    └── storage/        # SQLite 状态
```

关键依赖：

- `reqwest`: 调用 wxhook、LLM 和图片生成 API。
- `serde_json`: 解析 wxhook 原始事件和 API 响应。
- `rusqlite`: 保存每个群聊的 `last_trigger_time`。
- `tokio`: CLI runtime。

## 4. wxhook 接入

Rust Agent 不调用 wxhook Python API；它只依赖 wxhook 注入后暴露的本地 HTTP API。

已适配接口：

- `POST /api/checkLogin`: 检查微信登录状态。
- `POST /api/hookSyncMsg`: 将消息同步到 Rust callback listener。
- `POST /api/unhookSyncMsg`: 取消消息同步。
- `POST /api/sendTextMsg`: 降级文本回发。
- `POST /api/sendImagesMsg`: 图片回发。
- `POST /api/getDBInfo`: 获取数据库句柄。
- `POST /api/execSql`: 查询聊天历史。

消息接收：

- `wxhook-client` 使用 TCP listener 接收 wxhook 原始 JSON。
- 收到消息后返回 `200 OK`，与 wxhook `RequestHandler` 行为兼容。
- 文本群消息按 `fromUser` 或 `toUser` 是否以 `@chatroom` 结尾识别，兼容自己发送的群消息事件；`content` 中的 `wxid:\n文本` 会拆成发送者和正文。

## 5. 配置

默认配置位于 `rust-agent/config/agent.toml`：

```toml
[wxhook]
base_url = "http://127.0.0.1:19001"
request_timeout_ms = 5000
callback_host = "127.0.0.1"
callback_port = 18999
callback_timeout_seconds = 30
message_mode = "tcp"
wechat_version = "3.9.5.81"
start_command = "python -m wxhook_launcher"

[privacy]
redact_enabled = false
max_messages_to_llm = 800
max_chars_to_llm = 20000
cloud_allowed = true
sensitive_rooms = []

[llm]
api_key_env = "LLM_API_KEY"
base_url_env = "LLM_BASE_URL"
model_env = "LLM_MODEL"

[text_summary]
enabled = true
system_prompt = "生成适合直接发回微信群的简洁文字总结。"
user_prompt_template = "{chat_input}"

[image_summary]
system_prompt = "生成适合后续制作信息图的数据分析报告。"
user_prompt_template = "{chat_input}"

[image_prompt]
system_prompt = "把群聊分析结果转写成适合图像生成模型的完整中文提示词。"
user_prompt_template = """
图片总结 LLM 的群聊分析结果如下：
{image_summary}

原始聊天记录与统计输入如下：
{chat_input}

请生成最终生图 prompt。
"""

[image_gen]
enabled = true
api_key_env = "IMAGE_API_KEY"
base_url_env = "IMAGE_BASE_URL"
model_env = "IMAGE_MODEL"
```

隐私策略：

- `privacy.redact_enabled = false` 是默认值，表示原始聊天内容会进入 LLM 输入。
- 若改为 `true`，Rust 会在发送给 LLM 前替换 wxid、手机号和邮箱。
- `max_messages_to_llm` 与 `max_chars_to_llm` 保留为保护阈值，用于后续截断输入规模。

输出开关：

- `text_summary.enabled = true`: 使用 `text_summary.system_prompt` 生成适合直接发回群里的文字总结。
- `image_gen.enabled = true`: 开启图片生成，图片链路使用独立的 `image_summary.system_prompt` 先生成图片总结，再将图片总结拼入 `image_prompt.user_prompt_template` 生成最终生图 prompt。
- `image_prompt.user_prompt_template`: `{image_summary}` 会替换为图片总结结果，`{chat_input}` 会替换为聊天记录与统计输入。文字总结结果不会混入图片提示词链路。

## 6. 当前实现状态

已落地：

- `wxhook-client`: wxhook API 请求、TCP callback 事件解析、历史 SQL 生成。
- `core`: 配置、触发匹配、时间范围、聊天格式化、用户统计、隐私开关。
- `storage`: SQLite 群状态。
- `ai`: OpenAI-compatible LLM completion 和图片生成客户端。
- `app`: 读取配置、连接 wxhook API、注册消息 hook、接收事件、匹配触发、查询历史、按开关分别调用文字总结 LLM 与图片总结 LLM、生成生图 prompt、可选生图、回发结果并保存触发时间。

后续增强：

- 更完整的群成员昵称映射。
- 后台任务队列，避免长时间 LLM / Image 调用阻塞后续回调处理。
- 可选实现 wxhook 启动器，调用 `start-wechat.exe wxhook.dll <port>`。

## 7. 开发与运行

准备：

1. 安装 Rust stable。
2. 安装 wxhook，并确认 Windows 微信版本与 wxhook 支持版本匹配。
3. 启动 wxhook 注入，使本地 API 监听在 `wxhook.base_url`。
4. 设置环境变量：

```powershell
$env:LLM_API_KEY="sk-..."
$env:LLM_BASE_URL="https://api.openai.com/v1"
$env:LLM_MODEL="gpt-4o-mini"
$env:IMAGE_API_KEY="sk-..."
$env:IMAGE_BASE_URL="https://api.openai.com/v1"
$env:IMAGE_MODEL="gpt-image-1.5"
```

开发命令：

```powershell
cd rust-agent
cargo fmt --check
cargo test --workspace
cargo run -p wechat-summary-app -- --config config\agent.toml
```

## 8. 错误处理与降级

| 错误 | 场景 | 策略 |
|---|---|---|
| `WXHOOK_NOT_LOGIN` | 微信未登录或 wxhook API 未就绪 | 停止启动并提示重新登录 |
| `WXHOOK_API_FAILED` | wxhook 返回非 200 code | 记录 endpoint 和响应 |
| `WXHOOK_DB_QUERY_FAILED` | 数据库查询失败 | 记录 roomid、时间范围、SQL |
| `NO_HISTORY` | 时间段无文本消息 | 回发“没有可总结的文本消息” |
| `LLM_FAILED` | 摘要失败 | 重试后回发失败文本 |
| `IMAGE_FAILED` | 生图失败 | 降级为文本摘要 |
| `SEND_FAILED` | 回发失败 | 记录图片路径和目标 roomid，允许人工重发 |

## 9. 测试计划

已落地的测试方向：

- wxhook `hookSyncMsg` 请求字段兼容性。
- wxhook 群文本事件解析。
- 历史 SQL 转义。
- 触发词匹配、聊天格式化、用户统计、隐私脱敏开关。
- SQLite last trigger 持久化。

后续应补：

- mock HTTP server 验证 wxhook API endpoint 和 payload。
- fixture `execSql` 结果到 `ChatMessage` 映射。
- 端到端 fake pipeline：消息 -> 摘要 -> 图片 -> `sendImagesMsg`。
- Windows 真机验收：群内发送 `/总结` 后回发图片。

## 10. 合规与安全

- 本工具仅用于本人微信账号和已获得授权的群聊。
- wxhook 通过 DLL 注入读取本机微信数据，具有版本、稳定性和合规风险。
- 默认关闭隐私脱敏是为了保留摘要准确性；如果群聊包含敏感个人信息，应启用 `privacy.redact_enabled` 或切换本地 LLM。
- API Key 只允许通过环境变量或本地未提交配置注入，不得写入仓库。
