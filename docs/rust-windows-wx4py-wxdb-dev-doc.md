# Rust 群聊总结 Agent 开发文档

> 目标平台: Windows 10/11 + Windows 微信客户端 + `wx4py` + 内置 `wxdb`，以及 Discord Bot  
> 分支目标: 在新分支中替换 wxhook 链路，并抽象出可接入 Discord 的平台层  
> 代码边界: `rust-agent/` 继续作为独立 Rust workspace

## 1. 目标

本分支将原 wxhook 接入替换为两段式 Windows 本机方案：

- `wx4py`: 使用 Windows UI 自动化模拟操作微信，负责监听群消息、发送文本、发送图片。
- `wxdb`: 解密并读取本机微信数据库，负责按时间范围读取历史聊天记录。
- Discord Bot: 通过 Gateway 接收频道消息，通过 REST 读取频道历史与发送总结。
- Rust Agent: 负责触发匹配、时间范围、状态、LLM、图片生成和流程编排。

废弃本分支默认链路中的内容：

- wxhook HTTP API、`hookSyncMsg`、`execSql`
- WeChatFerry / `wcferry`
- Linux Bot / `wx-bot-cli`

## 2. 架构

```text
-------------------------- Windows Host --------------------------+
| WeChat PC                                                       |
|   ^  wx4py UI Automation: listen / send text / send image       |
|   |                                                             |
| wx4py_sidecar.py <---- JSON lines stdin/stdout ----> Rust Agent |
|                                                                 |
| wxdb decrypted chat history ----> Rust Agent                    |
+-----------------------------------------------------------------+

Discord Gateway MESSAGE_CREATE ----> Rust Agent
Discord REST channel messages -----> Rust Agent
Rust Agent ---- Discord REST create message / attachment ----> Discord channel
```

端到端流程：

```text
微信群消息 / Discord 频道消息 -> 平台适配层 -> Rust
  -> 触发词匹配 -> 解析 /总结 [platform] [time] [img] -> SQLite 读取上次触发时间
  -> wxdb 或 Discord REST 读取历史聊天
  -> 格式化原文 + 用户发言统计 -> 可选隐私脱敏
  -> text_summary LLM 可选回发文字
  -> image_summary LLM -> image_prompt LLM -> Image API 可选生图
  -> 平台适配层发送文字 / 图片到同群或同频道
```

## 3. Workspace

```text
rust-agent/
├── crates/
│   ├── app/            # CLI 入口和运行时编排
│   ├── core/           # 配置、模型、触发、统计、隐私、时间范围
│   ├── wx4py-client/   # wx4py sidecar + wxdb 历史读取适配
│   ├── app/src/platform.rs # wx4py / Discord 平台适配边界
│   ├── ai/             # OpenAI-compatible LLM / Image API
│   └── storage/        # SQLite 状态
└── config/agent.toml

scripts/
└── wx4py_sidecar.py    # Python sidecar，调用 wx4py UI 自动化能力
```

## 4. 配置

`rust-agent/config/agent.toml` 新增主要配置：

```toml
[platform]
kind = "wx"

[wx4py]
python_executable = "..\\.venv\\Scripts\\python.exe"
sidecar_script = "..\\scripts\\wx4py_sidecar.py"
ready_timeout_seconds = 60
groups = ["你的微信群显示名"]

[discord]
token_env = "DISCORD_BOT_TOKEN"
channels = ["Discord 频道 ID"]

[wxdb]
executable = "builtin"
export_format = "json"
timeout_seconds = 20
history_query_timeout_seconds = 45
temp_dir = ".\\runtime\\wx-exports"
cache_dir = ""
group_name_map = {}
```

注意：

- `platform.kind` 大小写不敏感，支持 `wx` / `微信` / `wechat` / `dc` / `discord`；`wx` 走 wx4py/微信适配器，`dc` / `discord` 走 Discord Bot 适配器。
- `wx4py.groups` 必须是微信里能搜索到的群显示名；wx4py 不能像 wxhook 一样按底层 `@chatroom` ID 全量监听。
- 若 `wx4py.groups` 为空，Rust 会回退使用 `listen.whitelist_rooms`；两者都为空时启动失败。
- `wxdb.group_name_map` 可把触发事件里的群名映射到 wxdb 可识别的聊天名。
- `wxdb.cache_dir` 控制内置 wxdb 的解密快照和媒体缓存位置；留空时使用用户目录下的默认缓存。
- `wxdb.history_query_timeout_seconds` 是整段历史查询的兜底超时，防止 wxdb 或本地 cache 查询卡住后阻塞新请求。
- `discord.token_env` 默认读取 `DISCORD_BOT_TOKEN`；也可以在 `[discord]` 中直接设置 `token`。
- `discord.channels` 和 `scheduled_summary.rooms` 对 Discord 必须填写频道 ID；`listen.whitelist_rooms` 可填写频道 ID 或频道名用于触发过滤。
- Discord 需要在 Developer Portal 开启 Message Content privileged intent，并给 Bot 授权 View Channel、Read Message History、Send Messages；若开启图片总结，还需要 Attach Files。

手动总结命令格式：

```text
/总结 [platform] [time] [img]
```

- `platform` 可选；留空时默认总结发送命令的平台。
- `platform` 大小写不敏感，支持 `wx` / `微信` / `wechat` / `dc` / `discord`。
- `time` 可选；支持 `30min` / `1h` / `1d` / `30分钟` / `1小时` / `1天` 等。
- `img` 可选；支持 `图片` / `image` / `img`。当 `[manual_summary].image_by_default = false` 时，只有带该参数的手动总结会生成图片。
- `[image_gen].enabled` 是图片能力总开关；`[manual_summary].image_by_default` 只控制手动总结是否默认生图，定时总结仍由 `[scheduled_summary].send_image` 控制。
- `wx` / `微信` / `wechat` 走 wx4py/微信适配器；`discord` / `dc` 走 Discord Bot 适配器。

## 5. Sidecar 协议

sidecar 使用 JSON Lines：

从 Python 到 Rust：

```json
{"kind":"ready","ok":true}
{"kind":"event","room_id":"群名","room_name":"群名","content":"/总结","timestamp":1769178600}
{"kind":"error","message":"..."}
```

从 Rust 到 Python：

```json
{"cmd":"send_text","room":"群名","text":"..."}
{"cmd":"send_image","room":"群名","path":"C:\\path\\summary.png"}
```

## 6. 提示词链路

文字总结和图片总结完全分离：

- `[text_summary]`: 直接回发到群的文字总结提示词。
- `[image_summary]`: 给图片链路用的结构化数据分析提示词。
- `[image_prompt]`: 把 `{image_summary}` 和 `{chat_input}` 转成最终生图 prompt。
- `[image_gen]`: 对 APIMart `gpt-image-2` 使用 OpenAI 兼容入口
  `/v1/images/generations`，请求体包含 `model`、`prompt`、`n`、`size`、
  `resolution`。接口返回 `task_id` 后，程序会轮询 `/v1/tasks/{task_id}`，
  从 `data.result.images[0].url[0]` 下载图片；普通 OpenAI 同步 `url` /
  `b64_json` 响应仍兼容。

`[llm]` 与 `[image_gen]` 都支持两种密钥配置方式：

- 推荐生产方式：保留 `api_key_env` / `base_url_env` / `model_env`，从环境变量读取。
- 本机持久化方式：直接填写 `api_key` / `base_url` / `model`。这些字段非空时优先使用配置文件值。

当两个开关都开启时，最多会调用三次 LLM：

1. 文字总结 LLM: 聊天记录 -> 群内文字总结。
2. 图片总结 LLM: 聊天记录 -> 信息图数据报告。
3. 图片 prompt LLM: 聊天记录 + 图片总结 -> 生图 prompt。

## 7. 运行

```powershell
python -m pip install -e ".[windows]"
python -m pip install wx4py

cd rust-agent
cargo run -p wechat-summary-app -- --config config\agent.toml
```

运行前确认：

- Windows 微信已登录。
- `[wx4py].groups` 已填写要监听的群显示名。
- 历史读取默认使用 `[wxdb].executable = "builtin"`；需要排障时可单独运行 `bin\\wxdb.exe doctor`。
- `LLM_*` 与可选 `IMAGE_*` 环境变量已设置。

## 8. 测试

```powershell
cd rust-agent
cargo fmt --check
cargo test --workspace

cd ..
python -m py_compile scripts\wx4py_sidecar.py
```

当前自动化测试覆盖：

- wxdb 历史读取命令生成。
- wxdb JSON 消息规范化。
- 北京时间与 UTC 转换。
- text/image 两套提示词链路分离。

## 9. 风险

- wx4py 是 UI 自动化方案，依赖微信窗口、控件树和焦点，稳定性低于协议/API 注入方案。
- wx4py 官方说明微信 4.x UIA 对发送者信息暴露有限，因此监听事件中发送者可能为空；历史统计以 wxdb 读取结果为准。
- wxdb 解密读取本机数据库有合规风险，只应在用户本人授权和本机数据范围内使用。
