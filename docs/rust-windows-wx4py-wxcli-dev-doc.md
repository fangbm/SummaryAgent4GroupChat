# Windows 单机 Rust + wx4py + wx-cli 开发文档

> 目标平台: Windows 10/11 + Windows 微信客户端 + `wx4py` + `wx-cli`  
> 分支目标: 在新分支中替换 wxhook 链路，不改动原分支产物  
> 代码边界: `rust-agent/` 继续作为独立 Rust workspace

## 1. 目标

本分支将原 wxhook 接入替换为两段式 Windows 本机方案：

- `wx4py`: 使用 Windows UI 自动化模拟操作微信，负责监听群消息、发送文本、发送图片。
- `wx-cli`: 解密并导出本机微信数据库，负责按时间范围读取历史聊天记录。
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
| wx-cli export ---- decrypted chat history JSON ----> Rust Agent |
+-----------------------------------------------------------------+
```

端到端流程：

```text
微信群消息 -> wx4py sidecar -> Rust
  -> 触发词匹配 -> SQLite 读取上次触发时间
  -> wx-cli export 解密导出历史聊天
  -> 格式化原文 + 用户发言统计 -> 可选隐私脱敏
  -> text_summary LLM 可选回发文字
  -> image_summary LLM -> image_prompt LLM -> Image API 可选生图
  -> wx4py sidecar 模拟发送图片到同群
```

## 3. Workspace

```text
rust-agent/
├── crates/
│   ├── app/            # CLI 入口和运行时编排
│   ├── core/           # 配置、模型、触发、统计、隐私、时间范围
│   ├── wx4py-client/   # wx4py sidecar + wx-cli 历史导出适配
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
kind = "wx4py"

[wx4py]
python_executable = "..\\.venv\\Scripts\\python.exe"
sidecar_script = "..\\scripts\\wx4py_sidecar.py"
ready_timeout_seconds = 60
groups = ["你的微信群显示名"]

[wx_cli]
executable = "wx"
export_format = "json"
max_messages = 5000
temp_dir = ".\\runtime\\wx-exports"
group_name_map = {}
```

注意：

- `platform.kind = "wx4py"` 是当前已实现的平台；`discord` 作为后续接入保留值，当前启动会给出未实现错误。
- `wx4py.groups` 必须是微信里能搜索到的群显示名；wx4py 不能像 wxhook 一样按底层 `@chatroom` ID 全量监听。
- 若 `wx4py.groups` 为空，Rust 会回退使用 `listen.whitelist_rooms`；两者都为空时启动失败。
- `wx_cli.group_name_map` 可把触发事件里的群名映射到 `wx-cli export` 可识别的聊天名。

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
- `wx` 命令可执行，或 `[wx_cli].executable` 指向完整路径。
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

- wx-cli export 命令生成。
- wx-cli JSON 消息规范化。
- 北京时间与 UTC 转换。
- text/image 两套提示词链路分离。

## 9. 风险

- wx4py 是 UI 自动化方案，依赖微信窗口、控件树和焦点，稳定性低于协议/API 注入方案。
- wx4py 官方说明微信 4.x UIA 对发送者信息暴露有限，因此监听事件中发送者可能为空；历史统计以 wx-cli 导出结果为准。
- wx-cli 解密读取本机数据库有合规风险，只应在用户本人授权和本机数据范围内使用。
