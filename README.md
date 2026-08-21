# SummaryAgent4GroupChat

Windows 原生群聊总结助手。它监听微信或 Discord 的群聊指令，按指定时间范围读取消息，调用兼容 OpenAI Chat Completions 的模型生成中文总结，并可选生成配图后发回原群。

项目使用 WinUI 3 原生 Windows 管理界面；Rust 控制服务负责配置校验、主程序生命周期、终端与日志脱敏。微信的窗口自动化由 `wx4py` 完成，聊天历史由独立的外部 `wxdb` 命令提供。主仓库不包含、链接或分发微信数据库读取实现。

## 能力

- 微信群与 Discord 频道接入，统一使用 `/总结` 指令和定时任务。
- `/总结 [platform] [time] [图片]`：支持 `wx`、`微信`、`wechat`、`dc`、`discord`，大小写不敏感；平台省略时使用收到指令的平台。
- 文本总结、图片总结、图片生成，以及超长历史按页读取和分段处理。
- 按群聊或频道单独关闭图片总结，让指定房间只发送文字总结。
- 可选图片转述、视频转述和语音转写；语音可通过 FFmpeg 转为 MP3。
- 5xx 指数退避重试、429 串行重试队列、多 API Key 并发控制、失败原因与请求 trace 脱敏落盘。
- 微信长文本自动分段发送，或按配置改为发送 `.txt` 文件。
- 原生管理 GUI：平台、群组、定时任务、模型、媒体、日志、终端和运行状态都可直接管理；配置变更会被主程序热重载。

## 工作方式

```text
微信 wx4py / Discord Gateway
            |
          指令或定时任务
            |
外部 wxdb 历史读取 / Discord 历史读取
            |
可选图片、视频、语音转述
            |
LLM 文本总结 -> 可选图片提示词 -> 图片生成
            |
原平台发送文字、图片或长文本文件
```

微信使用 UI 自动化，因此微信必须保持登录，并且运行 GUI 的用户需要能操作当前桌面会话。历史读取依赖外部 `wxdb`；请只在你有权访问的本机账号和聊天记录上使用它。

## 安装

### 使用 Windows 安装包

从本仓库的 [Releases](https://github.com/fangbm/SummaryAgent4GroupChat/releases) 下载 Inno Setup 安装程序并安装。安装完成后从开始菜单或桌面快捷方式打开 `SummaryAgent4GroupChat`。

GUI 与主程序默认以普通权限运行，不会额外弹出命令行窗口。只有“安装微信运行环境”和“运行 wxdb init”两个维护操作会单独请求管理员权限；点击后会打开原生进度弹窗，实时显示后台输出、成功状态或完整的脱敏错误信息。

在 GUI 左侧点击“安装微信运行环境”，安装器会：

1. 检测 Python 3.11/3.12；未安装时通过 `winget` 安装 Python 3.12。
2. 创建安装目录内的 `.venv` 并安装 `wx4py`。
3. 优先使用 GUI 已配置的 wxdb 路径，其次查找 `PATH`；都找不到时从独立 wxdb Release 下载 Windows x64 运行时。
4. 写入 Python、sidecar、wxdb 和 wxdb 缓存目录到 `config\agent.toml`。
5. 尝试执行 `wxdb init`。微信未登录、未运行或权限不足时会显示警告，可在登录微信后点击“运行外部 wxdb init”重试。

主项目不附带 wxdb。如果独立 wxdb 项目尚未发布 Windows Release，请自行配置本地 `wxdb.exe` 的绝对路径或将其加入 `PATH`，再运行安装流程。

### 从源码构建

前置条件：Windows 10/11、Rust stable、Python 3.11 或 3.12、已登录的 Windows 微信，以及可用的外部 wxdb 命令。

```powershell
git clone https://github.com/fangbm/SummaryAgent4GroupChat.git
cd SummaryAgent4GroupChat

cd rust-agent
cargo build --release -p wechat-summary-app -p wechat-summary-control -p wechat-summary-gui
cd ..
dotnet build .\windows-ui\SummaryAgent4GroupChat.WinUI.sln -c Release -p:Platform=x64
```

启动 WinUI GUI：

```powershell
.\windows-ui\src\SummaryAgent4GroupChat.WinUI\bin\x64\Release\net10.0-windows10.0.19041.0\win-x64\SummaryAgent4GroupChat.exe --config .\rust-agent\config\agent.toml
```

构建 zip 与 Inno Setup 安装程序：

```powershell
.\scripts\package-windows-installer.ps1
```

生成物位于 `dist\`。若脚本找不到 Inno Setup，可先安装：

```powershell
winget install --id JRSoftware.InnoSetup -e --scope user --accept-package-agreements --accept-source-agreements
```

## 首次配置

打开 GUI 后，依次完成以下内容：

1. 在“接入平台”页选择 `wx` 或 `discord`。
2. 微信模式填写可搜索到的群显示名；Discord 模式填写频道 ID 与机器人 Token 环境变量名。
3. 在“模型与图片”页填写 LLM 的 API Key、Base URL 和模型名称。支持直接填值，也支持环境变量名。
4. 微信模式点击“安装微信运行环境”，并在微信登录后运行 `wxdb init`。
5. 在“监听与命令”页确认触发词、白名单和冷却时间；保存配置后启动主程序。

默认配置文件为 [`rust-agent/config/agent.toml`](rust-agent/config/agent.toml)。安装版使用安装目录下的 `config\agent.toml`。GUI 保存后主程序会自动重载配置，无需重启。

### 更新检查

GUI 启动后会后台检查一次；也可在左侧“更新与依赖”页手动检查。检查范围包括应用 Release、独立 wxdb Release 与当前 `.venv` 中所有可升级的 pip 依赖。检查只读取版本信息，不会自动下载安装或修改你的 Python、wxdb、ffmpeg 等环境。

微信模式首次启动还会检测 Python、`wx4py` 与外部 wxdb 是否可用；缺少时会显示原生确认弹窗，可直接启动“安装微信运行环境”。“运行 wxdb init”和输出目录等维护操作也位于“更新与依赖”页；维护任务在后台执行，进度和错误会显示在操作弹窗中。

### 最小 LLM 配置

以下为 OpenAI 兼容服务的最小环境变量示例：

```powershell
$env:LLM_API_KEY = "your-api-key"
$env:LLM_BASE_URL = "https://api.example.com/v1"
$env:LLM_MODEL = "your-model"
```

也可将值直接写入 `[llm]`，或填写 `api_keys` / `api_keys_env` 使用多个 Key 并发。模型页面的“LLM 请求体覆盖（JSON）”可写入服务商特定字段，例如 `reasoning_effort`、`thinking`、`tools` 或 `tool_choice`。

### wxdb 配置

推荐使用绝对路径，避免安装版依赖交互式 shell 的 `PATH`：

```toml
[wxdb]
executable = "D:\\tools\\wxdb\\wxdb.exe"
cache_dir = "D:\\SummaryAgentCache\\wxdb"
# 多微信账号时可指定唯一的 db_storage 目录。
# db_dir = "D:\\Temp\\xwechat_files\\wxid_xxx\\db_storage"
```

`wxdb init` 会刷新本地密钥缓存。缓存和密钥数据敏感，请放在受信任磁盘；缓存目录可在 GUI 的接入平台页修改。

### 按群聊/频道能力覆盖

全局图片生成保持开启时，可以让某些群只发送文字总结。GUI 的“接入平台”页提供每行一个的房间列表；保存后会写入以下配置。微信填写群显示名，Discord 填频道 ID：

```toml
[room_capabilities]
"只发文字的微信群" = { image_summary_enabled = false }
"123456789012345678" = { image_summary_enabled = false }
```

未列出的房间继承全局图片总结配置。该覆盖同时作用于手动指令和定时任务，且不会开启图片冷却。

## 指令与定时任务

| 指令 | 作用 |
| --- | --- |
| `/总结` | 总结当前平台、默认时间范围内的聊天。 |
| `/总结 24h` | 总结最近 24 小时。支持 `30m`、`2h`、`1d`、`48h`、`30d` 等时长。 |
| `/总结 dc 1d` | 在当前群中请求 Discord 平台最近一天的总结。 |
| `/总结 微信 2h 图片` | 总结微信最近两小时，并按图片开关生成或跳过配图。 |

`图片`、`image`、`img` 均可用。`[manual_summary].image_by_default = false` 时，只有包含图片参数才生成图片；设为 `true` 时含图片参数表示跳过生图。

定时总结由 `[scheduled_summary]` 控制，默认每天本地时间 22:00 汇总 24 小时。定时任务不受手动图片冷却影响。

## 媒体与图片

| 功能 | 配置段 | 说明 |
| --- | --- | --- |
| 图片生成 | `[image_gen]` | 在文本总结后生成群聊配图。 |
| 图片转述 | `[image_caption]` | 将图片内容插回对应消息位置，再交给文本总结模型。 |
| 视频转述 | `[video_caption]` | 将视频以 base64 发送到多模态模型进行转述。 |
| 语音转写 | `[voice_transcription]` | 可先用 FFmpeg 统一转为 MP3，再请求转写模型。 |

四类模型均可单独设置 Provider、Key、Base URL、模型名、重试次数、并发数与请求体覆盖 JSON。媒体请求失败不会阻止纯文本聊天记录继续总结。

## 运行与排障

GUI 的“运行信息”页包含主程序终端与日志尾部。默认日志路径为：

```text
runtime\rust-output\wechat-summary-app.log
```

常见处理方式：

- **没有收到指令**：确认平台、群/频道白名单、触发词和主程序状态；微信还需确认 UI 自动化能找到该群。
- **wxdb 找不到密钥或没有消息**：保持目标微信登录，使用与微信相同权限启动 GUI，然后执行“运行外部 wxdb init”；多账号时配置 `wxdb.db_dir`。
- **图片、语音或视频失败**：先检查各自模型的 Key、Base URL、模型名和网络可达性；完整脱敏错误会写入运行日志。
- **长总结发不完**：在 `[wx4py]` 中保留 `long_text_delivery = "chunks"`，或改为 `file`，让超长结果以 UTF-8 文本文件发送。

更多细节见 [部署说明](docs/deploy-guide.md)、[故障排查](docs/troubleshooting.md)、[隐私与合规](docs/privacy-and-compliance.md) 和 [Rust 设计文档](docs/rust-windows-wx4py-wxdb-dev-doc.md)。

## 隐私与安全

- 只有启用 `cloud_allowed` 后，内容才会发给云端模型。
- 为敏感群配置 `privacy.sensitive_rooms`，并根据你的数据处理要求启用脱敏。
- API Key 优先放入环境变量；GUI 会对显示的密钥样式内容进行脱敏，但不要将真实密钥提交到 Git。
- AI trace 用于故障排查，会记录已脱敏的请求与响应；仅在受信任环境启用，并定期清理 `runtime.ai_trace_dir`。

## 开发与质量检查

```powershell
cd rust-agent
cargo fmt --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
```

WinUI GUI 使用普通权限 manifest；本地可用 `dotnet build .\windows-ui\SummaryAgent4GroupChat.WinUI.sln -p:Platform=x64` 验证界面工程。

## 发布

更新 [`rust-agent/Cargo.toml`](rust-agent/Cargo.toml) 的 `[workspace.package].version` 并推送到 `main` 后，GitHub Actions 会自动构建 Windows zip 和 Inno Setup 安装程序，创建 `v<version>` Release，并生成 GitHub Release Notes。

```toml
[workspace.package]
version = "0.1.4"
```

发布工作流只关注版本变化；仅修改 README、GUI 或配置不会生成 Release。这样可以避免每次普通提交都制造安装包版本。

## 许可证与范围

本项目采用 [MIT License](rust-agent/Cargo.toml)。微信历史读取由独立外部提供器完成；使用者须遵守当地法律、平台规则和账号数据权限边界。
