# WeChat-AI-Pipeline

Windows 单机版微信智能群聊监控系统。Windows 端直接监听微信群触发词、读取本机微信记录、统计发言、调用 LLM 摘要、生成图片，并回发到群聊。

当前仓库实现的是“生产增强版”工程骨架：Windows 单机主流程、协议、配置、SQLite 状态、隐私保护、重试降级、健康检查、指标、fake 依赖测试和部署文档都已经纳入。旧的 Linux Bot / WebSocket IPC 模式保留为兼容路径，但不再是默认方案。

## 快速开始

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
pytest
```

## Rust Windows-only Agent

新的 Rust 单机方案位于 `rust-agent/`，与现有 Python 项目隔离。本分支使用 `wx4py` 做 Windows 微信 UI 自动化监听和图片/文本发送；历史消息由用户单独安装和配置的外部历史提供器读取。主项目不包含、链接或分发数据库解密实现；Linux / `wx-bot-cli` 链路在该方案中废弃。

```powershell
cd rust-agent
cargo test --workspace
cargo run -p wechat-summary-app -- --config config\agent.toml
```

完整设计见 `docs/rust-windows-wx4py-wxdb-dev-doc.md`。

Windows 真实微信监听与回发默认使用 `wx4py`：

```powershell
python -m pip install -e ".[windows]"
python -m pip install wx4py
cd rust-agent
cargo run -p wechat-summary-app -- --config config\agent.toml
```

注意：`wx4py` 是 UI 自动化方案，需要 Windows 微信保持登录，并且 `rust-agent/config/agent.toml` 中 `[wx4py].groups` 必须填写可搜索到的微信群显示名。历史读取需要单独安装兼容的外部提供器，并将其路径配置到 `[wxdb].executable`。

## 目录

```text
src/pipeline_core/      # 共享协议、配置、鉴权、存储、隐私、重试、日志
src/linux_bot/          # 旧双端模式：Linux 监听与回发服务
src/windows_worker/     # Windows 单机监听、任务处理、回发、API Server、AI Provider
config/                 # 示例配置
docs/                   # 部署、API、隐私、故障排查
scripts/                # 服务安装/启动辅助脚本
tests/                  # 单元和集成测试
```

## 本地开发

1. 复制 `.env.example` 并填入本地密钥。
2. 按需修改 `config/worker.yaml`，重点设置 `wechat.provider`、群白名单、LLM 和图片模型。
3. Windows 单机启动：

```powershell
python -m windows_worker.main --config config\worker.yaml --mode single
```

4. 旧 IPC API 模式：

```powershell
python -m windows_worker.main --config config\worker.yaml --mode api
```

5. 旧 Linux Bot 模式：

```bash
python -m linux_bot.main --config config/bot.yaml
```

真实微信依赖通过 adapter 封装：`wx4py` 负责 Windows 本机 UI 自动化监听和回发，外部历史提供器负责返回 JSON 历史记录。`wxhook` / `wcferry` 在本分支中不作为默认链路。

## 质量门禁

```powershell
ruff check .
mypy src
pytest --cov
```
