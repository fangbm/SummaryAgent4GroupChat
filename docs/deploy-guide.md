# Deploy Guide

## Windows Single-Host Worker

1. 安装 Python 3.11+、Windows 微信客户端、`wx-cli`。
   当前 Rust 分支使用 `wx4py` UI 自动化监听/回发，并用 `wx-cli` 解密读取历史。
2. 执行 `wx init`，确认 `wx sessions --json` 与 `wx export` 能正常工作。
3. 创建虚拟环境并安装依赖。生产监听和回发默认使用 `wx4py`：

```powershell
python -m venv C:\wechat-pipeline\venv
C:\wechat-pipeline\venv\Scripts\Activate.ps1
python -m pip install -e ".[windows]"
python -m pip install wx4py
```

4. 配置 `.env`、`rust-agent\config\agent.toml`：

- `[wx4py].groups`: 微信群显示名
- `[wx_cli].group_name_map`: 群名到 `wx-cli export` 可识别名称
- `privacy.sensitive_groups`: 禁止云端 LLM 的敏感群

5. 启动 Rust 单机服务：

```powershell
cd rust-agent
cargo run -p wechat-summary-app -- --config config\agent.toml
```

生产运行可用 NSSM 或计划任务托管上述命令。微信客户端必须保持登录，wx4py 需要可操作桌面会话和稳定窗口焦点。

## Optional API Mode

旧双端模式仍可启动 Windows HTTP/WebSocket API：

```powershell
python -m windows_worker.main --config config\worker.yaml --mode api
```

这个模式用于兼容旧的 Linux Bot 或外部控制端，不是默认部署方式。

## Replacement for wx-bot-cli

当前不再依赖 `wx-bot-cli`。Windows 端推荐：

- `wx4py`: 当前分支主方案。通过 UI 自动化监听群消息、发送文本和图片/文件，适合不使用 DLL 注入的 Windows 本机闭环。
- WCFerry / `wcferry`: legacy 兼容 provider，不再作为默认方案。
- `wxauto`: UI 自动化兜底。适合简单发送和人工辅助场景，监听稳定性依赖微信窗口状态，不作为生产默认。

旧 Linux Bot 代码仍保留，后续如果需要重新启用双端部署，可以通过 `--mode api` 对接。

## 网络检查

- 单机模式不需要 Linux 网络连通。
- API 模式下确认 `http://<windows-host>:8765/health` 可访问。
- Windows 防火墙放行 Worker 端口。
- WebSocket token 一致。
- 图片下载 URL 在过期前可由外部控制端访问。
