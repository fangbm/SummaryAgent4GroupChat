# Troubleshooting

## `WXDB_DECRYPT_FAILED`

微信可能更新、未登录或数据库密钥未刷新。重新登录微信，并在安装目录运行 `bin\\wxdb.exe doctor` 查看诊断信息。

## `WXDB_NO_HISTORY`

指定时间段没有文本消息，检查触发时间范围和群 ID 到群名映射。

## `PRIVACY_BLOCKED`

当前群或全局策略禁止云端 LLM。切换到 Ollama/LM Studio，或从敏感群列表移除该群。

## 图片无法下载

检查 Windows Worker 的 `file_transfer.public_base_url` 是否是 Linux 可访问地址，并确认 URL 未过期。

## WebSocket 连接被拒绝

确认 Linux 与 Windows 的 `WECHAT_PIPELINE_IPC_TOKEN` 一致。
