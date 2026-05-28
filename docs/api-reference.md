# API Reference

## Modes

- `--mode single`: 旧 Python Windows 单机模式。当前 Rust 分支默认由 `wx4py` sidecar 监听消息并回发，HTTP/WebSocket 只作为可选运维接口。
- `--mode api`: 旧双端/外部控制模式。开放 WebSocket 信令与 HTTP 图片下载。

## WebSocket

默认路径：`/ws`。外部控制端连接 Windows Worker 时必须携带：

```http
Authorization: Bearer <WECHAT_PIPELINE_IPC_TOKEN>
```

所有信令统一包含：

```json
{
  "schema_version": "1.0",
  "msg_id": "uuid",
  "timestamp": "2026-05-23T00:00:00Z",
  "type": "TRIGGER_DETECTED",
  "payload": {},
  "reply_to": null
}
```

支持类型：

- `TRIGGER_DETECTED`: 控制端 -> Windows，触发摘要任务。
- `TASK_ACCEPTED`: Windows -> 控制端，任务已接收。
- `PROGRESS_UPDATE`: Windows -> 控制端，处理进度。
- `TASK_COMPLETED`: Windows -> 控制端，摘要完成，可带图片下载 URL。
- `TASK_FAILED`: Windows -> 控制端，任务失败及错误码。

## HTTP

- `GET /health`: 健康检查。
- `GET /metrics`: Prometheus 指标。
- `GET /images/{filename}?expires=...&sig=...`: 图片下载，URL 使用 HMAC 签名和过期时间。
