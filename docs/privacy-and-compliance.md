# Privacy And Compliance

- 默认不启用文本脱敏，以保留总结上下文；需要时请显式设置 `privacy.redact_enabled = true`，它会在发送给 LLM 前替换 `wxid_*`、手机号和邮箱。
- `privacy.max_chars_to_llm` 控制单次云端发送规模，超长记录会按完整消息边界分段。
- 设置 `privacy.cloud_allowed = false` 会阻止云端 LLM；`privacy.sensitive_rooms` 可将限制收窄到指定房间。
- API Key、IPC Token、下载签名密钥必须通过环境变量或本地配置注入，禁止提交真实密钥。
- 本工具仅用于本人微信账号数据的学习研究场景，使用前应获得群成员知情同意。
