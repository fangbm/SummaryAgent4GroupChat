# Privacy And Compliance

- 默认 `privacy.mode=protected`，会在发送给 LLM 前脱敏 `wxid_*`、手机号和邮箱。
- `privacy.max_messages_to_llm` 与 `privacy.max_chars_to_llm` 控制云端发送规模。
- `privacy.sensitive_groups` 中的群会拒绝云端 LLM，除非 Provider 标记为本地模型。
- API Key、IPC Token、下载签名密钥必须通过环境变量或本地配置注入，禁止提交真实密钥。
- 本工具仅用于本人微信账号数据的学习研究场景，使用前应获得群成员知情同意。

