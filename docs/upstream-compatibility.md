# Upstream Compatibility Notes

Validated on 2026-05-24:

## wx4py

- Documentation: `https://wx4py.biglongxia.com/`
- PyPI package inspected locally: `wx4py==0.2.1`
- Relevant Python APIs inspected in installed package:
  - `WeChatClient(auto_connect=False)`
  - `WeChatClient.connect()`
  - `WeChatClient.process_groups(groups, handlers, ignore_client_sent=True, block=False)`
  - `CallbackHandler(callback, auto_reply=False)`
  - `MessageEvent.group`, `MessageEvent.content`, `MessageEvent.timestamp`
  - `client.chat_window.send_to(group, text, target_type="group")`
  - `client.chat_window.send_file_to(group, image_path, target_type="group")`

Conclusion: this branch uses `wx4py` for Windows-side UI automation:
listening, text sends, and image/file sends. It is paired with the built-in
`wxdb` reader for decrypted history because wx4py's UIA history reader cannot
reliably expose full sender metadata.

## miloira/wxhook

- Repository: `https://github.com/miloira/wxhook`
- Local inspected revision: `0e6bde7`
- PyPI package: `wxhook`
- PyPI latest visible to this environment: `0.0.10`
- README compatibility note: the open-source package points to WeChat 3.9.5.81.
- Relevant Python APIs inspected in `wxhook/core.py`:
  - `Bot.handle(events.TEXT_MESSAGE / ALL_MESSAGE)`
  - `Bot.run()`
  - `Bot.check_login()`
  - `Bot.send_text(wxid, msg)`
  - `Bot.send_image(wxid, image_path)`
  - `Bot.get_contacts()`
  - `Bot.exec_sql(db_handle, sql)`

Conclusion: `wxhook` remains documented as the previous branch's injected API
approach, but this branch does not use it as the default runtime path.

## lich0821/WeChatFerry / wcferry

- Repository: `https://github.com/lich0821/WeChatFerry`
- Local inspected revision: `0f5c60a`
- PyPI package: `wcferry`
- PyPI latest visible to this environment: `39.5.2.0`
- Relevant Python APIs inspected in `clients/python/wcferry/client.py`:
  - `Wcf.enable_receiving_msg()`
  - `Wcf.get_msg()`
  - `Wcf.send_text(msg, receiver, aters="")`
  - `Wcf.send_image(path, receiver)`
  - `Wcf.get_contacts()`
  - `Wcf.query_sql(db, sql)`

Conclusion: WCFerry is kept as a legacy-compatible provider only. This branch's
default Windows provider is `wx4py` + built-in `wxdb`.

## cluic/wxauto

- Repository: `https://github.com/cluic/wxauto`
- Local inspected revision: `05e5a52`
- Relevant docs: `docs/class/WeChat.md`
- Relevant APIs documented: `AddListenChat`, `GetNextNewMessage`, `SendMsg`, `SendFiles`.

Conclusion: `wxauto` is useful as a Windows UI automation fallback, but production
listening is more fragile because it depends on the desktop WeChat UI state.

## jackwener/wx-cli

- Repository: `https://github.com/jackwener/wx-cli`
- Tested revision: `08af894`
- Binary name: `wx`
- Relevant command: `wx export <chat> --since "YYYY-MM-DD HH:MM:SS" --until "YYYY-MM-DD HH:MM:SS" --format json -o <file> -n <limit>`
- JSON messages include fields such as `timestamp`, `time`, `sender`, `sender_username`, `content`, and `type`.
- This is retained only as historical compatibility context. The default branch now uses the built-in `wxdb` reader for decrypted history while `wx4py` handles live UI automation.

## AirboZH/wx-bot-cli

- Repository: `https://github.com/AirboZH/wx-bot-cli`
- Tested revision: `69f97f8`
- npm package: `wx-bot-cli`
- Binary name: `wxbot`
- Commands exposed by README/source: `login`, `logout`, `send <text>`, `list`, `status`, hidden `_daemon`.

Compatibility gap:

- The upstream `wxbot` CLI sends text to the current active user, not to an explicit group ID.
- It does not expose `watch --json`.
- It does not expose `send-image`.
- Its IPC layer uses Unix socket paths, so Windows tests fail with `listen EACCES` for `.sock` paths.

Conclusion: `AirboZH/wx-bot-cli` can inform a future Linux/macOS bot adapter, but it is not yet a drop-in implementation for this pipeline's group-monitoring and image-return requirements.
