use anyhow::{Context, Result};
use chrono::{DateTime, Local, TimeZone, Utc};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::cache::{CacheMode, DbCache};
use crate::config::{self, RuntimeConfig};
use crate::keyring;

#[derive(Debug, Clone)]
pub struct HistoryQuery {
    pub chat_name: String,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: usize,
    pub text_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryMessage {
    pub timestamp: i64,
    pub time: String,
    pub sender: String,
    pub content: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_contact_display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_group_nickname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryResult {
    pub chat: String,
    pub username: String,
    pub is_group: bool,
    pub count: usize,
    pub messages: Vec<HistoryMessage>,
    pub meta: HistoryMeta,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HistoryMeta {
    pub db_dir: Option<PathBuf>,
    pub candidates_scanned: usize,
    pub shards_scanned: usize,
    pub shards_hit: usize,
    pub unknown_shards: Vec<String>,
    pub cache_mode_per_shard: HashMap<String, CacheMode>,
    pub warnings: Vec<String>,
}

#[derive(Clone)]
struct Names {
    map: HashMap<String, String>,
    msg_db_keys: Vec<String>,
}

#[derive(Debug, Clone)]
struct MessageShard {
    rel_key: String,
    path: PathBuf,
    table: String,
    max_ts: i64,
    cache_mode: CacheMode,
}

pub fn query_history(query: HistoryQuery) -> Result<HistoryResult> {
    let config = RuntimeConfig::load();
    query_history_with_config(&config, query)
}

pub fn query_history_with_config(
    config: &RuntimeConfig,
    query: HistoryQuery,
) -> Result<HistoryResult> {
    if config.db_dirs.is_empty() {
        anyhow::bail!(
            "未找到 WeChat db_storage 目录；可设置 WXDB_DB_DIR 或运行 wxdb doctor 查看候选"
        );
    }

    let mut best: Option<HistoryResult> = None;
    let mut errors = Vec::new();
    let mut missing_key_store_errors = Vec::new();
    let mut global_warnings = Vec::new();

    for db_dir in &config.db_dirs {
        match query_history_in_store(config, db_dir, &query) {
            Ok(mut result) => {
                result.meta.candidates_scanned = config.db_dirs.len();
                result.meta.db_dir = Some(db_dir.clone());
                if result.count > best.as_ref().map(|current| current.count).unwrap_or(0) {
                    best = Some(result);
                } else if result.count == 0 && best.is_none() {
                    best = Some(result);
                }
            }
            Err(error) => {
                let error = format!("{}: {error:#}", db_dir.display());
                if is_missing_db_key_error(&error) {
                    missing_key_store_errors.push(error);
                } else {
                    errors.push(error);
                }
            }
        }
    }

    if let Some(mut result) = best {
        global_warnings.extend(errors);
        result.meta.warnings.extend(global_warnings);
        return Ok(result);
    }

    errors.extend(missing_key_store_errors);
    anyhow::bail!("所有 WeChat 数据库候选均查询失败: {}", errors.join(" | "))
}

fn is_missing_db_key_error(error: &str) -> bool {
    error.contains("没有可用数据库密钥")
}

fn query_history_in_store(
    config: &RuntimeConfig,
    db_dir: &Path,
    query: &HistoryQuery,
) -> Result<HistoryResult> {
    let keys = keyring::ensure_keys_for_db_dir(config, db_dir)?;
    if keys.is_empty() {
        anyhow::bail!("没有可用数据库密钥；请确认微信正在运行，必要时用管理员权限执行 wxdb init");
    }

    let mut cache = DbCache::new(
        db_dir.to_path_buf(),
        config.cache_dir_for(db_dir),
        config.mtime_file_for(db_dir),
        keys,
    )?;
    let names = load_names(&mut cache)?;
    let username = resolve_username(&query.chat_name, &names)
        .with_context(|| format!("找不到联系人或群聊: {}", query.chat_name))?;
    let display = names
        .map
        .get(&username)
        .cloned()
        .unwrap_or_else(|| query.chat_name.clone());
    let is_group = username.contains("@chatroom");
    let (shards, scanned, mut warnings) = find_msg_shards(&mut cache, &names, &username)?;
    let unknown_shards = unknown_message_shards(&cache, &names);
    if shards.is_empty() {
        return Ok(HistoryResult {
            chat: display,
            username,
            is_group,
            count: 0,
            messages: Vec::new(),
            meta: HistoryMeta {
                db_dir: Some(db_dir.to_path_buf()),
                candidates_scanned: 1,
                shards_scanned: scanned,
                shards_hit: 0,
                unknown_shards,
                cache_mode_per_shard: HashMap::new(),
                warnings,
            },
        });
    }

    let group_nicknames = if is_group {
        load_group_nicknames(&mut cache, &username).unwrap_or_default()
    } else {
        HashMap::new()
    };
    let names_map = names.map.clone();
    let mut all_messages = Vec::new();
    let mut shards_hit = 0usize;
    let mut cache_modes = HashMap::new();

    for shard in &shards {
        cache_modes.insert(shard.rel_key.clone(), shard.cache_mode);
        let rows = query_messages(
            &shard.path,
            &shard.table,
            &username,
            is_group,
            &names_map,
            &group_nicknames,
            query.since.map(|dt| dt.timestamp()),
            query.until.map(|dt| dt.timestamp()),
            query.text_only,
            query.limit,
        )?;
        if !rows.is_empty() {
            shards_hit += 1;
        }
        all_messages.extend(rows);
    }

    all_messages.sort_by_key(|message| std::cmp::Reverse(message.timestamp));
    all_messages.truncate(query.limit);
    all_messages.sort_by_key(|message| message.timestamp);
    let count = all_messages.len();
    warnings.extend(
        unknown_shards
            .iter()
            .map(|shard| format!("磁盘存在但没有密钥的消息分片: {shard}")),
    );

    Ok(HistoryResult {
        chat: display,
        username,
        is_group,
        count,
        messages: all_messages,
        meta: HistoryMeta {
            db_dir: Some(db_dir.to_path_buf()),
            candidates_scanned: 1,
            shards_scanned: scanned,
            shards_hit,
            unknown_shards,
            cache_mode_per_shard: cache_modes,
            warnings,
        },
    })
}

fn load_names(cache: &mut DbCache) -> Result<Names> {
    let mut map = HashMap::new();
    if let Some(contact_path) = cache.get("contact/contact.db")? {
        let conn = Connection::open(&contact_path).context("打开 contact.db 失败")?;
        if let Ok(mut stmt) =
            conn.prepare("SELECT username, nick_name, remark, verify_flag FROM contact")
        {
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1).unwrap_or_default(),
                    row.get::<_, String>(2).unwrap_or_default(),
                    row.get::<_, i64>(3).unwrap_or(0),
                ))
            })?;
            for row in rows.flatten() {
                let (username, nick, remark, verify_flag) = row;
                let display = if !remark.is_empty() {
                    remark
                } else if !nick.is_empty() {
                    nick
                } else {
                    username.clone()
                };
                let _ = verify_flag;
                map.insert(username, display);
            }
        };
    }

    if let Some(session_path) = cache.get("session/session.db")? {
        let conn = Connection::open(&session_path).context("打开 session.db 失败")?;
        if let Ok(mut stmt) = conn.prepare("SELECT username FROM SessionTable") {
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            for username in rows.flatten() {
                map.entry(username.clone()).or_insert(username);
            }
        };
    }

    let msg_db_keys = config::message_db_keys(cache.db_dir())
        .into_iter()
        .filter(|rel| cache.keys().contains_key(rel))
        .collect();

    Ok(Names { map, msg_db_keys })
}

fn resolve_username(chat_name: &str, names: &Names) -> Option<String> {
    if names.map.contains_key(chat_name)
        || chat_name.contains("@chatroom")
        || chat_name.starts_with("wxid_")
    {
        return Some(chat_name.to_string());
    }
    let low = chat_name.to_lowercase();
    let mut exact: Vec<&String> = names
        .map
        .iter()
        .filter(|(_, display)| display.to_lowercase() == low)
        .map(|(username, _)| username)
        .collect();
    exact.sort();
    if let Some(username) = exact.into_iter().next() {
        return Some(username.clone());
    }
    let mut candidates: Vec<(&String, &String)> = names
        .map
        .iter()
        .filter(|(_, display)| display.to_lowercase().contains(&low))
        .collect();
    candidates.sort_by_key(|(username, display)| (display.len(), username.as_str()));
    candidates
        .into_iter()
        .next()
        .map(|(username, _)| username.clone())
}

fn find_msg_shards(
    cache: &mut DbCache,
    names: &Names,
    username: &str,
) -> Result<(Vec<MessageShard>, usize, Vec<String>)> {
    let table = msg_table_name(username);
    let mut scanned = 0usize;
    let mut warnings = Vec::new();
    let mut shards = Vec::new();

    for rel_key in &names.msg_db_keys {
        let Some(resolve) = cache.get_with_mode(rel_key)? else {
            continue;
        };
        scanned += 1;
        if let Some(warning) = resolve.warning.clone() {
            warnings.push(warning);
        }
        let conn = Connection::open(&resolve.path)?;
        let exists = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                [&table],
                |row| row.get::<_, i64>(0),
            )
            .ok()
            .is_some();
        if !exists {
            continue;
        }
        let max_ts = conn
            .query_row(
                &format!("SELECT MAX(create_time) FROM [{}]", table),
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten()
            .unwrap_or(0);
        shards.push(MessageShard {
            rel_key: rel_key.clone(),
            path: resolve.path,
            table: table.clone(),
            max_ts,
            cache_mode: resolve.mode,
        });
    }
    shards.sort_by_key(|shard| std::cmp::Reverse(shard.max_ts));
    Ok((shards, scanned, warnings))
}

fn query_messages(
    db_path: &Path,
    table: &str,
    chat_username: &str,
    is_group: bool,
    names_map: &HashMap<String, String>,
    group_nicknames: &HashMap<String, String>,
    since: Option<i64>,
    until: Option<i64>,
    text_only: bool,
    limit: usize,
) -> Result<Vec<HistoryMessage>> {
    let conn = Connection::open(db_path)?;
    let id2u = load_id2u(&conn);
    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    if let Some(since) = since {
        clauses.push("create_time >= ?".to_string());
        params.push(Box::new(since));
    }
    if let Some(until) = until {
        clauses.push("create_time <= ?".to_string());
        params.push(Box::new(until));
    }
    if text_only {
        clauses.push("(local_type & 4294967295) = ?".to_string());
        params.push(Box::new(1i64));
    }
    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    let sql = format!(
        "SELECT local_id, local_type, create_time, real_sender_id,
                message_content, WCDB_CT_message_content
         FROM [{}] {} ORDER BY create_time DESC LIMIT ?",
        table, where_clause
    );
    params.push(Box::new(limit as i64));
    let params_ref: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|param| param.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_ref.as_slice(), |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            get_content_bytes(row, 4),
            row.get::<_, i64>(5).unwrap_or(0),
        ))
    })?;

    let mut messages = Vec::new();
    for row in rows.flatten() {
        let (local_id, local_type, timestamp, real_sender_id, content_bytes, ct) = row;
        let raw_content = decompress_message(&content_bytes, ct);
        let sender_username =
            sender_username(real_sender_id, &raw_content, is_group, chat_username, &id2u);
        let sender = sender_label(
            &sender_username,
            is_group,
            names_map,
            group_nicknames,
            real_sender_id,
            &raw_content,
            chat_username,
            &id2u,
        );
        let content = format_content(local_id, local_type, &raw_content, is_group);
        if content.trim().is_empty() {
            continue;
        }
        messages.push(HistoryMessage {
            timestamp,
            time: fmt_time(timestamp),
            sender,
            content,
            msg_type: fmt_type(local_type),
            sender_username: (!sender_username.is_empty()).then_some(sender_username.clone()),
            sender_contact_display: (!sender_username.is_empty()).then(|| {
                names_map
                    .get(&sender_username)
                    .cloned()
                    .unwrap_or(sender_username.clone())
            }),
            sender_group_nickname: group_nicknames.get(&sender_username).cloned(),
            local_id: Some(local_id),
        });
    }
    Ok(messages)
}

fn load_id2u(conn: &Connection) -> HashMap<i64, String> {
    let mut map = HashMap::new();
    if let Ok(mut stmt) = conn.prepare("SELECT rowid, user_name FROM Name2Id") {
        if let Ok(rows) = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        }) {
            for (id, username) in rows.flatten() {
                map.insert(id, username);
            }
        }
    }
    map
}

fn load_group_nicknames(
    cache: &mut DbCache,
    chat_username: &str,
) -> Result<HashMap<String, String>> {
    if !chat_username.contains("@chatroom") {
        return Ok(HashMap::new());
    }
    let Some(contact_path) = cache.get("contact/contact.db")? else {
        return Ok(HashMap::new());
    };
    let conn = Connection::open(contact_path)?;
    Ok(load_group_nickname_map_from_conn(&conn, chat_username))
}

fn load_group_nickname_map_from_conn(
    conn: &Connection,
    chat_username: &str,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(room_id) = [
        "SELECT id FROM chat_room WHERE username = ?",
        "SELECT id FROM chat_room WHERE chat_room_name = ?",
        "SELECT id FROM chat_room WHERE name = ?",
    ]
    .iter()
    .find_map(|sql| {
        conn.query_row(sql, [chat_username], |row| row.get::<_, i64>(0))
            .ok()
    }) else {
        return out;
    };

    if let Ok(mut stmt) = conn.prepare(
        "SELECT c.username, c.nick_name, c.remark
         FROM chatroom_member cm
         LEFT JOIN contact c ON c.id = cm.member_id
         WHERE cm.room_id = ?",
    ) {
        if let Ok(rows) = stmt.query_map([room_id], |row| {
            Ok((
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, String>(1).unwrap_or_default(),
                row.get::<_, String>(2).unwrap_or_default(),
            ))
        }) {
            for (username, nick, remark) in rows.flatten() {
                if username.is_empty() {
                    continue;
                }
                let display = if !remark.is_empty() { remark } else { nick };
                if !display.is_empty() {
                    out.insert(username, display);
                }
            }
        }
    }
    out
}

fn sender_username(
    real_sender_id: i64,
    content: &str,
    is_group: bool,
    chat_username: &str,
    id2u: &HashMap<i64, String>,
) -> String {
    let sender_uname = id2u.get(&real_sender_id).cloned().unwrap_or_default();
    if !is_group {
        if !sender_uname.is_empty() && sender_uname != chat_username {
            return sender_uname;
        }
        return String::new();
    }
    if !sender_uname.is_empty() && sender_uname != chat_username {
        return sender_uname;
    }
    content
        .split_once(":\n")
        .map(|(sender, _)| sender.to_string())
        .unwrap_or_default()
}

fn sender_label(
    sender_username: &str,
    is_group: bool,
    names: &HashMap<String, String>,
    group_nicknames: &HashMap<String, String>,
    real_sender_id: i64,
    content: &str,
    chat_username: &str,
    id2u: &HashMap<i64, String>,
) -> String {
    if is_group {
        if sender_username.is_empty() {
            return String::new();
        }
        return group_nicknames
            .get(sender_username)
            .filter(|value| !value.is_empty())
            .cloned()
            .or_else(|| names.get(sender_username).cloned())
            .unwrap_or_else(|| sender_username.to_string());
    }
    let sender_uname = id2u.get(&real_sender_id).cloned().unwrap_or_default();
    if !sender_uname.is_empty() && sender_uname != chat_username {
        return names.get(&sender_uname).cloned().unwrap_or(sender_uname);
    }
    if let Some((sender, _)) = content.split_once(":\n") {
        return sender.to_string();
    }
    String::new()
}

fn get_content_bytes(row: &rusqlite::Row<'_>, idx: usize) -> Vec<u8> {
    row.get::<_, Vec<u8>>(idx)
        .or_else(|_| row.get::<_, String>(idx).map(|value| value.into_bytes()))
        .unwrap_or_default()
}

fn decompress_message(data: &[u8], ct: i64) -> String {
    if ct == 4 && !data.is_empty() {
        if let Ok(dec) = zstd::decode_all(data) {
            return String::from_utf8_lossy(&dec).into_owned();
        }
    }
    String::from_utf8_lossy(data).into_owned()
}

fn format_content(local_id: i64, local_type: i64, content: &str, is_group: bool) -> String {
    let base = (local_type as u64 & 0xFFFF_FFFF) as i64;
    match base {
        3 => return format!("[图片] local_id={local_id}"),
        34 => return "[语音]".to_string(),
        43 => return "[视频]".to_string(),
        47 => return "[表情]".to_string(),
        50 => return "[通话]".to_string(),
        10000 => return "[系统消息]".to_string(),
        10002 => return "[撤回了一条消息]".to_string(),
        _ => {}
    }
    let text = if is_group {
        content
            .split_once(":\n")
            .map(|(_, content)| content)
            .unwrap_or(content)
    } else {
        content
    };
    text.to_string()
}

fn fmt_type(local_type: i64) -> String {
    match (local_type as u64 & 0xFFFF_FFFF) as i64 {
        1 => "text".to_string(),
        3 => "image".to_string(),
        34 => "voice".to_string(),
        43 => "video".to_string(),
        47 => "sticker".to_string(),
        49 => "link".to_string(),
        10000 | 10002 => "system".to_string(),
        other => other.to_string(),
    }
}

fn fmt_time(timestamp: i64) -> String {
    Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| timestamp.to_string())
}

fn msg_table_name(username: &str) -> String {
    format!("Msg_{:x}", md5::compute(username.as_bytes()))
}

fn unknown_message_shards(cache: &DbCache, names: &Names) -> Vec<String> {
    config::message_db_keys(cache.db_dir())
        .into_iter()
        .filter(|rel| !names.msg_db_keys.contains(rel))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_group_content_without_sender_prefix() {
        assert_eq!(
            format_content(1, 1, "wxid_abc:\nhello", true),
            "hello".to_string()
        );
    }

    #[test]
    fn md5_table_name_is_stable() {
        assert_eq!(
            msg_table_name("test").len(),
            "Msg_098f6bcd4621d373cade4e832627b4f6".len()
        );
    }

    #[test]
    fn recognizes_missing_db_key_errors() {
        assert!(is_missing_db_key_error(
            r"\\?\D:\Temp\xwechat_files\wxid_a\db_storage: 没有可用数据库密钥；请确认微信正在运行"
        ));
        assert!(!is_missing_db_key_error("打开 contact.db 失败"));
    }
}
