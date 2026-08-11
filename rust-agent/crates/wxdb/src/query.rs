use anyhow::{Context, Result};
use chrono::{DateTime, Local, TimeZone, Utc};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration as StdDuration, Instant};

use crate::cache::{CacheMode, DbCache};
use crate::config::{self, RuntimeConfig};
use crate::keyring;
use crate::media;

#[derive(Debug, Clone)]
pub struct HistoryQuery {
    pub chat_name: String,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub before_local_id: Option<i64>,
    pub limit: usize,
    pub text_only: bool,
    pub msg_types: Vec<String>,
    pub media_decode_limit: Option<usize>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_candidates: Vec<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decoded_media_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_decoder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_decode_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryResult {
    pub chat: String,
    pub username: String,
    pub is_group: bool,
    pub count: usize,
    pub messages: Vec<HistoryMessage>,
    pub meta: HistoryMeta,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    let mut successful_stores = 0usize;
    let mut errors = Vec::new();
    let mut missing_key_store_errors = Vec::new();
    let mut global_warnings = Vec::new();
    let query_started = Instant::now();

    tracing::debug!(
        chat_name = %query.chat_name,
        since = ?query.since,
        until = ?query.until,
        limit = query.limit,
        text_only = query.text_only,
        msg_types = ?query.msg_types,
        media_decode_limit = ?query.media_decode_limit,
        db_dirs = config.db_dirs.len(),
        "wxdb query started"
    );

    for db_dir in &config.db_dirs {
        let store_started = Instant::now();
        tracing::debug!(
            db_dir = %db_dir.display(),
            chat_name = %query.chat_name,
            "wxdb store query started"
        );
        let store_result = match query_history_in_store(config, db_dir, &query) {
            Err(error) if should_retry_store_query_error(&error) => {
                let first_error = format!("{error:#}");
                tracing::warn!(
                    db_dir = %db_dir.display(),
                    chat_name = %query.chat_name,
                    error = %first_error,
                    "wxdb store query failed with a transient cache/decrypt error; retrying once"
                );
                thread::sleep(StdDuration::from_millis(800));
                query_history_in_store(config, db_dir, &query)
                    .with_context(|| format!("重试后仍无法读取微信数据库；首次错误: {first_error}"))
            }
            other => other,
        };

        match store_result {
            Ok(mut result) => {
                successful_stores += 1;
                result.meta.candidates_scanned = config.db_dirs.len();
                result.meta.db_dir = Some(db_dir.clone());
                tracing::debug!(
                    db_dir = %db_dir.display(),
                    chat_name = %query.chat_name,
                    count = result.count,
                    shards_scanned = result.meta.shards_scanned,
                    shards_hit = result.meta.shards_hit,
                    warnings = result.meta.warnings.len(),
                    elapsed_ms = store_started.elapsed().as_millis(),
                    "wxdb store query completed"
                );
                if best
                    .as_ref()
                    .is_none_or(|current| result.count > current.count)
                {
                    best = Some(result);
                }
            }
            Err(error) => {
                let error = format!("{}: {error:#}", db_dir.display());
                if is_missing_db_key_error(&error) {
                    tracing::debug!(
                        chat_name = %query.chat_name,
                        elapsed_ms = store_started.elapsed().as_millis(),
                        "wxdb store query skipped because database key is unavailable"
                    );
                    missing_key_store_errors.push(error);
                } else {
                    tracing::warn!(
                        db_dir = %db_dir.display(),
                        chat_name = %query.chat_name,
                        error = %error,
                        elapsed_ms = store_started.elapsed().as_millis(),
                        "wxdb store query failed"
                    );
                    errors.push(error);
                }
            }
        }
    }

    if !config.explicit_db_dir && successful_stores > 1 {
        anyhow::bail!(
            "微信数据库查询存在多个可读账号目录，无法确认群聊所属账号；请在 Agent 配置的 [wxdb].db_dir 中显式选择账号目录"
        );
    }

    if let Some(mut result) = best {
        global_warnings.extend(errors);
        result.meta.warnings.extend(global_warnings);
        tracing::debug!(
            chat_name = %query.chat_name,
            count = result.count,
            db_dir = ?result.meta.db_dir,
            elapsed_ms = query_started.elapsed().as_millis(),
            "wxdb query completed"
        );
        return Ok(result);
    }

    let failure_message = format_store_query_failure(&errors, &missing_key_store_errors);
    tracing::warn!(
        chat_name = %query.chat_name,
        elapsed_ms = query_started.elapsed().as_millis(),
        errors = errors.len() + missing_key_store_errors.len(),
        "wxdb query failed for all stores"
    );
    anyhow::bail!("{failure_message}")
}

fn is_missing_db_key_error(error: &str) -> bool {
    error.contains("没有可用数据库密钥") || error.contains("消息分片缺少数据库密钥")
}

fn should_retry_store_query_error(error: &anyhow::Error) -> bool {
    let error = format!("{error:#}");
    if error.contains("wxdb 缓存磁盘空间不足") || error.contains("磁盘空间不足") {
        return false;
    }
    error.contains("源数据库在解密期间发生变化")
        || error.contains("微信数据库持续写入")
        || error.contains("file is not a database")
        || error.contains("File opened that is not a database file")
        || error.contains("解密结果不是可读 SQLite 数据库")
}

fn format_store_query_failure(errors: &[String], missing_key_errors: &[String]) -> String {
    if let Some(primary) = errors.first() {
        let mut message = format!("微信数据库查询失败。主要错误: {primary}");
        if errors.len() > 1 {
            message.push_str(&format!("；另有 {} 个候选目录查询失败", errors.len() - 1));
        }
        if !missing_key_errors.is_empty() {
            message.push_str(&format!(
                "；另有 {} 个候选账号目录缺少数据库密钥，已作为次要诊断处理",
                missing_key_errors.len()
            ));
        }
        return message;
    }

    if missing_key_errors.is_empty() {
        return "微信数据库查询失败，但未返回具体错误".to_string();
    }

    format!(
        "未找到任何带可用数据库密钥的微信账号目录（共 {} 个候选）；请确认目标微信账号正在运行，然后执行 wxdb init",
        missing_key_errors.len()
    )
}

fn query_history_in_store(
    config: &RuntimeConfig,
    db_dir: &Path,
    query: &HistoryQuery,
) -> Result<HistoryResult> {
    let store_started = Instant::now();
    tracing::debug!(
        db_dir = %db_dir.display(),
        chat_name = %query.chat_name,
        "wxdb store initialization started"
    );
    let keys = keyring::ensure_keys_for_db_dir(config, db_dir)?;
    if keys.is_empty() {
        anyhow::bail!("没有可用数据库密钥；请确认微信正在运行，必要时用管理员权限执行 wxdb init");
    }
    tracing::debug!(
        db_dir = %db_dir.display(),
        key_count = keys.len(),
        elapsed_ms = store_started.elapsed().as_millis(),
        "wxdb store keys loaded"
    );

    let cache_started = Instant::now();
    let mut cache = DbCache::new(
        db_dir.to_path_buf(),
        config.cache_dir_for(db_dir),
        config.mtime_file_for(db_dir),
        keys,
    )?;
    tracing::debug!(
        db_dir = %db_dir.display(),
        elapsed_ms = cache_started.elapsed().as_millis(),
        "wxdb cache opened"
    );
    let names_started = Instant::now();
    let names = load_names(&mut cache)?;
    tracing::debug!(
        db_dir = %db_dir.display(),
        contacts = names.map.len(),
        known_message_shards = names.msg_db_keys.len(),
        elapsed_ms = names_started.elapsed().as_millis(),
        "wxdb names loaded"
    );
    let username = resolve_username(&query.chat_name, &names)?
        .ok_or_else(|| anyhow::anyhow!("找不到联系人或群聊: {}", query.chat_name))?;
    let display = names
        .map
        .get(&username)
        .cloned()
        .unwrap_or_else(|| query.chat_name.clone());
    let is_group = username.contains("@chatroom");
    let shard_started = Instant::now();
    let (shards, scanned, warnings) = find_msg_shards(&mut cache, &names, &username)?;
    let unknown_shards = unknown_message_shards(&cache, &names);
    tracing::debug!(
        db_dir = %db_dir.display(),
        chat_name = %query.chat_name,
        username = %username,
        is_group,
        shards_scanned = scanned,
        shards_matched = shards.len(),
        unknown_shards = unknown_shards.len(),
        elapsed_ms = shard_started.elapsed().as_millis(),
        "wxdb message shards resolved"
    );
    if !unknown_shards.is_empty() {
        let mut details = unknown_shards.join(", ");
        if details.chars().count() > 900 {
            details = details.chars().take(900).chain("...".chars()).collect();
        }
        anyhow::bail!(
            "微信数据库读取不完整：以下消息分片缺少数据库密钥，无法确认历史是否完整: {details}"
        );
    }
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

    let nick_started = Instant::now();
    let group_nicknames = if is_group {
        load_group_nicknames(&mut cache, &username).unwrap_or_default()
    } else {
        HashMap::new()
    };
    tracing::debug!(
        db_dir = %db_dir.display(),
        chat_name = %query.chat_name,
        group_nicknames = group_nicknames.len(),
        elapsed_ms = nick_started.elapsed().as_millis(),
        "wxdb group nicknames loaded"
    );
    let names_map = names.map.clone();
    let media_cache_dir = config.cache_dir_for(db_dir).join("media");
    let mut all_messages = Vec::new();
    let mut shards_hit = 0usize;
    let mut cache_modes = HashMap::new();
    let mut media_decode_remaining = query.media_decode_limit;
    let mut image_resolver = ImageResolveContext::default();

    for shard in &shards {
        cache_modes.insert(shard.rel_key.clone(), shard.cache_mode);
        let before_count = all_messages.len();
        let rows = query_messages(
            &shard.rel_key,
            &shard.path,
            &shard.table,
            &username,
            is_group,
            &names_map,
            &group_nicknames,
            db_dir.parent(),
            &media_cache_dir,
            query.since.map(|dt| dt.timestamp()),
            query.until.map(|dt| dt.timestamp()),
            query.before_local_id,
            query.text_only,
            &query.msg_types,
            query.limit,
            &mut media_decode_remaining,
            &mut image_resolver,
        )?;
        if !rows.is_empty() {
            shards_hit += 1;
        }
        all_messages.extend(rows);
        tracing::debug!(
            db_dir = %db_dir.display(),
            chat_name = %query.chat_name,
            shard = %shard.rel_key,
            accumulated_messages = all_messages.len(),
            added_messages = all_messages.len().saturating_sub(before_count),
            media_decode_remaining = ?media_decode_remaining,
            "wxdb shard accumulated"
        );
    }

    let mut seen_local_ids = HashSet::new();
    all_messages.retain(|message| {
        message
            .local_id
            .map(|local_id| seen_local_ids.insert(local_id))
            .unwrap_or(true)
    });
    all_messages.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| right.local_id.cmp(&left.local_id))
    });
    all_messages.truncate(query.limit);
    all_messages.sort_by_key(|message| message.timestamp);
    let count = all_messages.len();
    tracing::debug!(
        db_dir = %db_dir.display(),
        chat_name = %query.chat_name,
        count,
        shards_hit,
        elapsed_ms = store_started.elapsed().as_millis(),
        "wxdb store result prepared"
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

fn resolve_username(chat_name: &str, names: &Names) -> Result<Option<String>> {
    if names.map.contains_key(chat_name)
        || chat_name.contains("@chatroom")
        || chat_name.starts_with("wxid_")
    {
        return Ok(Some(chat_name.to_string()));
    }
    let low = chat_name.to_lowercase();
    let mut exact: Vec<&String> = names
        .map
        .iter()
        .filter(|(_, display)| display.to_lowercase() == low)
        .map(|(username, _)| username)
        .collect();
    exact.sort();
    if exact.len() > 1 {
        anyhow::bail!(
            "微信账号目录中群聊名称 {:?} 存在多个精确匹配，无法安全选择；请改用 wxid/@chatroom 标识或在 [wxdb].db_dir 中选择正确账号",
            chat_name
        );
    }
    if let Some(username) = exact.into_iter().next() {
        return Ok(Some(username.clone()));
    }
    let mut candidates: Vec<(&String, &String)> = names
        .map
        .iter()
        .filter(|(_, display)| display.to_lowercase().contains(&low))
        .collect();
    candidates.sort_by_key(|(username, display)| (display.len(), username.as_str()));
    if candidates.len() > 1 {
        anyhow::bail!(
            "微信账号目录中群聊名称 {:?} 存在多个模糊匹配，无法安全选择；请改用完整群名或 wxid/@chatroom 标识",
            chat_name
        );
    }
    Ok(candidates
        .into_iter()
        .next()
        .map(|(username, _)| username.clone()))
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

#[allow(clippy::too_many_arguments)]
fn query_messages(
    shard_rel_key: &str,
    db_path: &Path,
    table: &str,
    chat_username: &str,
    is_group: bool,
    names_map: &HashMap<String, String>,
    group_nicknames: &HashMap<String, String>,
    account_root: Option<&Path>,
    media_cache_dir: &Path,
    since: Option<i64>,
    until: Option<i64>,
    before_local_id: Option<i64>,
    text_only: bool,
    msg_types: &[String],
    limit: usize,
    media_decode_remaining: &mut Option<usize>,
    image_resolver: &mut ImageResolveContext,
) -> Result<Vec<HistoryMessage>> {
    let started = Instant::now();
    let budget_before = *media_decode_remaining;
    tracing::debug!(
        shard = %shard_rel_key,
        db_path = %db_path.display(),
        table,
        since = ?since,
        until = ?until,
        text_only,
        msg_types = ?msg_types,
        limit,
        media_decode_remaining = ?media_decode_remaining,
        "wxdb shard query started"
    );
    let conn = Connection::open(db_path)?;
    let id2u = load_id2u(&conn);
    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    if let Some(since) = since {
        clauses.push("create_time >= ?".to_string());
        params.push(Box::new(since));
    }
    if let (Some(until), Some(before_local_id)) = (until, before_local_id) {
        clauses.push("(create_time < ? OR (create_time = ? AND local_id < ?))".to_string());
        params.push(Box::new(until));
        params.push(Box::new(until));
        params.push(Box::new(before_local_id));
    } else if let Some(until) = until {
        clauses.push("create_time <= ?".to_string());
        params.push(Box::new(until));
    }
    let msg_type_values = msg_type_filter_values(text_only, msg_types);
    if !msg_type_values.is_empty() {
        let placeholders = std::iter::repeat_n("?", msg_type_values.len())
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!("(local_type & 4294967295) IN ({placeholders})"));
        for value in msg_type_values {
            params.push(Box::new(value));
        }
    }
    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    let sql = format!(
        "SELECT local_id, local_type, create_time, real_sender_id,
                message_content, WCDB_CT_message_content, packed_info_data
         FROM [{}] {} ORDER BY create_time DESC, local_id DESC LIMIT ?",
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
            get_content_bytes(row, 6),
        ))
    })?;

    let mut messages = Vec::new();
    let mut stats = QueryMessageStats::default();
    for row in rows {
        let Ok(row) = row else {
            stats.row_errors += 1;
            continue;
        };
        stats.rows_seen += 1;
        let (local_id, local_type, timestamp, real_sender_id, content_bytes, ct, packed_info_data) =
            row;
        let raw_content = decompress_message(&content_bytes, ct);
        let base_type = base_msg_type(local_type);
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
            stats.empty_content += 1;
            continue;
        }
        let media = match base_type {
            3 => {
                stats.image_messages += 1;
                if consume_media_decode_budget(media_decode_remaining) {
                    stats.media_decode_attempts += 1;
                    let media = resolve_image_media(
                        image_resolver,
                        account_root,
                        media_cache_dir,
                        chat_username,
                        timestamp,
                        &raw_content,
                    );
                    record_media_decode_stats(&mut stats, &media);
                    media
                } else {
                    stats.media_decode_skipped_budget += 1;
                    None
                }
            }
            34 => {
                stats.voice_messages += 1;
                if consume_media_decode_budget(media_decode_remaining) {
                    stats.media_decode_attempts += 1;
                    let media = resolve_voice_media(
                        image_resolver,
                        account_root,
                        media_cache_dir,
                        chat_username,
                        timestamp,
                        &raw_content,
                    );
                    record_media_decode_stats(&mut stats, &media);
                    media
                } else {
                    stats.media_decode_skipped_budget += 1;
                    None
                }
            }
            43 => {
                stats.video_messages += 1;
                if consume_media_decode_budget(media_decode_remaining) {
                    stats.media_decode_attempts += 1;
                    let media = resolve_video_media(
                        image_resolver,
                        account_root,
                        media_cache_dir,
                        chat_username,
                        timestamp,
                        &raw_content,
                        &packed_info_data,
                    );
                    record_media_decode_stats(&mut stats, &media);
                    media
                } else {
                    stats.media_decode_skipped_budget += 1;
                    None
                }
            }
            _ => None,
        };
        stats.messages_kept += 1;
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
            image_md5: (base_type == 3)
                .then(|| xml_attr(&raw_content, "md5"))
                .flatten(),
            media_path: media.as_ref().and_then(|media| media.media_path.clone()),
            thumbnail_path: media
                .as_ref()
                .and_then(|media| media.thumbnail_path.clone()),
            media_candidates: media
                .as_ref()
                .map(|media| media.candidates.clone())
                .unwrap_or_default(),
            decoded_media_path: media.as_ref().and_then(|media| media.decoded_path.clone()),
            media_decoder: media.as_ref().and_then(|media| media.decoder.clone()),
            media_decode_error: media.as_ref().and_then(|media| media.decode_error.clone()),
        });
    }
    tracing::debug!(
        shard = %shard_rel_key,
        db_path = %db_path.display(),
        table,
        rows_seen = stats.rows_seen,
        row_errors = stats.row_errors,
        empty_content = stats.empty_content,
        messages = stats.messages_kept,
        image_messages = stats.image_messages,
        voice_messages = stats.voice_messages,
        video_messages = stats.video_messages,
        media_decode_attempts = stats.media_decode_attempts,
        media_decode_success = stats.media_decode_success,
        media_decode_errors = stats.media_decode_errors,
        media_decode_unresolved = stats.media_decode_unresolved,
        media_decode_no_candidates = stats.media_decode_no_candidates,
        media_decode_skipped_budget = stats.media_decode_skipped_budget,
        media_decode_budget_before = ?budget_before,
        media_decode_budget_after = ?media_decode_remaining,
        elapsed_ms = started.elapsed().as_millis(),
        "wxdb shard query completed"
    );
    Ok(messages)
}

#[derive(Debug, Default)]
struct QueryMessageStats {
    rows_seen: usize,
    row_errors: usize,
    empty_content: usize,
    messages_kept: usize,
    image_messages: usize,
    voice_messages: usize,
    video_messages: usize,
    media_decode_attempts: usize,
    media_decode_success: usize,
    media_decode_errors: usize,
    media_decode_unresolved: usize,
    media_decode_no_candidates: usize,
    media_decode_skipped_budget: usize,
}

fn record_media_decode_stats(stats: &mut QueryMessageStats, media: &Option<ImageMedia>) {
    match media {
        Some(media) if media.decoded_path.is_some() => stats.media_decode_success += 1,
        Some(media) if media.decode_error.is_some() => stats.media_decode_errors += 1,
        Some(_) => stats.media_decode_unresolved += 1,
        None => stats.media_decode_no_candidates += 1,
    }
}

fn consume_media_decode_budget(media_decode_remaining: &mut Option<usize>) -> bool {
    match media_decode_remaining {
        Some(0) => false,
        Some(remaining) => {
            *remaining -= 1;
            true
        }
        None => true,
    }
}

#[derive(Debug, Clone, Default)]
struct ImageMedia {
    media_path: Option<PathBuf>,
    thumbnail_path: Option<PathBuf>,
    candidates: Vec<PathBuf>,
    decoded_path: Option<PathBuf>,
    decoder: Option<String>,
    decode_error: Option<String>,
}

#[derive(Debug, Default)]
struct ImageResolveContext {
    dir_cache: HashMap<PathBuf, Vec<MediaCandidateEntry>>,
    recursive_dir_cache: HashMap<(PathBuf, usize), Vec<MediaCandidateEntry>>,
}

#[derive(Debug, Clone)]
struct MediaCandidateEntry {
    path: PathBuf,
    modified_secs: i64,
}

impl ImageResolveContext {
    fn collect_nearby_files(&mut self, dir: &Path, timestamp: i64, out: &mut Vec<PathBuf>) {
        let entries = self
            .dir_cache
            .entry(dir.to_path_buf())
            .or_insert_with(|| read_media_candidate_entries(dir));
        for entry in entries {
            let delta = (entry.modified_secs - timestamp).abs();
            if delta <= 300 {
                out.push(entry.path.clone());
            }
        }
    }

    fn collect_files_matching_keys(&mut self, dir: &Path, keys: &[String], out: &mut Vec<PathBuf>) {
        if keys.is_empty() {
            return;
        }
        let keys = keys
            .iter()
            .map(|key| key.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let entries = self
            .dir_cache
            .entry(dir.to_path_buf())
            .or_insert_with(|| read_media_candidate_entries(dir));
        for entry in entries {
            let file_name = entry
                .path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if keys.iter().any(|key| file_name.starts_with(key)) {
                out.push(entry.path.clone());
            }
        }
    }

    fn collect_nearby_files_recursive(
        &mut self,
        dir: &Path,
        max_depth: usize,
        timestamp: i64,
        out: &mut Vec<PathBuf>,
    ) {
        let entries = self
            .recursive_dir_cache
            .entry((dir.to_path_buf(), max_depth))
            .or_insert_with(|| read_media_candidate_entries_recursive(dir, max_depth));
        for entry in entries {
            let delta = (entry.modified_secs - timestamp).abs();
            if delta <= 300 {
                out.push(entry.path.clone());
            }
        }
    }

    fn collect_files_matching_keys_recursive(
        &mut self,
        dir: &Path,
        max_depth: usize,
        keys: &[String],
        out: &mut Vec<PathBuf>,
    ) {
        if keys.is_empty() {
            return;
        }
        let keys = keys
            .iter()
            .map(|key| key.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let entries = self
            .recursive_dir_cache
            .entry((dir.to_path_buf(), max_depth))
            .or_insert_with(|| read_media_candidate_entries_recursive(dir, max_depth));
        for entry in entries {
            let file_name = entry
                .path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if keys.iter().any(|key| file_name.starts_with(key)) {
                out.push(entry.path.clone());
            }
        }
    }
}

fn resolve_image_media(
    resolver: &mut ImageResolveContext,
    account_root: Option<&Path>,
    media_cache_dir: &Path,
    chat_username: &str,
    timestamp: i64,
    raw_content: &str,
) -> Option<ImageMedia> {
    let account_root = account_root?;
    let chat_hash = format!("{:x}", md5::compute(chat_username.as_bytes()));
    let month = Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|dt| dt.format("%Y-%m").to_string())?;
    let mut candidates = Vec::new();
    let attach_dir = account_root
        .join("msg")
        .join("attach")
        .join(&chat_hash)
        .join(&month)
        .join("Img");
    let temp_dir = account_root
        .join("temp")
        .join(&chat_hash)
        .join(&month)
        .join("Img");
    let cache_message_dir = account_root
        .join("cache")
        .join(&month)
        .join("Message")
        .join(&chat_hash);

    resolver.collect_nearby_files(&attach_dir, timestamp, &mut candidates);
    resolver.collect_nearby_files(&temp_dir, timestamp, &mut candidates);
    if !has_full_size_candidate(&candidates) {
        resolver.collect_nearby_files(
            &cache_message_dir.join("ImageTemp"),
            timestamp,
            &mut candidates,
        );
        resolver.collect_nearby_files(
            &cache_message_dir.join("Bubble"),
            timestamp,
            &mut candidates,
        );
        resolver.collect_nearby_files(&cache_message_dir.join("Thumb"), timestamp, &mut candidates);
    }
    candidates.sort();
    candidates.dedup();

    let image_md5 = xml_attr(raw_content, "md5");
    let preferred = candidates
        .iter()
        .filter(|path| !is_thumbnail_candidate(path))
        .min_by_key(|path| media_candidate_score(path, timestamp, image_md5.as_deref()))
        .cloned();
    let thumbnail = candidates
        .iter()
        .filter(|path| is_thumbnail_candidate(path))
        .min_by_key(|path| media_candidate_score(path, timestamp, image_md5.as_deref()))
        .cloned();
    let decode_result = decode_first_media_candidate(
        account_root,
        media_cache_dir,
        preferred.as_ref(),
        thumbnail.as_ref(),
        &candidates,
    );
    let (decoded_path, decoder, decode_error) = match decode_result {
        Ok(Some(decoded)) => (
            Some(decoded.path),
            Some(decoded.decoder.to_string()),
            None::<String>,
        ),
        Ok(None) => (None, None, None),
        Err(error) => (None, None, Some(error.to_string())),
    };
    (!candidates.is_empty()).then_some(ImageMedia {
        media_path: preferred,
        thumbnail_path: thumbnail,
        candidates,
        decoded_path,
        decoder,
        decode_error,
    })
}

fn resolve_voice_media(
    resolver: &mut ImageResolveContext,
    account_root: Option<&Path>,
    media_cache_dir: &Path,
    chat_username: &str,
    timestamp: i64,
    raw_content: &str,
) -> Option<ImageMedia> {
    let account_root = account_root?;
    let chat_hash = format!("{:x}", md5::compute(chat_username.as_bytes()));
    let month = Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|dt| dt.format("%Y-%m").to_string())?;
    let mut candidates = Vec::new();

    for leaf in ["Voice", "Audio", "Record", "voice", "audio", "record"] {
        resolver.collect_nearby_files(
            &account_root
                .join("msg")
                .join("attach")
                .join(&chat_hash)
                .join(&month)
                .join(leaf),
            timestamp,
            &mut candidates,
        );
        resolver.collect_nearby_files(
            &account_root
                .join("temp")
                .join(&chat_hash)
                .join(&month)
                .join(leaf),
            timestamp,
            &mut candidates,
        );
        resolver.collect_nearby_files(
            &account_root
                .join("cache")
                .join(&month)
                .join("Message")
                .join(&chat_hash)
                .join(leaf),
            timestamp,
            &mut candidates,
        );
    }

    for dir in [
        account_root
            .join("msg")
            .join("attach")
            .join(&chat_hash)
            .join(&month)
            .join("Rec"),
        account_root
            .join("msg")
            .join("attach")
            .join(&chat_hash)
            .join(&month)
            .join("RecTmp"),
        account_root
            .join("cache")
            .join(&month)
            .join("Message")
            .join(&chat_hash)
            .join("Rec"),
        account_root
            .join("cache")
            .join(&month)
            .join("Message")
            .join(&chat_hash)
            .join("RecTmp"),
    ] {
        resolver.collect_nearby_files_recursive(&dir, 5, timestamp, &mut candidates);
    }

    candidates.sort();
    candidates.dedup();
    let preferred = candidates
        .iter()
        .min_by_key(|path| media_candidate_score(path, timestamp, None))
        .cloned();
    let decode_result = decode_first_voice_candidate(
        account_root,
        media_cache_dir,
        preferred.as_ref(),
        &candidates,
    );
    let (decoded_path, decoder, decode_error) = match decode_result {
        Ok(Some(decoded)) => (
            Some(decoded.path),
            Some(decoded.decoder.to_string()),
            None::<String>,
        ),
        Ok(None) => {
            let error = voice_has_cdn_metadata(raw_content).then(|| {
                "语音文件未落盘，数据库仅包含 CDN 元数据；当前 wxdb 暂不能直接下载微信 CDN 语音"
                    .to_string()
            });
            (None, None, error)
        }
        Err(error) => (None, None, Some(error.to_string())),
    };
    (!candidates.is_empty() || voice_has_cdn_metadata(raw_content)).then_some(ImageMedia {
        media_path: preferred,
        thumbnail_path: None,
        candidates,
        decoded_path,
        decoder,
        decode_error,
    })
}

fn resolve_video_media(
    resolver: &mut ImageResolveContext,
    account_root: Option<&Path>,
    media_cache_dir: &Path,
    chat_username: &str,
    timestamp: i64,
    raw_content: &str,
    packed_info_data: &[u8],
) -> Option<ImageMedia> {
    let account_root = account_root?;
    let chat_hash = format!("{:x}", md5::compute(chat_username.as_bytes()));
    let month = Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|dt| dt.format("%Y-%m").to_string())?;
    let mut candidates = Vec::new();
    let mut local_keys = video_local_keys(raw_content, packed_info_data);
    local_keys.sort();
    local_keys.dedup();
    let direct_video_dir = account_root.join("msg").join("video").join(&month);

    resolver.collect_files_matching_keys(&direct_video_dir, &local_keys, &mut candidates);
    if candidates.is_empty() {
        resolver.collect_nearby_files(&direct_video_dir, timestamp, &mut candidates);
    }

    for leaf in ["Video", "video", "Media", "media", "File", "file"] {
        resolver.collect_nearby_files(
            &account_root
                .join("msg")
                .join("attach")
                .join(&chat_hash)
                .join(&month)
                .join(leaf),
            timestamp,
            &mut candidates,
        );
        resolver.collect_nearby_files(
            &account_root
                .join("temp")
                .join(&chat_hash)
                .join(&month)
                .join(leaf),
            timestamp,
            &mut candidates,
        );
        resolver.collect_nearby_files(
            &account_root
                .join("cache")
                .join(&month)
                .join("Message")
                .join(&chat_hash)
                .join(leaf),
            timestamp,
            &mut candidates,
        );
    }

    for dir in [
        account_root
            .join("msg")
            .join("attach")
            .join(&chat_hash)
            .join(&month)
            .join("Rec"),
        account_root
            .join("msg")
            .join("attach")
            .join(&chat_hash)
            .join(&month)
            .join("RecTmp"),
        account_root
            .join("cache")
            .join(&month)
            .join("Message")
            .join(&chat_hash)
            .join("Rec"),
        account_root
            .join("cache")
            .join(&month)
            .join("Message")
            .join(&chat_hash)
            .join("RecTmp"),
    ] {
        resolver.collect_files_matching_keys_recursive(&dir, 5, &local_keys, &mut candidates);
        resolver.collect_nearby_files_recursive(&dir, 5, timestamp, &mut candidates);
    }

    candidates.sort();
    candidates.dedup();
    let preferred = candidates
        .iter()
        .filter(|path| is_video_candidate(path))
        .min_by_key(|path| media_candidate_score(path, timestamp, None))
        .cloned();
    let thumbnail = candidates
        .iter()
        .filter(|path| is_video_thumbnail_candidate(path))
        .min_by_key(|path| media_candidate_score(path, timestamp, None))
        .cloned();
    let decode_result = decode_first_video_candidate(
        account_root,
        media_cache_dir,
        preferred.as_ref(),
        &candidates,
    );
    let (decoded_path, decoder, decode_error) = match decode_result {
        Ok(Some(decoded)) => (
            Some(decoded.path),
            Some(decoded.decoder.to_string()),
            None::<String>,
        ),
        Ok(None) => {
            let error = if !candidates.is_empty() && preferred.is_none() {
                Some(
                    "完整视频未落盘，仅找到视频缩略图；需要打开微信缓存原视频或实现 CDN 下载"
                        .to_string(),
                )
            } else if video_has_cdn_metadata(raw_content) {
                Some("完整视频未落盘，数据库仅包含 CDN 元数据；当前 wxdb 暂不能直接下载微信 CDN 视频".to_string())
            } else {
                None
            };
            (None, None, error)
        }
        Err(error) => (None, None, Some(error.to_string())),
    };
    (!candidates.is_empty() || video_has_cdn_metadata(raw_content)).then_some(ImageMedia {
        media_path: preferred,
        thumbnail_path: thumbnail,
        candidates,
        decoded_path,
        decoder,
        decode_error,
    })
}

fn decode_first_media_candidate(
    account_root: &Path,
    media_cache_dir: &Path,
    preferred: Option<&PathBuf>,
    thumbnail: Option<&PathBuf>,
    candidates: &[PathBuf],
) -> Result<Option<media::DecodedMedia>> {
    let mut paths = Vec::<&PathBuf>::new();
    if let Some(path) = preferred {
        paths.push(path);
    }
    if let Some(path) = thumbnail {
        paths.push(path);
    }
    for path in candidates {
        if !paths.contains(&path) {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        return Ok(None);
    }

    let mut errors = Vec::new();
    let mut fallback = None;
    for path in paths {
        match media::decode_media_to_cache(path, Some(account_root), media_cache_dir) {
            Ok(decoded) if is_caption_friendly_image_format(decoded.format) => {
                return Ok(Some(decoded))
            }
            Ok(decoded) => {
                if fallback.is_none() {
                    fallback = Some(decoded);
                }
            }
            Err(error) => errors.push(format!("{}: {error:#}", path.display())),
        }
    }
    if fallback.is_some() {
        return Ok(fallback);
    }
    anyhow::bail!("{}", errors.join(" | "))
}

fn decode_first_video_candidate(
    account_root: &Path,
    media_cache_dir: &Path,
    preferred: Option<&PathBuf>,
    candidates: &[PathBuf],
) -> Result<Option<media::DecodedMedia>> {
    let mut paths = Vec::<&PathBuf>::new();
    if let Some(path) = preferred {
        paths.push(path);
    }
    for path in candidates {
        if is_video_candidate(path) && !paths.contains(&path) {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        return Ok(None);
    }

    let mut errors = Vec::new();
    for path in paths {
        match media::decode_media_to_cache(path, Some(account_root), media_cache_dir) {
            Ok(decoded) if is_caption_friendly_video_format(decoded.format) => {
                return Ok(Some(decoded))
            }
            Ok(decoded) => errors.push(format!(
                "{}: 解码得到 {}，不是可转述视频格式",
                path.display(),
                decoded.format
            )),
            Err(error) => errors.push(format!("{}: {error:#}", path.display())),
        }
    }
    anyhow::bail!("{}", errors.join(" | "))
}

fn decode_first_voice_candidate(
    account_root: &Path,
    media_cache_dir: &Path,
    preferred: Option<&PathBuf>,
    candidates: &[PathBuf],
) -> Result<Option<media::DecodedMedia>> {
    let mut paths = Vec::<&PathBuf>::new();
    if let Some(path) = preferred {
        paths.push(path);
    }
    for path in candidates {
        if !paths.contains(&path) {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        return Ok(None);
    }

    let mut errors = Vec::new();
    for path in paths {
        match media::decode_voice_to_cache(path, Some(account_root), media_cache_dir) {
            Ok(decoded) => return Ok(Some(decoded)),
            Err(error) => errors.push(format!("{}: {error:#}", path.display())),
        }
    }
    anyhow::bail!("{}", errors.join(" | "))
}

fn is_caption_friendly_image_format(format: &str) -> bool {
    matches!(format, "jpg" | "png" | "gif" | "webp" | "bmp")
}

fn is_caption_friendly_video_format(format: &str) -> bool {
    matches!(format, "mp4" | "mov" | "mkv" | "webm" | "m4v")
}

fn has_full_size_candidate(paths: &[PathBuf]) -> bool {
    paths.iter().any(|path| !is_thumbnail_candidate(path))
}

fn read_media_candidate_entries(dir: &Path) -> Vec<MediaCandidateEntry> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() || !is_media_candidate_name(&path) {
            continue;
        }
        let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) else {
            continue;
        };
        let Ok(modified_secs) = modified.duration_since(std::time::UNIX_EPOCH) else {
            continue;
        };
        out.push(MediaCandidateEntry {
            path,
            modified_secs: modified_secs.as_secs() as i64,
        });
    }
    out
}

fn read_media_candidate_entries_recursive(
    dir: &Path,
    max_depth: usize,
) -> Vec<MediaCandidateEntry> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            if !is_media_candidate_name(&path) {
                continue;
            }
            let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) else {
                continue;
            };
            let Ok(modified_secs) = modified.duration_since(std::time::UNIX_EPOCH) else {
                continue;
            };
            out.push(MediaCandidateEntry {
                path,
                modified_secs: modified_secs.as_secs() as i64,
            });
        } else if max_depth > 0 && path.is_dir() {
            out.extend(read_media_candidate_entries_recursive(&path, max_depth - 1));
        }
    }
    out
}

fn media_candidate_score(
    path: &Path,
    timestamp: i64,
    image_md5: Option<&str>,
) -> (u8, u8, i64, u64) {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let md5_score = if image_md5.is_some_and(|md5| name.contains(&md5.to_ascii_lowercase())) {
        0
    } else {
        1
    };
    let kind_score = media_candidate_kind_score(path);
    let mtime_delta = path
        .metadata()
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| (duration.as_secs() as i64 - timestamp).abs())
        .unwrap_or(i64::MAX);
    let size_rank = path
        .metadata()
        .map(|meta| u64::MAX - meta.len())
        .unwrap_or(0);
    (md5_score, kind_score, mtime_delta, size_rank)
}

fn media_candidate_kind_score(path: &Path) -> u8 {
    let text = path.to_string_lossy().to_ascii_lowercase();
    if text.contains("\\msg\\attach\\") || text.contains("/msg/attach/") {
        if is_thumbnail_candidate(path) {
            3
        } else {
            0
        }
    } else if text.contains("\\imagetemp\\") || text.contains("/imagetemp/") {
        1
    } else if text.contains("\\bubble\\") || text.contains("/bubble/") {
        2
    } else if is_thumbnail_candidate(path) {
        3
    } else {
        4
    }
}

fn is_media_candidate_name(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    name.ends_with(".dat")
        || name.ends_with(".aud")
        || name.ends_with(".silk")
        || name.ends_with(".amr")
        || name.ends_with(".mp3")
        || name.ends_with(".wav")
        || name.ends_with(".m4a")
        || name.ends_with(".aac")
        || name.ends_with(".ogg")
        || name.ends_with(".flac")
        || name.ends_with(".mp4")
        || name.ends_with(".m4v")
        || name.ends_with(".mov")
        || name.ends_with(".mkv")
        || name.ends_with(".webm")
        || name.ends_with(".jpg")
        || name.ends_with(".jpeg")
        || name.ends_with(".png")
        || name.ends_with(".gif")
        || name.ends_with(".webp")
}

fn is_video_candidate(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    name.ends_with(".mp4")
        || name.ends_with(".m4v")
        || name.ends_with(".mov")
        || name.ends_with(".mkv")
        || name.ends_with(".webm")
}

fn is_video_thumbnail_candidate(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    is_thumbnail_candidate(path)
        || name.ends_with(".jpg")
        || name.ends_with(".jpeg")
        || name.ends_with(".png")
        || name.ends_with(".webp")
}

fn is_thumbnail_candidate(path: &Path) -> bool {
    let text = path.to_string_lossy().to_ascii_lowercase();
    text.contains("\\thumb\\")
        || text.contains("/thumb/")
        || text.contains("_thumb")
        || text.contains("_t.")
}

fn video_local_keys(raw_content: &str, packed_info_data: &[u8]) -> Vec<String> {
    let mut keys = packed_ascii_hex_keys(packed_info_data);
    for attr in ["md5", "newmd5", "rawmd5", "originsourcemd5"] {
        if let Some(value) = xml_attr(raw_content, attr) {
            if is_probable_hex_media_key(&value) {
                keys.push(value);
            }
        }
    }
    keys
}

fn video_has_cdn_metadata(raw_content: &str) -> bool {
    xml_attr(raw_content, "cdnvideourl").is_some()
        || xml_attr(raw_content, "cdnrawvideourl").is_some()
}

fn voice_has_cdn_metadata(raw_content: &str) -> bool {
    xml_attr(raw_content, "voiceurl").is_some()
}

fn packed_ascii_hex_keys(data: &[u8]) -> Vec<String> {
    let mut keys = Vec::new();
    let mut start = None;
    for (index, byte) in data.iter().copied().enumerate() {
        if byte.is_ascii_hexdigit() {
            if start.is_none() {
                start = Some(index);
            }
            continue;
        }
        if let Some(start_index) = start.take() {
            push_packed_hex_key(data, start_index, index, &mut keys);
        }
    }
    if let Some(start_index) = start {
        push_packed_hex_key(data, start_index, data.len(), &mut keys);
    }
    keys
}

fn push_packed_hex_key(data: &[u8], start: usize, end: usize, keys: &mut Vec<String>) {
    if end.saturating_sub(start) < 16 {
        return;
    }
    if let Ok(value) = std::str::from_utf8(&data[start..end]) {
        if is_probable_hex_media_key(value) {
            keys.push(value.to_ascii_lowercase());
        }
    }
}

fn is_probable_hex_media_key(value: &str) -> bool {
    let value = value.trim();
    value.len() >= 16 && value.len() <= 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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

#[allow(clippy::too_many_arguments)]
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
    match base_msg_type(local_type) {
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
    match base_msg_type(local_type) {
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

fn base_msg_type(local_type: i64) -> i64 {
    (local_type as u64 & 0xFFFF_FFFF) as i64
}

fn msg_type_filter_values(text_only: bool, msg_types: &[String]) -> Vec<i64> {
    let mut values = if msg_types.is_empty() {
        if text_only {
            vec![1]
        } else {
            Vec::new()
        }
    } else {
        msg_types
            .iter()
            .filter_map(|value| msg_type_name_to_local_type(value))
            .collect::<Vec<_>>()
    };
    values.sort_unstable();
    values.dedup();
    values
}

fn msg_type_name_to_local_type(value: &str) -> Option<i64> {
    match value.trim().to_ascii_lowercase().as_str() {
        "all" | "*" => None,
        "text" | "文本" | "文字" | "1" => Some(1),
        "image" | "img" | "图片" | "3" => Some(3),
        "voice" | "语音" | "34" => Some(34),
        "video" | "视频" | "43" => Some(43),
        "sticker" | "emoji" | "表情" | "47" => Some(47),
        "link" | "链接" | "49" => Some(49),
        "system" | "系统" | "10000" => Some(10000),
        _ => value.parse::<i64>().ok(),
    }
}

fn xml_attr(text: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let (_, rest) = text.split_once(&needle)?;
    let (value, _) = rest.split_once('"')?;
    (!value.is_empty()).then(|| value.to_string())
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
    fn formats_image_placeholder_with_local_id() {
        assert_eq!(
            format_content(26032, 3, r#"<msg><img md5="abc" /></msg>"#, true),
            "[图片] local_id=26032"
        );
        assert_eq!(fmt_type(3), "image");
    }

    #[test]
    fn message_type_filter_supports_text_image_and_voice() {
        assert_eq!(
            msg_type_filter_values(
                false,
                &["text".to_string(), "image".to_string(), "voice".to_string()]
            ),
            vec![1, 3, 34]
        );
        assert_eq!(
            msg_type_filter_values(false, &["图片".to_string()]),
            vec![3]
        );
        assert_eq!(
            msg_type_filter_values(false, &["语音".to_string()]),
            vec![34]
        );
        assert!(msg_type_filter_values(false, &["all".to_string()]).is_empty());
    }

    #[test]
    fn image_media_candidates_prefer_attach_over_bubble() {
        assert_eq!(
            media_candidate_kind_score(Path::new(
                r"\\?\D:\Temp\xwechat_files\wxid_x\msg\attach\chat\2026-06\Img\a.dat"
            )),
            0
        );
        assert_eq!(
            media_candidate_kind_score(Path::new(
                r"\\?\D:\Temp\xwechat_files\wxid_x\cache\2026-06\Message\chat\Bubble\a_b.dat"
            )),
            2
        );
        assert_eq!(
            media_candidate_kind_score(Path::new(
                r"\\?\D:\Temp\xwechat_files\wxid_x\msg\attach\chat\2026-06\Img\a_t.dat"
            )),
            3
        );
    }

    #[test]
    fn caption_friendly_image_formats_exclude_hevc() {
        assert!(is_caption_friendly_image_format("jpg"));
        assert!(is_caption_friendly_image_format("png"));
        assert!(!is_caption_friendly_image_format("hevc"));
    }

    #[test]
    fn media_candidate_names_include_voice_files() {
        assert!(is_media_candidate_name(Path::new("msg.aud")));
        assert!(is_media_candidate_name(Path::new("msg.silk")));
        assert!(is_media_candidate_name(Path::new("msg.amr")));
        assert!(is_media_candidate_name(Path::new("msg.wav")));
    }

    #[test]
    fn media_candidate_names_include_video_files() {
        assert!(is_media_candidate_name(Path::new("clip.mp4")));
        assert!(is_media_candidate_name(Path::new("clip.mov")));
        assert!(is_media_candidate_name(Path::new("clip.mkv")));
        assert!(is_video_candidate(Path::new("clip.webm")));
    }

    #[test]
    fn packed_info_extracts_video_local_key() {
        let key = "53f905fb8d4377e83a12752fb977905c";
        let mut data = vec![0x08, 0x04, 0x10, 0x02, 0x22, 0x22, 0x42, 0x20];
        data.extend_from_slice(key.as_bytes());
        data.push(0x58);

        assert_eq!(packed_ascii_hex_keys(&data), vec![key.to_string()]);
    }

    #[test]
    fn video_media_uses_direct_video_dir_and_keeps_thumbnail_separate() {
        let root = temp_test_dir("wxdb-video-media");
        let key = "53f905fb8d4377e83a12752fb977905c";
        let video_dir = root.join("msg").join("video").join("2026-06");
        std::fs::create_dir_all(&video_dir).unwrap();
        let full = video_dir.join(format!("{key}.mp4"));
        let thumb = video_dir.join(format!("{key}_thumb.jpg"));
        std::fs::write(&full, b"\x00\x00\x00\x18ftypmp42payload").unwrap();
        std::fs::write(&thumb, [0xff, 0xd8, 0xff, 0xe0]).unwrap();
        let mut packed = vec![0x08, 0x04, 0x10, 0x02, 0x22, 0x22, 0x42, 0x20];
        packed.extend_from_slice(key.as_bytes());
        let timestamp = Local
            .with_ymd_and_hms(2026, 6, 6, 11, 50, 10)
            .unwrap()
            .timestamp();
        let mut resolver = ImageResolveContext::default();

        let media = resolve_video_media(
            &mut resolver,
            Some(&root),
            &root.join("cache"),
            "chat@chatroom",
            timestamp,
            r#"<videomsg cdnvideourl="cdn" />"#,
            &packed,
        )
        .unwrap();

        assert_eq!(media.media_path.as_deref(), Some(full.as_path()));
        assert_eq!(media.thumbnail_path.as_deref(), Some(thumb.as_path()));
        assert!(media.decoded_path.is_some());
        assert_eq!(media.decoder.as_deref(), Some("plain"));
        assert!(media.decode_error.is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_media_does_not_use_thumbnail_as_decoded_video() {
        let root = temp_test_dir("wxdb-video-thumb-only");
        let key = "d2b0b3ee90e762e15080415f948070f1";
        let video_dir = root.join("msg").join("video").join("2026-06");
        std::fs::create_dir_all(&video_dir).unwrap();
        let thumb = video_dir.join(format!("{key}_thumb.jpg"));
        std::fs::write(&thumb, [0xff, 0xd8, 0xff, 0xe0]).unwrap();
        let mut packed = vec![0x08, 0x04, 0x10, 0x02, 0x22, 0x22, 0x42, 0x20];
        packed.extend_from_slice(key.as_bytes());
        let timestamp = Local
            .with_ymd_and_hms(2026, 6, 6, 9, 22, 6)
            .unwrap()
            .timestamp();
        let mut resolver = ImageResolveContext::default();

        let media = resolve_video_media(
            &mut resolver,
            Some(&root),
            &root.join("cache"),
            "chat@chatroom",
            timestamp,
            r#"<videomsg cdnvideourl="cdn" />"#,
            &packed,
        )
        .unwrap();

        assert!(media.media_path.is_none());
        assert_eq!(media.thumbnail_path.as_deref(), Some(thumb.as_path()));
        assert!(media.decoded_path.is_none());
        assert!(media
            .decode_error
            .as_deref()
            .unwrap()
            .contains("完整视频未落盘"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_media_finds_nested_rec_source_file() {
        let root = temp_test_dir("wxdb-video-rec-source");
        let key = "878779b7187a1c3c0653b21df139d47d";
        let chat_hash = format!("{:x}", md5::compute("chat@chatroom".as_bytes()));
        let rec_video_dir = root
            .join("msg")
            .join("attach")
            .join(&chat_hash)
            .join("2026-06")
            .join("Rec")
            .join("nested")
            .join("V");
        std::fs::create_dir_all(&rec_video_dir).unwrap();
        let full = rec_video_dir.join(format!("{key}.mp4"));
        std::fs::write(&full, b"\x00\x00\x00\x18ftypmp42payload").unwrap();
        let mut packed = vec![0x08, 0x04, 0x10, 0x02, 0x22, 0x22, 0x42, 0x20];
        packed.extend_from_slice(key.as_bytes());
        let timestamp = Local
            .with_ymd_and_hms(2026, 6, 6, 12, 5, 0)
            .unwrap()
            .timestamp();
        let mut resolver = ImageResolveContext::default();

        let media = resolve_video_media(
            &mut resolver,
            Some(&root),
            &root.join("cache"),
            "chat@chatroom",
            timestamp,
            r#"<videomsg cdnvideourl="cdn" />"#,
            &packed,
        )
        .unwrap();

        assert_eq!(media.media_path.as_deref(), Some(full.as_path()));
        assert!(media.decoded_path.is_some());
        assert_eq!(media.decoder.as_deref(), Some("plain"));
        assert!(media.decode_error.is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn voice_media_finds_nested_rectmp_candidate() {
        let root = temp_test_dir("wxdb-voice-rectmp-source");
        let chat_hash = format!("{:x}", md5::compute("chat@chatroom".as_bytes()));
        let now = Local::now();
        let month = now.format("%Y-%m").to_string();
        let rec_voice_dir = root
            .join("cache")
            .join(&month)
            .join("Message")
            .join(&chat_hash)
            .join("RecTmp")
            .join("nested")
            .join("Voice");
        std::fs::create_dir_all(&rec_voice_dir).unwrap();
        let voice = rec_voice_dir.join("voice.silk");
        std::fs::write(&voice, b"not-a-real-silk").unwrap();
        let timestamp = now.timestamp();
        let mut resolver = ImageResolveContext::default();

        let media = resolve_voice_media(
            &mut resolver,
            Some(&root),
            &root.join("cache"),
            "chat@chatroom",
            timestamp,
            r#"<voicemsg voiceurl="cdn" />"#,
        )
        .unwrap();

        assert_eq!(media.media_path.as_deref(), Some(voice.as_path()));
        assert!(media.decoded_path.is_none());
        assert!(media.decode_error.is_some());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn media_decode_budget_can_disable_or_limit_image_resolution() {
        let mut disabled = Some(0);
        assert!(!consume_media_decode_budget(&mut disabled));
        assert_eq!(disabled, Some(0));

        let mut limited = Some(2);
        assert!(consume_media_decode_budget(&mut limited));
        assert_eq!(limited, Some(1));
        assert!(consume_media_decode_budget(&mut limited));
        assert_eq!(limited, Some(0));
        assert!(!consume_media_decode_budget(&mut limited));

        let mut unlimited = None;
        assert!(consume_media_decode_budget(&mut unlimited));
        assert!(consume_media_decode_budget(&mut unlimited));
        assert_eq!(unlimited, None);
    }

    #[test]
    fn extracts_xml_attribute_values() {
        assert_eq!(
            xml_attr(r#"<img md5="0ebf" aeskey="abc" />"#, "md5").as_deref(),
            Some("0ebf")
        );
        assert_eq!(
            xml_attr(r#"<img md5="0ebf" aeskey="abc" />"#, "aeskey").as_deref(),
            Some("abc")
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
        assert!(is_missing_db_key_error(
            "微信数据库读取不完整：以下消息分片缺少数据库密钥"
        ));
        assert!(!is_missing_db_key_error("打开 contact.db 失败"));
    }

    #[test]
    fn retries_when_a_stable_source_snapshot_cannot_be_captured() {
        let error = anyhow::anyhow!("微信数据库持续写入，4 次尝试仍无法取得稳定快照");

        assert!(should_retry_store_query_error(&error));
    }

    #[test]
    fn store_failure_prefers_real_error_over_missing_key_candidates() {
        let message = format_store_query_failure(
            &[
                r"\\?\D:\active\db_storage: 解密数据库失败: 磁盘空间不足。 (os error 112)"
                    .to_string(),
            ],
            &[
                r"\\?\D:\old-a\db_storage: 没有可用数据库密钥".to_string(),
                r"\\?\D:\old-b\db_storage: 没有可用数据库密钥".to_string(),
            ],
        );

        assert!(message.contains("主要错误"));
        assert!(message.contains("磁盘空间不足"));
        assert!(message.contains("另有 2 个候选账号目录缺少数据库密钥"));
    }

    #[test]
    fn store_failure_reports_all_candidates_missing_keys() {
        let message = format_store_query_failure(
            &[],
            &[
                "store-a: 没有可用数据库密钥".to_string(),
                "store-b: 没有可用数据库密钥".to_string(),
            ],
        );

        assert!(message.contains("未找到任何带可用数据库密钥的微信账号目录"));
        assert!(message.contains("共 2 个候选"));
    }

    fn temp_test_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()))
    }
}
