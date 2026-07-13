use anyhow::Result;
use chrono::{Local, TimeZone, Utc};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use wechat_summary_wxdb::{doctor, query_history, refresh_keys, HistoryQuery};

#[derive(Parser)]
#[command(name = "wxdb")]
#[command(about = "WeChat database history CLI with memory key scan and mtime cache")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init,
    Doctor {
        #[arg(long)]
        json: bool,
    },
    History {
        chat: String,
        #[arg(short = 'n', long = "limit", default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long = "type")]
        msg_type: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        media_decode_limit: Option<usize>,
        #[arg(long)]
        before_local_id: Option<i64>,
    },
    Export {
        chat: String,
        #[arg(short = 'n', long = "limit", default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long = "format", default_value = "json")]
        format: String,
        #[arg(short = 'o', long = "output")]
        output: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init => {
            let config = wechat_summary_wxdb::RuntimeConfig::load();
            let report = refresh_keys(&config)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Doctor { json } => {
            let report = doctor()?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("cache_dir: {}", report.cache_dir.display());
                println!("keys_file: {}", report.keys_file.display());
                for store in report.stores {
                    println!(
                        "{} message_shards={} known_keys={} missing_message_keys={}",
                        store.db_dir.display(),
                        store.message_shards,
                        store.known_keys,
                        store.missing_message_keys.len()
                    );
                }
            }
        }
        Command::History {
            chat,
            limit,
            since,
            until,
            msg_type,
            json,
            media_decode_limit,
            before_local_id,
        } => {
            let result = query_history(HistoryQuery {
                chat_name: chat,
                since: since.as_deref().map(parse_time).transpose()?,
                until: until.as_deref().map(parse_time_end).transpose()?,
                before_local_id,
                limit,
                text_only: msg_type.as_deref().map(is_text_type).unwrap_or(false),
                msg_types: msg_type
                    .as_deref()
                    .map(history_type_filter)
                    .unwrap_or_default(),
                media_decode_limit,
            })?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                for message in result.messages {
                    println!(
                        "[{}] {}: {}",
                        message.time,
                        if message.sender.is_empty() {
                            "unknown"
                        } else {
                            &message.sender
                        },
                        message.content
                    );
                }
            }
        }
        Command::Export {
            chat,
            limit,
            since,
            until,
            format,
            output,
        } => {
            if format.to_lowercase() != "json" {
                anyhow::bail!("wxdb export 目前只支持 --format json");
            }
            let result = query_history(HistoryQuery {
                chat_name: chat,
                since: since.as_deref().map(parse_time).transpose()?,
                until: until.as_deref().map(parse_time_end).transpose()?,
                before_local_id: None,
                limit,
                text_only: false,
                msg_types: Vec::new(),
                media_decode_limit: None,
            })?;
            if let Some(parent) = output.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(output, serde_json::to_string_pretty(&result)?)?;
        }
    }
    Ok(())
}

fn parse_time(s: &str) -> Result<chrono::DateTime<Utc>> {
    if let Ok(value) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(value.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return local_to_utc(dt, s);
        }
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return local_to_utc(date.and_hms_opt(0, 0, 0).unwrap(), s);
    }
    anyhow::bail!(
        "无法解析时间 '{}'，支持 YYYY-MM-DD / YYYY-MM-DD HH:MM / YYYY-MM-DD HH:MM:SS",
        s
    )
}

fn parse_time_end(s: &str) -> Result<chrono::DateTime<Utc>> {
    if s.len() == 10 {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            return local_to_utc(date.and_hms_opt(23, 59, 59).unwrap(), s);
        }
    }
    parse_time(s)
}

fn local_to_utc(dt: chrono::NaiveDateTime, raw: &str) -> Result<chrono::DateTime<Utc>> {
    Local
        .from_local_datetime(&dt)
        .single()
        .map(|dt| dt.with_timezone(&Utc))
        .ok_or_else(|| anyhow::anyhow!("本地时间歧义: {raw}"))
}

fn is_text_type(value: &str) -> bool {
    matches!(
        value.to_lowercase().as_str(),
        "text" | "1" | "文本" | "文字"
    )
}

fn history_type_filter(value: &str) -> Vec<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "all" | "*" => Vec::new(),
        "txt" => vec!["text".to_string()],
        other => vec![other.to_string()],
    }
}
