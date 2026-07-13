use anyhow::{bail, Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use crate::config;
use crate::crypto::{self, wal};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheMode {
    CacheHit,
    WalIncremental,
    FullDecrypt,
    StaleCache,
}

#[derive(Debug, Clone)]
pub struct CacheResolve {
    pub path: PathBuf,
    pub mode: CacheMode,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MtimeEntry {
    db_mtime: u64,
    wal_mtime: u64,
    path: String,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    db_mtime: u64,
    wal_mtime: u64,
    decrypted_path: PathBuf,
}

pub struct DbCache {
    db_dir: PathBuf,
    cache_dir: PathBuf,
    mtime_file: PathBuf,
    keys: HashMap<String, String>,
    entries: HashMap<String, CacheEntry>,
}

static CACHE_FILE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
const CACHE_SNAPSHOTS_PER_DB: usize = 4;
const STALE_TEMP_MAX_AGE: Duration = Duration::from_secs(10 * 60);
const CACHE_DISK_SAFETY_BYTES: u64 = 512 * 1024 * 1024;

impl DbCache {
    pub fn new(
        db_dir: PathBuf,
        cache_dir: PathBuf,
        mtime_file: PathBuf,
        keys: HashMap<String, String>,
    ) -> Result<Self> {
        fs::create_dir_all(&cache_dir)?;
        cleanup_stale_temp_artifacts(&cache_dir);
        let mut cache = Self {
            db_dir,
            cache_dir,
            mtime_file,
            keys,
            entries: HashMap::new(),
        };
        cache.load_persistent();
        Ok(cache)
    }

    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    pub fn keys(&self) -> &HashMap<String, String> {
        &self.keys
    }

    pub fn get(&mut self, rel_key: &str) -> Result<Option<PathBuf>> {
        Ok(self.get_with_mode(rel_key)?.map(|resolve| resolve.path))
    }

    pub fn get_with_mode(&mut self, rel_key: &str) -> Result<Option<CacheResolve>> {
        let Some(enc_key_hex) = self.keys.get(rel_key).cloned() else {
            return Ok(None);
        };
        let db_path = self.db_dir.join(config::rel_to_path(rel_key));
        if !db_path.exists() {
            return Ok(None);
        }

        let wal_path = wal_path_for(&db_path);
        let db_mtime = mtime_nanos(&db_path);
        let wal_mtime = if wal_path.exists() {
            mtime_nanos(&wal_path)
        } else {
            0
        };
        let db_size = file_len(&db_path);
        let wal_size = if wal_path.exists() {
            file_len(&wal_path)
        } else {
            0
        };
        let enc_key = crypto::hex_to_32bytes(&enc_key_hex)
            .with_context(|| format!("密钥格式错误: {rel_key}"))?;
        let out_path = self.cache_file_path(rel_key, db_mtime, wal_mtime);
        let cache_lock = cache_file_lock(&db_path);
        let _cache_guard = cache_lock.lock().unwrap();
        cleanup_stale_temp_artifacts(&self.cache_dir);
        self.load_persistent();
        if out_path.exists() {
            if let Err(error) = validate_sqlite_cache(&out_path) {
                tracing::warn!(
                    rel_key,
                    path = %out_path.display(),
                    error = %error,
                    "wxdb cache snapshot is unreadable; rebuilding"
                );
                remove_cache_snapshot(&out_path);
                self.entries.remove(rel_key);
            } else {
                self.entries.insert(
                    rel_key.to_string(),
                    CacheEntry {
                        db_mtime,
                        wal_mtime,
                        decrypted_path: out_path.clone(),
                    },
                );
                self.save_persistent();
                self.prune_old_cache_snapshots(rel_key, &out_path);
                return Ok(Some(CacheResolve {
                    path: out_path,
                    mode: CacheMode::CacheHit,
                    warning: None,
                }));
            }
        }

        let cached = self.entries.get(rel_key).cloned();
        if let Some(entry) = cached.clone() {
            if entry.db_mtime == db_mtime && entry.decrypted_path.exists() {
                match self.publish_from_cached_entry(
                    rel_key, &entry, &out_path, &wal_path, &enc_key, wal_mtime,
                ) {
                    Ok(mode) => {
                        self.entries.insert(
                            rel_key.to_string(),
                            CacheEntry {
                                db_mtime,
                                wal_mtime,
                                decrypted_path: out_path.clone(),
                            },
                        );
                        self.save_persistent();
                        self.prune_old_cache_snapshots(rel_key, &out_path);
                        return Ok(Some(CacheResolve {
                            path: out_path,
                            mode,
                            warning: None,
                        }));
                    }
                    Err(error) => {
                        if validate_sqlite_cache(&entry.decrypted_path).is_ok() {
                            let warning =
                                format!("缓存快照刷新失败，降级使用上一份缓存 {rel_key}: {error}");
                            return Ok(Some(CacheResolve {
                                path: entry.decrypted_path,
                                mode: CacheMode::StaleCache,
                                warning: Some(warning),
                            }));
                        }
                        tracing::warn!(
                            rel_key,
                            path = %entry.decrypted_path.display(),
                            error = %error,
                            "wxdb stale cache snapshot is unreadable; rebuilding from source"
                        );
                        remove_cache_snapshot(&entry.decrypted_path);
                        self.entries.remove(rel_key);
                    }
                }
            }
        }

        let tmp_path = out_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        let decrypt_result = (|| -> Result<()> {
            ensure_cache_space(
                &self.cache_dir,
                db_size
                    .saturating_add(wal_size)
                    .saturating_add(CACHE_DISK_SAFETY_BYTES),
            )?;
            crypto::full_decrypt(&db_path, &tmp_path, &enc_key)?;
            apply_wal_to_cached(&wal_path, &tmp_path, &enc_key)?;
            let latest_db_mtime = mtime_nanos(&db_path);
            let latest_wal_mtime = if wal_path.exists() {
                mtime_nanos(&wal_path)
            } else {
                0
            };
            if let Err(error) = validate_sqlite_cache(&tmp_path) {
                if latest_db_mtime != db_mtime || latest_wal_mtime != wal_mtime {
                    bail!(
                        "源数据库在解密期间发生变化且临时快照不可读，丢弃本次快照并等待下一次重试: {rel_key}: {error:#}"
                    );
                }
                return Err(error).with_context(|| {
                    format!(
                        "解密结果不是可读 SQLite 数据库，可能数据库密钥已过期或不匹配: {rel_key}"
                    )
                });
            }
            publish_temp_cache(&tmp_path, &out_path)?;
            Ok(())
        })();

        if let Err(error) = decrypt_result {
            remove_cache_snapshot(&tmp_path);
            if let Some(entry) = cached {
                if entry.decrypted_path.exists()
                    && validate_sqlite_cache(&entry.decrypted_path).is_ok()
                {
                    let warning = format!("全量解密失败，降级使用上一份缓存 {rel_key}: {error}");
                    return Ok(Some(CacheResolve {
                        path: entry.decrypted_path,
                        mode: CacheMode::StaleCache,
                        warning: Some(warning),
                    }));
                }
            }
            return Err(error).with_context(|| format!("解密数据库失败: {rel_key}"));
        }

        self.entries.insert(
            rel_key.to_string(),
            CacheEntry {
                db_mtime,
                wal_mtime,
                decrypted_path: out_path.clone(),
            },
        );
        self.save_persistent();
        self.prune_old_cache_snapshots(rel_key, &out_path);
        Ok(Some(CacheResolve {
            path: out_path,
            mode: CacheMode::FullDecrypt,
            warning: None,
        }))
    }

    fn cache_file_path(&self, rel_key: &str, db_mtime: u64, wal_mtime: u64) -> PathBuf {
        let hash = cache_file_hash(rel_key);
        self.cache_dir
            .join(format!("{hash}-{db_mtime:x}-{wal_mtime:x}.db"))
    }

    fn prune_old_cache_snapshots(&self, rel_key: &str, current_path: &Path) {
        let hash = cache_file_hash(rel_key);
        let Ok(entries) = std::fs::read_dir(&self.cache_dir) else {
            return;
        };
        let mut snapshots = entries
            .flatten()
            .filter_map(|entry| {
                let path = entry.path();
                if !is_cache_snapshot_for_hash(&path, &hash) {
                    return None;
                }
                let modified = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                Some((modified, path))
            })
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|(modified, _)| std::cmp::Reverse(*modified));

        let mut keep = HashSet::new();
        keep.insert(current_path.to_path_buf());
        for (_, path) in snapshots.iter().take(CACHE_SNAPSHOTS_PER_DB) {
            keep.insert(path.clone());
        }

        for (_, path) in snapshots {
            if !keep.contains(&path) {
                remove_cache_snapshot(&path);
            }
        }
    }

    fn publish_from_cached_entry(
        &self,
        rel_key: &str,
        entry: &CacheEntry,
        out_path: &Path,
        wal_path: &Path,
        enc_key: &[u8; 32],
        wal_mtime: u64,
    ) -> Result<CacheMode> {
        let tmp_path = out_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        let result = (|| -> Result<CacheMode> {
            let cached_size = file_len(&entry.decrypted_path);
            ensure_cache_space(
                &self.cache_dir,
                cached_size.saturating_add(CACHE_DISK_SAFETY_BYTES),
            )?;
            fs::copy(&entry.decrypted_path, &tmp_path).with_context(|| {
                format!(
                    "复制缓存快照失败: {} -> {}",
                    entry.decrypted_path.display(),
                    tmp_path.display()
                )
            })?;
            let mode = if entry.wal_mtime == wal_mtime {
                CacheMode::CacheHit
            } else {
                apply_wal_to_cached(wal_path, &tmp_path, enc_key)
                    .with_context(|| format!("WAL 增量应用失败: {rel_key}"))?;
                CacheMode::WalIncremental
            };
            validate_sqlite_cache(&tmp_path).with_context(|| {
                format!("缓存快照不是可读 SQLite 数据库，可能数据库密钥已过期或不匹配: {rel_key}")
            })?;
            publish_temp_cache(&tmp_path, out_path)?;
            Ok(mode)
        })();
        if result.is_err() {
            remove_cache_snapshot(&tmp_path);
        }
        result
    }

    fn load_persistent(&mut self) {
        let Ok(content) = fs::read_to_string(&self.mtime_file) else {
            return;
        };
        let Ok(saved) = serde_json::from_str::<HashMap<String, MtimeEntry>>(&content) else {
            return;
        };
        for (rel_key, entry) in saved {
            let path = PathBuf::from(&entry.path);
            if !path.exists() {
                continue;
            }
            let db_path = self.db_dir.join(config::rel_to_path(&rel_key));
            if !db_path.exists() {
                continue;
            }
            if mtime_nanos(&db_path) == entry.db_mtime {
                self.entries.insert(
                    rel_key,
                    CacheEntry {
                        db_mtime: entry.db_mtime,
                        wal_mtime: entry.wal_mtime,
                        decrypted_path: path,
                    },
                );
            }
        }
    }

    fn save_persistent(&self) {
        let data: HashMap<String, MtimeEntry> = self
            .entries
            .iter()
            .map(|(rel, entry)| {
                (
                    rel.clone(),
                    MtimeEntry {
                        db_mtime: entry.db_mtime,
                        wal_mtime: entry.wal_mtime,
                        path: entry.decrypted_path.to_string_lossy().into_owned(),
                    },
                )
            })
            .collect();
        if config::ensure_parent(&self.mtime_file).is_err() {
            return;
        }
        let tmp = self
            .mtime_file
            .with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        if let Ok(json) = serde_json::to_string_pretty(&data) {
            if fs::write(&tmp, json).is_ok() {
                if self.mtime_file.exists() {
                    let _ = fs::remove_file(&self.mtime_file);
                }
                let _ = fs::rename(tmp, &self.mtime_file);
            }
        }
    }
}

fn cache_file_lock(path: &Path) -> Arc<Mutex<()>> {
    let locks = CACHE_FILE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().unwrap();
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn cache_file_hash(rel_key: &str) -> String {
    format!("{:x}", md5::compute(rel_key.as_bytes()))
}

fn validate_sqlite_cache(path: &Path) -> Result<()> {
    let conn = Connection::open(path)
        .with_context(|| format!("打开 SQLite 缓存失败: {}", path.display()))?;
    conn.query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))
        .with_context(|| format!("读取 SQLite schema 失败: {}", path.display()))?;
    Ok(())
}

fn is_cache_snapshot_for_hash(path: &Path, hash: &str) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    name == format!("{hash}.db") || (name.starts_with(&format!("{hash}-")) && name.ends_with(".db"))
}

fn remove_cache_snapshot(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("db-wal"));
    let _ = fs::remove_file(path.with_extension("db-shm"));
    let _ = fs::remove_file(sqlite_suffix_path(path, "-wal"));
    let _ = fs::remove_file(sqlite_suffix_path(path, "-shm"));
}

fn publish_temp_cache(tmp_path: &Path, out_path: &Path) -> Result<()> {
    if out_path.exists() {
        remove_cache_snapshot(out_path);
    }
    fs::rename(tmp_path, out_path)
        .with_context(|| format!("发布缓存快照失败: {}", out_path.display()))
}

fn apply_wal_to_cached(wal_path: &Path, out_path: &Path, enc_key: &[u8; 32]) -> Result<()> {
    if wal_path.exists() {
        wal::apply_wal(wal_path, out_path, enc_key)?;
    }
    Ok(())
}

fn wal_path_for(db_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db_path.display()))
}

fn mtime_nanos(path: &Path) -> u64 {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn ensure_cache_space(cache_dir: &Path, required_bytes: u64) -> Result<()> {
    cleanup_stale_temp_artifacts(cache_dir);
    let Some(free_bytes) = available_space(cache_dir) else {
        return Ok(());
    };
    if free_bytes < required_bytes {
        bail!(
            "wxdb 缓存磁盘空间不足: cache_dir={} free={} required={}；请清理磁盘，或在 GUI 将 wxdb 缓存位置改到空间充足的磁盘",
            cache_dir.display(),
            format_bytes(free_bytes),
            format_bytes(required_bytes)
        );
    }
    Ok(())
}

fn cleanup_stale_temp_artifacts(cache_dir: &Path) {
    let Ok(entries) = fs::read_dir(cache_dir) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let path = entry.path();
        if !is_temp_artifact(&path) {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_none_or(|age| age >= STALE_TEMP_MAX_AGE);
        if stale {
            remove_cache_snapshot(&path);
        }
    }
}

fn is_temp_artifact(path: &Path) -> bool {
    path.file_name()
        .and_then(OsStr::to_str)
        .is_some_and(|name| name.contains(".tmp-") || name.contains("_mtimes.tmp-"))
}

fn sqlite_suffix_path(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{}", path.display(), suffix))
}

fn format_bytes(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    }
}

#[cfg(target_os = "windows")]
fn available_space(path: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows::core::PCWSTR;
    use windows::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    wide.push(0);
    let mut free = 0u64;
    let ok = unsafe { GetDiskFreeSpaceExW(PCWSTR(wide.as_ptr()), Some(&mut free), None, None) };
    ok.is_ok().then_some(free)
}

#[cfg(not(target_os = "windows"))]
fn available_space(_path: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_snapshot_matcher_accepts_versioned_and_legacy_db_files() {
        let hash = "170fb82914dc74888c50bb22f0798c60";

        assert!(is_cache_snapshot_for_hash(
            Path::new("170fb82914dc74888c50bb22f0798c60.db"),
            hash
        ));
        assert!(is_cache_snapshot_for_hash(
            Path::new("170fb82914dc74888c50bb22f0798c60-1-2.db"),
            hash
        ));
        assert!(!is_cache_snapshot_for_hash(
            Path::new("170fb82914dc74888c50bb22f0798c60-1-2.db-wal"),
            hash
        ));
        assert!(!is_cache_snapshot_for_hash(
            Path::new("923f080775be265c49f979fda84c0cb6-1-2.db"),
            hash
        ));
    }
}
