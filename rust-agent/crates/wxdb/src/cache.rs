use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::config;
use crate::crypto::{self, wal};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

impl DbCache {
    pub fn new(
        db_dir: PathBuf,
        cache_dir: PathBuf,
        mtime_file: PathBuf,
        keys: HashMap<String, String>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&cache_dir)?;
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
        let enc_key = crypto::hex_to_32bytes(&enc_key_hex)
            .with_context(|| format!("密钥格式错误: {rel_key}"))?;
        let cached = self.entries.get(rel_key).cloned();

        if let Some(entry) = cached {
            if entry.db_mtime == db_mtime && entry.decrypted_path.exists() {
                if entry.wal_mtime == wal_mtime {
                    return Ok(Some(CacheResolve {
                        path: entry.decrypted_path,
                        mode: CacheMode::CacheHit,
                        warning: None,
                    }));
                }
                match apply_wal_to_cached(&wal_path, &entry.decrypted_path, &enc_key) {
                    Ok(()) => {
                        self.entries.insert(
                            rel_key.to_string(),
                            CacheEntry {
                                db_mtime,
                                wal_mtime,
                                decrypted_path: entry.decrypted_path.clone(),
                            },
                        );
                        self.save_persistent();
                        return Ok(Some(CacheResolve {
                            path: entry.decrypted_path,
                            mode: CacheMode::WalIncremental,
                            warning: None,
                        }));
                    }
                    Err(error) => {
                        let warning =
                            format!("WAL 增量应用失败，降级使用上一份缓存 {}: {error}", rel_key);
                        return Ok(Some(CacheResolve {
                            path: entry.decrypted_path,
                            mode: CacheMode::StaleCache,
                            warning: Some(warning),
                        }));
                    }
                }
            }
        }

        let out_path = self.cache_file_path(rel_key);
        let tmp_path = out_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        let decrypt_result = (|| -> Result<()> {
            crypto::full_decrypt(&db_path, &tmp_path, &enc_key)?;
            apply_wal_to_cached(&wal_path, &tmp_path, &enc_key)?;
            if out_path.exists() {
                let _ = std::fs::remove_file(&out_path);
            }
            std::fs::rename(&tmp_path, &out_path)?;
            Ok(())
        })();

        if let Err(error) = decrypt_result {
            let _ = std::fs::remove_file(&tmp_path);
            if let Some(entry) = self.entries.get(rel_key).cloned() {
                if entry.decrypted_path.exists() {
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
        Ok(Some(CacheResolve {
            path: out_path,
            mode: CacheMode::FullDecrypt,
            warning: None,
        }))
    }

    fn cache_file_path(&self, rel_key: &str) -> PathBuf {
        let hash = format!("{:x}", md5::compute(rel_key.as_bytes()));
        self.cache_dir.join(format!("{hash}.db"))
    }

    fn load_persistent(&mut self) {
        let Ok(content) = std::fs::read_to_string(&self.mtime_file) else {
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
            if std::fs::write(&tmp, json).is_ok() {
                if self.mtime_file.exists() {
                    let _ = std::fs::remove_file(&self.mtime_file);
                }
                let _ = std::fs::rename(tmp, &self.mtime_file);
            }
        }
    }
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
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}
