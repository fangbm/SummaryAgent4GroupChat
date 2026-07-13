use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub db_dirs: Vec<PathBuf>,
    pub cache_dir: PathBuf,
    pub keys_file: PathBuf,
    pub legacy_wx_cli_dir: PathBuf,
    pub explicit_db_dir: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreCandidate {
    pub db_dir: PathBuf,
    pub message_shards: usize,
    pub encrypted_databases: usize,
    pub latest_mtime: Option<u64>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreHealth {
    pub db_dir: PathBuf,
    pub message_shards: usize,
    pub encrypted_databases: usize,
    pub known_keys: usize,
    pub missing_message_keys: Vec<String>,
    pub latest_mtime: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    pub cache_dir: PathBuf,
    pub keys_file: PathBuf,
    pub candidates: Vec<StoreCandidate>,
    pub stores: Vec<StoreHealth>,
}

#[derive(Debug, Deserialize)]
struct WxdbConfigFile {
    #[serde(default)]
    db_dir: Option<PathBuf>,
    #[serde(default)]
    db_dirs: Vec<PathBuf>,
    #[serde(default)]
    cache_dir: Option<PathBuf>,
    #[serde(default)]
    keys_file: Option<PathBuf>,
}

impl RuntimeConfig {
    pub fn load() -> Self {
        let app_dir = app_dir();
        let legacy_wx_cli_dir = home_dir().join(".wx-cli");
        let file_config = read_config_file(&app_dir.join("config.json"));

        let cache_dir = env_path("WXDB_CACHE_DIR")
            .or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|config| config.cache_dir.clone())
            })
            .unwrap_or_else(|| app_dir.join("cache"));
        let keys_file = env_path("WXDB_KEYS_FILE")
            .or_else(|| {
                file_config
                    .as_ref()
                    .and_then(|config| config.keys_file.clone())
            })
            .unwrap_or_else(|| app_dir.join("keys.json"));

        let mut db_dirs = BTreeSet::new();
        let explicit_db_dir = std::env::var_os("WXDB_DB_DIR").is_some()
            || file_config
                .as_ref()
                .is_some_and(|config| config.db_dir.is_some() || !config.db_dirs.is_empty());
        for path in env_paths("WXDB_DB_DIR") {
            add_db_dir_candidate(&mut db_dirs, path);
        }
        if let Some(config) = &file_config {
            if let Some(path) = &config.db_dir {
                add_db_dir_candidate(&mut db_dirs, path.clone());
            }
            for path in &config.db_dirs {
                add_db_dir_candidate(&mut db_dirs, path.clone());
            }
        }
        if let Some((db_dir, _keys_file)) = legacy_config(&legacy_wx_cli_dir) {
            add_db_dir_candidate(&mut db_dirs, db_dir.clone());
            if let Some(parent) = db_dir.parent().and_then(Path::parent) {
                discover_under_xwechat_root(parent, &mut db_dirs);
            }
        }
        for root in default_xwechat_roots() {
            discover_under_xwechat_root(&root, &mut db_dirs);
        }

        let mut db_dirs: Vec<PathBuf> =
            db_dirs.into_iter().filter(|path| is_db_dir(path)).collect();
        db_dirs.sort_by(|a, b| store_score(b).cmp(&store_score(a)).then_with(|| a.cmp(b)));

        Self {
            db_dirs,
            cache_dir,
            keys_file,
            legacy_wx_cli_dir,
            explicit_db_dir,
        }
    }

    pub fn cache_dir_for(&self, db_dir: &Path) -> PathBuf {
        self.cache_dir.join(safe_dir_name(db_dir))
    }

    pub fn with_cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = cache_dir.into();
        self
    }

    pub fn with_db_dir(mut self, db_dir: impl Into<PathBuf>) -> Self {
        self.db_dirs = vec![db_dir.into()];
        self.explicit_db_dir = true;
        self
    }

    pub fn mtime_file_for(&self, db_dir: &Path) -> PathBuf {
        self.cache_dir_for(db_dir).join("_mtimes.json")
    }

    pub(crate) fn legacy_keys_file_for(&self, db_dir: &Path) -> Option<PathBuf> {
        let (legacy_db_dir, keys_file) = legacy_config(&self.legacy_wx_cli_dir)?;
        same_path(&legacy_db_dir, db_dir).then_some(keys_file)
    }

    pub fn candidates(&self) -> Vec<StoreCandidate> {
        self.db_dirs
            .iter()
            .map(|db_dir| StoreCandidate {
                db_dir: db_dir.clone(),
                message_shards: message_db_keys(db_dir).len(),
                encrypted_databases: encrypted_db_count(db_dir),
                latest_mtime: latest_mtime_secs(db_dir),
                source: "auto".to_string(),
            })
            .collect()
    }
}

pub fn doctor() -> Result<DoctorReport> {
    let config = RuntimeConfig::load();
    let candidates = config.candidates();
    let keyring = crate::keyring::load_keyring(&config.keys_file).unwrap_or_default();
    let stores = config
        .db_dirs
        .iter()
        .map(|db_dir| {
            let keys = crate::keyring::keys_for_db_dir(&keyring, db_dir);
            let message_keys = message_db_keys(db_dir);
            let missing_message_keys = message_keys
                .iter()
                .filter(|key| !keys.contains_key(*key))
                .cloned()
                .collect();
            StoreHealth {
                db_dir: db_dir.clone(),
                message_shards: message_keys.len(),
                encrypted_databases: encrypted_db_count(db_dir),
                known_keys: keys.len(),
                missing_message_keys,
                latest_mtime: latest_mtime_secs(db_dir),
            }
        })
        .collect();
    Ok(DoctorReport {
        cache_dir: config.cache_dir.clone(),
        keys_file: config.keys_file.clone(),
        candidates,
        stores,
    })
}

pub(crate) fn message_db_keys(db_dir: &Path) -> Vec<String> {
    let mut keys = Vec::new();
    let message_dir = db_dir.join("message");
    if let Ok(entries) = std::fs::read_dir(message_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if is_message_db_file(name) {
                keys.push(format!("message/{name}"));
            }
        }
    }
    keys.sort_by_key(|key| message_db_sort_key(key));
    keys
}

pub(crate) fn all_db_keys(db_dir: &Path) -> Vec<String> {
    let mut keys = Vec::new();
    collect_db_keys(db_dir, db_dir, &mut keys);
    keys.sort();
    keys
}

pub(crate) fn latest_mtime_secs(path: &Path) -> Option<u64> {
    let mut latest = None;
    collect_latest_mtime(path, &mut latest);
    latest.and_then(system_time_secs)
}

pub(crate) fn legacy_config(legacy_dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let config_path = legacy_dir.join("config.json");
    let content = std::fs::read_to_string(&config_path).ok()?;
    let raw: serde_json::Value = serde_json::from_str(&content).ok()?;
    let db_dir = raw.get("db_dir")?.as_str().map(PathBuf::from)?;
    let keys_file = raw
        .get("keys_file")
        .and_then(|value| value.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("all_keys.json"));
    let keys_file = if keys_file.is_absolute() {
        keys_file
    } else {
        legacy_dir.join(keys_file)
    };
    Some((db_dir, keys_file))
}

pub(crate) fn app_dir() -> PathBuf {
    env_path("WXDB_HOME").unwrap_or_else(|| home_dir().join(".wx-summary-agent").join("wxdb"))
}

fn home_dir() -> PathBuf {
    dirs::home_dir().unwrap_or_else(|| PathBuf::from("."))
}

fn read_config_file(path: &Path) -> Option<WxdbConfigFile> {
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn env_paths(name: &str) -> Vec<PathBuf> {
    let Some(value) = std::env::var_os(name) else {
        return Vec::new();
    };
    std::env::split_paths(&value).collect()
}

fn add_db_dir_candidate(out: &mut BTreeSet<PathBuf>, path: PathBuf) {
    if is_db_dir(&path) {
        out.insert(normalize_path(path));
        return;
    }
    discover_under_xwechat_root(&path, out);
}

fn discover_under_xwechat_root(root: &Path, out: &mut BTreeSet<PathBuf>) {
    if !root.is_dir() {
        return;
    }
    if is_db_dir(root) {
        out.insert(normalize_path(root.to_path_buf()));
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if is_db_dir(&path) {
            out.insert(normalize_path(path));
            continue;
        }
        let storage = path.join("db_storage");
        if is_db_dir(&storage) {
            out.insert(normalize_path(storage));
        }
    }
}

fn default_xwechat_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let home = home_dir();
    roots.push(home.join("Documents").join("xwechat_files"));
    if let Some(temp) = std::env::var_os("TEMP").map(PathBuf::from) {
        roots.push(temp.join("xwechat_files"));
        if let Some(parent) = temp.parent() {
            roots.push(parent.join("xwechat_files"));
        }
    }
    if let Some(local) = std::env::var_os("LOCALAPPDATA").map(PathBuf::from) {
        roots.push(local.join("Tencent").join("xwechat_files"));
    }
    if let Some(appdata) = std::env::var_os("APPDATA").map(PathBuf::from) {
        roots.push(appdata.join("Tencent").join("xwechat"));
    }
    roots
}

fn is_db_dir(path: &Path) -> bool {
    path.join("message").is_dir()
        && (path.join("contact").is_dir() || path.join("session").is_dir())
        && !message_db_keys(path).is_empty()
}

fn store_score(path: &Path) -> (Option<u64>, usize) {
    (latest_mtime_secs(path), message_db_keys(path).len())
}

fn encrypted_db_count(db_dir: &Path) -> usize {
    all_db_keys(db_dir)
        .into_iter()
        .filter(|key| crate::scanner::read_db_salt(&db_dir.join(rel_to_path(key))).is_some())
        .count()
}

fn collect_db_keys(base: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_db_keys(base, &path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("db") {
            if let Ok(rel) = path.strip_prefix(base) {
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
}

fn collect_latest_mtime(path: &Path, latest: &mut Option<SystemTime>) {
    let Ok(metadata) = std::fs::metadata(path) else {
        return;
    };
    if let Ok(modified) = metadata.modified() {
        if latest.map(|current| modified > current).unwrap_or(true) {
            *latest = Some(modified);
        }
    }
    if !metadata.is_dir() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        collect_latest_mtime(&entry.path(), latest);
    }
}

fn system_time_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs())
}

fn is_message_db_file(name: &str) -> bool {
    name.starts_with("message_") && name.ends_with(".db") && {
        let middle = &name["message_".len()..name.len() - ".db".len()];
        middle.chars().all(|ch| ch.is_ascii_digit())
    }
}

fn message_db_sort_key(key: &str) -> (u32, String) {
    let number = key
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_prefix("message_"))
        .and_then(|name| name.strip_suffix(".db"))
        .and_then(|n| n.parse::<u32>().ok())
        .unwrap_or(u32::MAX);
    (number, key.to_string())
}

pub(crate) fn rel_to_path(rel_key: &str) -> PathBuf {
    rel_key
        .split('/')
        .fold(PathBuf::new(), |path, part| path.join(part))
}

fn safe_dir_name(path: &Path) -> String {
    format!("{:x}", md5::compute(path.to_string_lossy().as_bytes()))
}

pub(crate) fn normalize_path(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

pub(crate) fn same_path(a: &Path, b: &Path) -> bool {
    normalize_path(a.to_path_buf())
        .to_string_lossy()
        .eq_ignore_ascii_case(&normalize_path(b.to_path_buf()).to_string_lossy())
}

pub(crate) fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("创建目录失败: {}", parent.display()))?;
    }
    Ok(())
}

pub(crate) fn flatten_key_map(value: &serde_json::Value) -> HashMap<String, String> {
    let mut keys = HashMap::new();
    let Some(object) = value.as_object() else {
        return keys;
    };
    for (rel, item) in object {
        let enc_key = item.as_str().map(ToOwned::to_owned).or_else(|| {
            item.get("enc_key")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
        });
        if let Some(enc_key) = enc_key.filter(|key| key.len() == 64) {
            keys.insert(rel.replace('\\', "/"), enc_key.to_ascii_lowercase());
        }
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_db_keys_are_sorted_numerically() {
        assert_eq!(
            vec![
                "message/message_0.db".to_string(),
                "message/message_2.db".to_string(),
                "message/message_10.db".to_string()
            ]
            .into_iter()
            .map(|key| message_db_sort_key(&key))
            .collect::<Vec<_>>()
            .len(),
            3
        );
        assert!(
            message_db_sort_key("message/message_2.db")
                < message_db_sort_key("message/message_10.db")
        );
    }

    #[test]
    fn flatten_key_map_accepts_legacy_shape() {
        let value = serde_json::json!({
            "message/message_0.db": {"enc_key": "a".repeat(64), "salt": "b".repeat(32)},
            "contact/contact.db": "c".repeat(64),
            "bad.db": {"enc_key": "short"}
        });
        let keys = flatten_key_map(&value);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains_key("message/message_0.db"));
        assert!(keys.contains_key("contact/contact.db"));
    }
}
