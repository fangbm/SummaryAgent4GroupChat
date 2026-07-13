use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[cfg(target_os = "windows")]
mod windows;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyEntry {
    pub db_name: String,
    pub enc_key: String,
    pub salt: String,
}

pub fn scan_keys(db_dir: &Path) -> Result<Vec<KeyEntry>> {
    #[cfg(target_os = "windows")]
    {
        windows::scan_keys(db_dir)
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = db_dir;
        anyhow::bail!("当前平台暂只支持 Windows 微信内存密钥扫描")
    }
}

pub fn read_db_salt(path: &Path) -> Option<String> {
    let mut buf = [0u8; 16];
    let mut file = std::fs::File::open(path).ok()?;
    use std::io::Read;
    file.read_exact(&mut buf).ok()?;
    if &buf[..15] == b"SQLite format 3" {
        return None;
    }
    Some(hex_encode(&buf))
}

pub fn collect_db_salts(db_dir: &Path) -> Vec<(String, String)> {
    let mut result = Vec::new();
    collect_recursive(db_dir, db_dir, &mut result);
    result
}

fn collect_recursive(base: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_recursive(base, &path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("db") {
            if let Some(salt) = read_db_salt(&path) {
                if let Ok(rel) = path.strip_prefix(base) {
                    out.push((salt, rel.to_string_lossy().replace('\\', "/")));
                }
            }
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_header_has_no_salt() {
        let dir = std::env::temp_dir().join(format!("wxdb-salt-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("plain.db");
        let mut data = b"SQLite format 3\x00".to_vec();
        data.extend_from_slice(&[0; 32]);
        std::fs::write(&path, data).unwrap();
        assert!(read_db_salt(&path).is_none());
        let _ = std::fs::remove_dir_all(dir);
    }
}
