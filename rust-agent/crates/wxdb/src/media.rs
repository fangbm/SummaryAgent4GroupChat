use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant, UNIX_EPOCH},
};

const V1_MAGIC: &[u8; 6] = b"\x07\x08V1\x08\x07";
const V2_MAGIC: &[u8; 6] = b"\x07\x08V2\x08\x07";
const PACKED_HEADER_LEN: usize = 15;
const CACHEABLE_MEDIA_FORMATS: &[&str] = &[
    "jpg", "png", "gif", "webp", "bmp", "hevc", "tif", "mp4", "mov", "mkv", "webm", "m4v",
];
const V2_KEY_FAILURE_CACHE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct DecodedMedia {
    pub path: PathBuf,
    pub format: &'static str,
    pub decoder: &'static str,
}

#[derive(Debug, Clone)]
struct DecodedBytes {
    data: Vec<u8>,
    format: &'static str,
    decoder: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct V2KeyMaterial {
    aes_key: [u8; 16],
    xor_key: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredV2KeyMaterial {
    account_root: String,
    aes_key: String,
    xor_key: u8,
}

#[derive(Debug, Clone)]
struct V2KeyFailure {
    error: String,
    at: Instant,
}

pub fn decode_media_to_cache(
    dat_path: &Path,
    account_root: Option<&Path>,
    cache_dir: &Path,
) -> Result<DecodedMedia> {
    let cache_key = media_cache_key(dat_path);
    if let Some(cached) = cached_decoded_media(cache_dir, &cache_key) {
        return Ok(cached);
    }

    let bytes =
        fs::read(dat_path).with_context(|| format!("读取媒体文件 {}", dat_path.display()))?;
    let decoded = decode_media_bytes(&bytes, account_root, Some(cache_dir))?;
    if decoded.format == "bin" {
        bail!("解码后不是可识别媒体格式");
    }

    fs::create_dir_all(cache_dir)
        .with_context(|| format!("创建媒体缓存目录 {}", cache_dir.display()))?;
    let output_path = cache_dir.join(format!("{}.{}", cache_key, decoded.format));
    if !output_path.exists() {
        fs::write(&output_path, &decoded.data)
            .with_context(|| format!("写入解码媒体 {}", output_path.display()))?;
    }

    Ok(DecodedMedia {
        path: output_path,
        format: decoded.format,
        decoder: decoded.decoder,
    })
}

pub fn decode_voice_to_cache(
    voice_path: &Path,
    account_root: Option<&Path>,
    cache_dir: &Path,
) -> Result<DecodedMedia> {
    let cache_key = media_cache_key(voice_path);
    if let Some(cached) = cached_decoded_media(cache_dir, &cache_key) {
        return Ok(cached);
    }

    let bytes =
        fs::read(voice_path).with_context(|| format!("读取语音文件 {}", voice_path.display()))?;
    let decoded = decode_voice_bytes(&bytes, account_root, Some(cache_dir))?;
    if decoded.format == "bin" {
        bail!("解码后不是可识别语音格式");
    }

    fs::create_dir_all(cache_dir)
        .with_context(|| format!("创建媒体缓存目录 {}", cache_dir.display()))?;
    let output_path = cache_dir.join(format!("{}.{}", cache_key, decoded.format));
    if !output_path.exists() {
        fs::write(&output_path, &decoded.data)
            .with_context(|| format!("写入解码语音 {}", output_path.display()))?;
    }

    Ok(DecodedMedia {
        path: output_path,
        format: decoded.format,
        decoder: decoded.decoder,
    })
}

fn decode_media_bytes(
    bytes: &[u8],
    account_root: Option<&Path>,
    cache_dir: Option<&Path>,
) -> Result<DecodedBytes> {
    if bytes.is_empty() {
        bail!("空媒体文件");
    }
    if let Some(format) = detect_image_format(bytes).or_else(|| detect_video_format(bytes)) {
        return Ok(DecodedBytes {
            data: bytes.to_vec(),
            format,
            decoder: "plain",
        });
    }
    if bytes.starts_with(V1_MAGIC) {
        let key = *b"cfcd208495d565ef";
        return decode_packed_aes_xor(bytes, key, 0x88, "v1_aes");
    }
    if bytes.starts_with(V2_MAGIC) {
        let account_root = account_root.ok_or_else(|| anyhow!("V2 图片缺少账号根目录"))?;
        let key = image_v2_key(account_root, cache_dir)?;
        return decode_packed_aes_xor(bytes, key.aes_key, key.xor_key, "v2_aes");
    }
    decode_legacy_xor(bytes)
}

fn decode_voice_bytes(
    bytes: &[u8],
    account_root: Option<&Path>,
    cache_dir: Option<&Path>,
) -> Result<DecodedBytes> {
    if bytes.is_empty() {
        bail!("空语音文件");
    }
    if let Some(format) = detect_audio_format(bytes) {
        return Ok(DecodedBytes {
            data: bytes.to_vec(),
            format,
            decoder: "plain",
        });
    }
    if bytes.starts_with(V1_MAGIC) {
        let key = *b"cfcd208495d565ef";
        return decode_packed_audio_aes_xor(bytes, key, 0x88, "v1_aes");
    }
    if bytes.starts_with(V2_MAGIC) {
        let account_root = account_root.ok_or_else(|| anyhow!("V2 语音缺少账号根目录"))?;
        let key = image_v2_key(account_root, cache_dir)?;
        return decode_packed_audio_aes_xor(bytes, key.aes_key, key.xor_key, "v2_aes");
    }
    decode_legacy_audio_xor(bytes)
}

fn decode_packed_aes_xor(
    bytes: &[u8],
    aes_key: [u8; 16],
    xor_key: u8,
    decoder: &'static str,
) -> Result<DecodedBytes> {
    if bytes.len() < PACKED_HEADER_LEN {
        bail!("packed 图片文件过短: {}", bytes.len());
    }
    let aes_size = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
    let xor_size = u32::from_le_bytes(bytes[10..14].try_into().unwrap()) as usize;
    let aligned_aes_size = aes_size + (16 - (aes_size % 16));
    let aes_end = PACKED_HEADER_LEN
        .checked_add(aligned_aes_size)
        .ok_or_else(|| anyhow!("AES 段长度溢出"))?;
    let raw_end = bytes
        .len()
        .checked_sub(xor_size)
        .ok_or_else(|| anyhow!("XOR 段长度溢出"))?;
    if aes_end > bytes.len() || aes_end > raw_end {
        bail!(
            "packed 图片长度不合法: aes_size={} aligned={} xor_size={} file_len={}",
            aes_size,
            aligned_aes_size,
            xor_size,
            bytes.len()
        );
    }

    let mut data = aes128_ecb_decrypt_pkcs7(&aes_key, &bytes[PACKED_HEADER_LEN..aes_end])?;
    data.extend_from_slice(&bytes[aes_end..raw_end]);
    data.extend(bytes[raw_end..].iter().map(|byte| byte ^ xor_key));
    let format = detect_image_format(&data)
        .or_else(|| detect_video_format(&data))
        .unwrap_or("bin");
    if format == "bin" {
        bail!("{decoder}: 解密成功但媒体 magic 不识别，可能 media key 不匹配");
    }
    Ok(DecodedBytes {
        data,
        format,
        decoder,
    })
}

fn decode_packed_audio_aes_xor(
    bytes: &[u8],
    aes_key: [u8; 16],
    xor_key: u8,
    decoder: &'static str,
) -> Result<DecodedBytes> {
    if bytes.len() < PACKED_HEADER_LEN {
        bail!("packed 语音文件过短: {}", bytes.len());
    }
    let aes_size = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
    let xor_size = u32::from_le_bytes(bytes[10..14].try_into().unwrap()) as usize;
    let aligned_aes_size = aes_size + (16 - (aes_size % 16));
    let aes_end = PACKED_HEADER_LEN
        .checked_add(aligned_aes_size)
        .ok_or_else(|| anyhow!("AES 段长度溢出"))?;
    let raw_end = bytes
        .len()
        .checked_sub(xor_size)
        .ok_or_else(|| anyhow!("XOR 段长度溢出"))?;
    if aes_end > bytes.len() || aes_end > raw_end {
        bail!(
            "packed 语音长度不合法: aes_size={} aligned={} xor_size={} file_len={}",
            aes_size,
            aligned_aes_size,
            xor_size,
            bytes.len()
        );
    }

    let mut data = aes128_ecb_decrypt_pkcs7(&aes_key, &bytes[PACKED_HEADER_LEN..aes_end])?;
    data.extend_from_slice(&bytes[aes_end..raw_end]);
    data.extend(bytes[raw_end..].iter().map(|byte| byte ^ xor_key));
    let format = detect_audio_format(&data).unwrap_or("bin");
    if format == "bin" {
        bail!("{decoder}: 解密成功但语音 magic 不识别，可能 media key 不匹配");
    }
    Ok(DecodedBytes {
        data,
        format,
        decoder,
    })
}

fn aes128_ecb_decrypt_pkcs7(key: &[u8; 16], cipher: &[u8]) -> Result<Vec<u8>> {
    use aes::cipher::{generic_array::GenericArray, BlockDecrypt, KeyInit};

    if cipher.is_empty() || !cipher.len().is_multiple_of(16) {
        bail!("AES 输入长度不是 16 的倍数: {}", cipher.len());
    }
    let aes = aes::Aes128::new(key.into());
    let mut out = Vec::with_capacity(cipher.len());
    for chunk in cipher.chunks_exact(16) {
        let mut block = GenericArray::clone_from_slice(chunk);
        aes.decrypt_block(&mut block);
        out.extend_from_slice(&block);
    }

    let pad = *out.last().ok_or_else(|| anyhow!("AES 输出为空"))? as usize;
    if pad == 0 || pad > 16 || pad > out.len() {
        bail!("AES PKCS7 padding 长度非法: {pad}");
    }
    if !out[out.len() - pad..]
        .iter()
        .all(|byte| *byte as usize == pad)
    {
        bail!("AES PKCS7 padding 字节不一致");
    }
    out.truncate(out.len() - pad);
    Ok(out)
}

fn decode_legacy_xor(bytes: &[u8]) -> Result<DecodedBytes> {
    let key = detect_legacy_xor_key(bytes).ok_or_else(|| anyhow!("legacy XOR key 探测失败"))?;
    let data = bytes.iter().map(|byte| byte ^ key).collect::<Vec<_>>();
    let format = detect_image_format(&data)
        .or_else(|| detect_video_format(&data))
        .unwrap_or("bin");
    if format == "bin" {
        bail!("legacy XOR key=0x{key:02x} 解码后媒体 magic 不识别");
    }
    Ok(DecodedBytes {
        data,
        format,
        decoder: "legacy_xor",
    })
}

fn decode_legacy_audio_xor(bytes: &[u8]) -> Result<DecodedBytes> {
    let key = detect_legacy_audio_xor_key(bytes)
        .ok_or_else(|| anyhow!("legacy audio XOR key 探测失败"))?;
    let data = bytes.iter().map(|byte| byte ^ key).collect::<Vec<_>>();
    let format = detect_audio_format(&data).unwrap_or("bin");
    if format == "bin" {
        bail!("legacy audio XOR key=0x{key:02x} 解码后语音 magic 不识别");
    }
    Ok(DecodedBytes {
        data,
        format,
        decoder: "legacy_xor",
    })
}

fn detect_legacy_audio_xor_key(bytes: &[u8]) -> Option<u8> {
    let header = &bytes[..bytes.len().min(16)];
    for magic in [
        b"#!SI" as &[u8],
        b"\x02#!S",
        b"#!AM",
        b"ID3",
        &[0xff, 0xfb],
        &[0xff, 0xf3],
        &[0xff, 0xf2],
        b"OggS",
        b"fLaC",
        b"RIFF",
        &[0xff, 0xf1],
        &[0xff, 0xf9],
    ] {
        if let Some(key) = xor_key_for_magic(header, magic) {
            return Some(key);
        }
    }
    None
}

fn detect_legacy_xor_key(bytes: &[u8]) -> Option<u8> {
    let header = &bytes[..bytes.len().min(16)];
    for magic in [
        &[0x89, 0x50, 0x4e, 0x47][..],
        b"GIF8",
        &[0x49, 0x49, 0x2a, 0x00],
        b"RIFF",
        &[0xff, 0xd8, 0xff],
    ] {
        if let Some(key) = xor_key_for_magic(header, magic) {
            return Some(key);
        }
    }
    let key = xor_key_for_magic(header, b"BM")?;
    if header.len() < 14 {
        return None;
    }
    let decoded = header.iter().map(|byte| byte ^ key).collect::<Vec<_>>();
    let bmp_size = u32::from_le_bytes(decoded[2..6].try_into().ok()?);
    let bmp_offset = u32::from_le_bytes(decoded[10..14].try_into().ok()?);
    let file_size = bytes.len() as u32;
    (file_size.abs_diff(bmp_size) < 1024 && (14..=1078).contains(&bmp_offset)).then_some(key)
}

fn xor_key_for_magic(header: &[u8], magic: &[u8]) -> Option<u8> {
    if header.len() < magic.len() {
        return None;
    }
    let key = header[0] ^ magic[0];
    magic
        .iter()
        .enumerate()
        .all(|(index, expected)| header[index] ^ key == *expected)
        .then_some(key)
}

pub fn detect_image_format(bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 4 && &bytes[..4] == b"wxgf" {
        return Some("hevc");
    }
    if bytes.len() >= 3 && bytes[..3] == [0xff, 0xd8, 0xff] {
        return Some("jpg");
    }
    if bytes.len() >= 4 && bytes[..4] == [0x89, 0x50, 0x4e, 0x47] {
        return Some("png");
    }
    if bytes.len() >= 3 && &bytes[..3] == b"GIF" {
        return Some("gif");
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some("webp");
    }
    if bytes.len() >= 4 && bytes[..4] == [0x49, 0x49, 0x2a, 0x00] {
        return Some("tif");
    }
    if bytes.len() >= 2 && &bytes[..2] == b"BM" {
        return Some("bmp");
    }
    None
}

pub fn detect_audio_format(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"#!SILK") || bytes.starts_with(b"\x02#!SILK") {
        return Some("silk");
    }
    if bytes.starts_with(b"#!AMR") {
        return Some("amr");
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        return Some("wav");
    }
    if bytes.len() >= 2 && bytes[0] == 0xff && matches!(bytes[1], 0xf1 | 0xf9) {
        return Some("aac");
    }
    if bytes.starts_with(b"ID3")
        || bytes.len() >= 2 && bytes[0] == 0xff && matches!(bytes[1], 0xfb | 0xf3 | 0xf2)
    {
        return Some("mp3");
    }
    if bytes.starts_with(b"OggS") {
        return Some("ogg");
    }
    if bytes.starts_with(b"fLaC") {
        return Some("flac");
    }
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        return Some("m4a");
    }
    None
}

pub fn detect_video_format(bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        let brand = &bytes[8..12];
        if brand.starts_with(b"qt") {
            return Some("mov");
        }
        return Some("mp4");
    }
    if bytes.len() >= 4 && bytes[..4] == [0x1a, 0x45, 0xdf, 0xa3] {
        return Some("mkv");
    }
    None
}

fn media_cache_key(path: &Path) -> String {
    let metadata = path.metadata().ok();
    let len = metadata
        .as_ref()
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let modified = metadata
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let input = format!("{}:{len}:{modified}", path.display());
    format!("{:x}", md5::compute(input.as_bytes()))
}

fn cached_decoded_media(cache_dir: &Path, cache_key: &str) -> Option<DecodedMedia> {
    for &format in CACHEABLE_MEDIA_FORMATS {
        let path = cache_dir.join(format!("{cache_key}.{format}"));
        if path.is_file() {
            return Some(DecodedMedia {
                path,
                format,
                decoder: "cache",
            });
        }
    }
    None
}

fn image_v2_key(account_root: &Path, cache_dir: Option<&Path>) -> Result<V2KeyMaterial> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, V2KeyMaterial>>> = OnceLock::new();
    static FAILURES: OnceLock<Mutex<HashMap<PathBuf, V2KeyFailure>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let failures = FAILURES.get_or_init(|| Mutex::new(HashMap::new()));
    let key_path = account_root.to_path_buf();
    if let Some(key) = cache.lock().unwrap().get(&key_path).copied() {
        return Ok(key);
    }
    if let Some(error) = cached_v2_key_failure(failures, &key_path) {
        return Err(anyhow!(error));
    }
    if let Some(cache_dir) = cache_dir {
        if let Some(key) = load_cached_v2_key(cache_dir, account_root) {
            failures.lock().unwrap().remove(&key_path);
            cache.lock().unwrap().insert(key_path, key);
            return Ok(key);
        }
    }
    let attach_root = account_root.join("msg").join("attach");
    let key = match platform_image_v2_key(&attach_root) {
        Ok(key) => key,
        Err(error) => {
            failures.lock().unwrap().insert(
                key_path,
                V2KeyFailure {
                    error: error.to_string(),
                    at: Instant::now(),
                },
            );
            return Err(error);
        }
    };
    if let Some(cache_dir) = cache_dir {
        store_cached_v2_key(cache_dir, account_root, key);
    }
    failures.lock().unwrap().remove(&key_path);
    cache.lock().unwrap().insert(key_path, key);
    Ok(key)
}

fn cached_v2_key_failure(
    failures: &Mutex<HashMap<PathBuf, V2KeyFailure>>,
    key_path: &Path,
) -> Option<String> {
    let mut failures = failures.lock().unwrap();
    match failures.get(key_path) {
        Some(failure) if failure.at.elapsed() < V2_KEY_FAILURE_CACHE_TTL => {
            Some(failure.error.clone())
        }
        Some(_) => {
            failures.remove(key_path);
            None
        }
        None => None,
    }
}

fn load_cached_v2_key(cache_dir: &Path, account_root: &Path) -> Option<V2KeyMaterial> {
    let path = v2_key_cache_path(cache_dir);
    let raw = fs::read_to_string(path).ok()?;
    let stored: StoredV2KeyMaterial = serde_json::from_str(&raw).ok()?;
    if stored.account_root != account_root.display().to_string() {
        return None;
    }
    Some(V2KeyMaterial {
        aes_key: parse_hex_16(&stored.aes_key)?,
        xor_key: stored.xor_key,
    })
}

fn store_cached_v2_key(cache_dir: &Path, account_root: &Path, key: V2KeyMaterial) {
    let _ = fs::create_dir_all(cache_dir);
    let stored = StoredV2KeyMaterial {
        account_root: account_root.display().to_string(),
        aes_key: hex_bytes(&key.aes_key),
        xor_key: key.xor_key,
    };
    if let Ok(raw) = serde_json::to_string_pretty(&stored) {
        let _ = fs::write(v2_key_cache_path(cache_dir), raw);
    }
}

fn v2_key_cache_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("image-v2-key.json")
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hex_16(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).ok()?;
        out[index] = u8::from_str_radix(text, 16).ok()?;
    }
    Some(out)
}

#[cfg(target_os = "windows")]
fn platform_image_v2_key(attach_root: &Path) -> Result<V2KeyMaterial> {
    windows_image_v2_key(attach_root)
}

#[cfg(not(target_os = "windows"))]
fn platform_image_v2_key(_attach_root: &Path) -> Result<V2KeyMaterial> {
    bail!("V2 图片解密当前只支持 Windows 内存 key 扫描")
}

#[cfg(target_os = "windows")]
fn windows_image_v2_key(attach_root: &Path) -> Result<V2KeyMaterial> {
    let templates = v2_template_ciphertexts(attach_root, 3, 1024)?;
    if templates.is_empty() {
        bail!("找不到可验证的 V2 图片模板文件");
    }
    let xor_key = derive_v2_xor_key(attach_root, 10, 3)?.unwrap_or(0x88);
    let pid = find_wechat_pid().context("找不到 Weixin.exe/WeChat.exe 进程")?;
    let aes_key = scan_wechat_process_for_v2_key(pid, &templates)?;
    Ok(V2KeyMaterial { aes_key, xor_key })
}

#[cfg(target_os = "windows")]
fn v2_template_ciphertexts(
    attach_root: &Path,
    max_templates: usize,
    max_files: usize,
) -> Result<Vec<[u8; 16]>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    visit_files(attach_root, max_files, &mut |path| {
        if out.len() >= max_templates {
            return Ok(true);
        }
        let bytes = fs::read(path)?;
        if bytes.len() >= 31 && bytes.starts_with(V2_MAGIC) {
            let block: [u8; 16] = bytes[15..31].try_into().unwrap();
            if seen.insert(block) {
                out.push(block);
            }
        }
        Ok(out.len() >= max_templates)
    })?;
    Ok(out)
}

#[cfg(target_os = "windows")]
fn derive_v2_xor_key(attach_root: &Path, sample: usize, min_samples: usize) -> Result<Option<u8>> {
    let mut votes = Vec::new();
    visit_files(attach_root, 256, &mut |path| {
        if votes.len() >= sample {
            return Ok(true);
        }
        let bytes = fs::read(path)?;
        if bytes.len() >= 32 && bytes.starts_with(V2_MAGIC) {
            if let Some(last) = bytes.last() {
                votes.push(last ^ 0xd9);
            }
        }
        Ok(votes.len() >= sample)
    })?;
    if votes.len() < min_samples {
        return Ok(None);
    }
    let mut counts = [0usize; 256];
    for vote in votes {
        counts[vote as usize] += 1;
    }
    Ok(counts
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| *count)
        .map(|(key, _)| key as u8))
}

#[cfg(target_os = "windows")]
fn visit_files<F>(root: &Path, max_files: usize, f: &mut F) -> Result<bool>
where
    F: FnMut(&Path) -> Result<bool>,
{
    if !root.is_dir() || max_files == 0 {
        return Ok(false);
    }
    let mut stack = vec![root.to_path_buf()];
    let mut examined = 0usize;
    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir)
            .with_context(|| format!("读取目录 {}", dir.display()))?
            .flatten()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if !path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("dat"))
            {
                continue;
            }
            examined += 1;
            if f(&path)? || examined >= max_files {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[cfg(target_os = "windows")]
fn decrypt_template_block(aes_key: &[u8; 16], ciphertext: &[u8; 16]) -> Option<&'static str> {
    use aes::cipher::{generic_array::GenericArray, BlockDecrypt, KeyInit};

    let aes = aes::Aes128::new(aes_key.into());
    let mut block = GenericArray::clone_from_slice(ciphertext);
    aes.decrypt_block(&mut block);
    detect_image_format(&block)
}

#[cfg(target_os = "windows")]
fn verify_v2_key(aes_key: &[u8; 16], templates: &[[u8; 16]]) -> bool {
    !templates.is_empty()
        && templates
            .iter()
            .all(|template| decrypt_template_block(aes_key, template).is_some())
}

#[cfg(target_os = "windows")]
fn find_wechat_pid() -> Option<u32> {
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32First, Process32Next, PROCESSENTRY32, TH32CS_SNAPPROCESS,
    };

    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0).ok()? };
    let mut entry = PROCESSENTRY32 {
        dwSize: std::mem::size_of::<PROCESSENTRY32>() as u32,
        ..Default::default()
    };
    unsafe {
        if Process32First(snapshot, &mut entry).is_err() {
            let _ = CloseHandle(snapshot);
            return None;
        }
        loop {
            let name = std::ffi::CStr::from_ptr(entry.szExeFile.as_ptr()).to_string_lossy();
            if name.eq_ignore_ascii_case("Weixin.exe") || name.eq_ignore_ascii_case("WeChat.exe") {
                let pid = entry.th32ProcessID;
                let _ = CloseHandle(snapshot);
                return Some(pid);
            }
            if Process32Next(snapshot, &mut entry).is_err() {
                break;
            }
        }
        let _ = CloseHandle(snapshot);
    }
    None
}

#[cfg(target_os = "windows")]
fn scan_wechat_process_for_v2_key(pid: u32, templates: &[[u8; 16]]) -> Result<[u8; 16]> {
    use windows::Win32::Foundation::CloseHandle;
    use windows::Win32::System::Diagnostics::Debug::ReadProcessMemory;
    use windows::Win32::System::Memory::{
        VirtualQueryEx, MEMORY_BASIC_INFORMATION, MEM_COMMIT, PAGE_EXECUTE_READWRITE,
        PAGE_EXECUTE_WRITECOPY, PAGE_GUARD, PAGE_NOACCESS, PAGE_NOCACHE, PAGE_READWRITE,
        PAGE_WRITECOMBINE, PAGE_WRITECOPY,
    };
    use windows::Win32::System::Threading::{
        OpenProcess, PROCESS_QUERY_INFORMATION, PROCESS_VM_READ,
    };

    const CHUNK_SIZE: usize = 2 * 1024 * 1024;
    const MAX_REGION_SIZE: usize = 50 * 1024 * 1024;
    const OVERLAP: usize = 31;

    let process = unsafe {
        OpenProcess(PROCESS_VM_READ | PROCESS_QUERY_INFORMATION, false, pid)
            .context("OpenProcess 失败，请以管理员权限运行")?
    };
    let mut seen = HashSet::<[u8; 16]>::new();
    let mut address = 0usize;
    let mut found = None;

    loop {
        let mut mbi = MEMORY_BASIC_INFORMATION::default();
        let ret = unsafe {
            VirtualQueryEx(
                process,
                Some(address as *const _),
                &mut mbi,
                std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
            )
        };
        if ret == 0 {
            break;
        }
        let base = mbi.BaseAddress as usize;
        let size = mbi.RegionSize;
        let protect = mbi.Protect.0;
        let base_protect = protect & !(PAGE_GUARD.0 | PAGE_NOCACHE.0 | PAGE_WRITECOMBINE.0);
        let readable = protect != PAGE_NOACCESS.0
            && (protect & PAGE_GUARD.0) == 0
            && matches!(
                base_protect,
                value if value == PAGE_READWRITE.0
                    || value == PAGE_WRITECOPY.0
                    || value == PAGE_EXECUTE_READWRITE.0
                    || value == PAGE_EXECUTE_WRITECOPY.0
            );
        if mbi.State == MEM_COMMIT && readable && size <= MAX_REGION_SIZE {
            let mut offset = 0usize;
            while offset < size {
                let chunk_size = CHUNK_SIZE.min(size - offset);
                let addr = base + offset;
                let mut buf = vec![0u8; chunk_size];
                let mut bytes_read = 0usize;
                let ok = unsafe {
                    ReadProcessMemory(
                        process,
                        addr as *const _,
                        buf.as_mut_ptr() as *mut _,
                        chunk_size,
                        Some(&mut bytes_read),
                    )
                    .is_ok()
                };
                if ok && bytes_read > 0 {
                    buf.truncate(bytes_read);
                    if let Some(key) = scan_buffer_for_v2_key(&buf, templates, &mut seen) {
                        found = Some(key);
                        break;
                    }
                }
                offset += if chunk_size > OVERLAP {
                    chunk_size - OVERLAP
                } else {
                    chunk_size
                };
            }
        }
        if found.is_some() {
            break;
        }
        address = base.saturating_add(size);
        if address == 0 {
            break;
        }
    }
    unsafe {
        let _ = CloseHandle(process);
    }
    found.ok_or_else(|| anyhow!("微信进程内存里没有找到可验证的 V2 图片 AES key"))
}

#[cfg(target_os = "windows")]
fn scan_buffer_for_v2_key(
    buf: &[u8],
    templates: &[[u8; 16]],
    seen: &mut HashSet<[u8; 16]>,
) -> Option<[u8; 16]> {
    for candidate in ascii_alnum_runs(buf, 32) {
        let mut key = [0u8; 16];
        key.copy_from_slice(&candidate[..16]);
        if seen.insert(key) && verify_v2_key(&key, templates) {
            return Some(key);
        }
    }
    for candidate in ascii_alnum_runs(buf, 16) {
        let mut key = [0u8; 16];
        key.copy_from_slice(candidate);
        if seen.insert(key) && verify_v2_key(&key, templates) {
            return Some(key);
        }
    }
    None
}

#[cfg(target_os = "windows")]
fn ascii_alnum_runs(buf: &[u8], len: usize) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut index = 0usize;
    while index < buf.len() {
        if !buf[index].is_ascii_alphanumeric() {
            index += 1;
            continue;
        }
        let start = index;
        while index < buf.len() && buf[index].is_ascii_alphanumeric() {
            index += 1;
        }
        if index - start == len {
            out.push(&buf[start..index]);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xor_encrypt(plain: &[u8], key: u8) -> Vec<u8> {
        plain.iter().map(|byte| byte ^ key).collect()
    }

    #[test]
    fn detects_common_image_formats() {
        assert_eq!(detect_image_format(&[0xff, 0xd8, 0xff, 0xe0]), Some("jpg"));
        assert_eq!(detect_image_format(&[0x89, 0x50, 0x4e, 0x47]), Some("png"));
        assert_eq!(detect_image_format(b"GIF89a"), Some("gif"));
        assert_eq!(detect_image_format(b"RIFFxxxxWEBP"), Some("webp"));
        assert_eq!(detect_image_format(b"BMxxxx"), Some("bmp"));
        assert_eq!(detect_image_format(b"xxxx"), None);
    }

    #[test]
    fn detects_common_audio_formats() {
        assert_eq!(detect_audio_format(b"#!SILK_V3"), Some("silk"));
        assert_eq!(detect_audio_format(b"\x02#!SILK_V3"), Some("silk"));
        assert_eq!(detect_audio_format(b"#!AMR\n"), Some("amr"));
        assert_eq!(detect_audio_format(b"RIFFxxxxWAVE"), Some("wav"));
        assert_eq!(detect_audio_format(b"ID3xxxx"), Some("mp3"));
        assert_eq!(detect_audio_format(b"OggSxxxx"), Some("ogg"));
        assert_eq!(detect_audio_format(b"fLaCxxxx"), Some("flac"));
        assert_eq!(detect_audio_format(b"xxxxftypM4A "), Some("m4a"));
        assert_eq!(detect_audio_format(&[0xff, 0xf1, 0, 0]), Some("aac"));
        assert_eq!(detect_audio_format(b"xxxx"), None);
    }

    #[test]
    fn detects_common_video_formats() {
        assert_eq!(
            detect_video_format(b"\x00\x00\x00\x18ftypmp42"),
            Some("mp4")
        );
        assert_eq!(
            detect_video_format(b"\x00\x00\x00\x18ftypqt  "),
            Some("mov")
        );
        assert_eq!(
            detect_video_format(&[0x1a, 0x45, 0xdf, 0xa3, 0, 0, 0, 0]),
            Some("mkv")
        );
        assert_eq!(detect_video_format(b"xxxx"), None);
    }

    #[test]
    fn legacy_xor_decodes_jpg() {
        let plain = [0xff, 0xd8, 0xff, 0xe0, 0, 1, 2, 3];
        let encoded = xor_encrypt(&plain, 0xab);
        let decoded = decode_legacy_xor(&encoded).unwrap();

        assert_eq!(decoded.format, "jpg");
        assert_eq!(decoded.decoder, "legacy_xor");
        assert_eq!(decoded.data, plain);
    }

    #[test]
    fn legacy_xor_decodes_silk_voice() {
        let plain = b"#!SILK_V3 test voice bytes";
        let encoded = xor_encrypt(plain, 0x5a);
        let decoded = decode_legacy_audio_xor(&encoded).unwrap();

        assert_eq!(decoded.format, "silk");
        assert_eq!(decoded.decoder, "legacy_xor");
        assert_eq!(decoded.data, plain);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn ascii_alnum_run_detection_requires_exact_length() {
        let buf = b"xx 0123456789abcdef yy too_long_0123456789abcdefz";
        let hits = ascii_alnum_runs(buf, 16);

        assert_eq!(hits, vec![&buf[3..19]]);
    }
}
