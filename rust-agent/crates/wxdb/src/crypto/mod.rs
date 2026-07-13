pub mod wal;

use aes::Aes256;
use anyhow::{bail, Result};
use cbc::cipher::{BlockDecryptMut, KeyIvInit};
use cbc::Decryptor;
use std::io::{Read, Write};
use std::path::Path;

type Block = aes::cipher::Block<Aes256>;
type Aes256CbcDec = Decryptor<Aes256>;

pub const PAGE_SZ: usize = 4096;
pub const SALT_SZ: usize = 16;
pub const RESERVE_SZ: usize = 80;
pub const SQLITE_HDR: &[u8] = b"SQLite format 3\x00";

pub fn decrypt_page(enc_key: &[u8; 32], page_data: &[u8], pgno: u32) -> Result<Vec<u8>> {
    if page_data.len() < PAGE_SZ {
        bail!("页面数据不足 {} 字节", PAGE_SZ);
    }

    let iv_offset = PAGE_SZ - RESERVE_SZ;
    let iv: &[u8; 16] = page_data[iv_offset..iv_offset + 16]
        .try_into()
        .expect("IV length is fixed");
    let mut result = vec![0u8; PAGE_SZ];

    if pgno == 1 {
        let enc = &page_data[SALT_SZ..PAGE_SZ - RESERVE_SZ];
        let dec = aes_cbc_decrypt(enc_key, iv, enc)?;
        result[..16].copy_from_slice(SQLITE_HDR);
        result[16..PAGE_SZ - RESERVE_SZ].copy_from_slice(&dec);
    } else {
        let enc = &page_data[..PAGE_SZ - RESERVE_SZ];
        let dec = aes_cbc_decrypt(enc_key, iv, enc)?;
        result[..PAGE_SZ - RESERVE_SZ].copy_from_slice(&dec);
    }

    Ok(result)
}

fn aes_cbc_decrypt(key: &[u8; 32], iv: &[u8; 16], data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() || !data.len().is_multiple_of(16) {
        bail!("密文长度不是 AES 块大小的倍数: {}", data.len());
    }
    let mut blocks: Vec<Block> = data.chunks_exact(16).map(Block::clone_from_slice).collect();
    Aes256CbcDec::new(key.into(), iv.into()).decrypt_blocks_mut(&mut blocks);
    Ok(blocks
        .iter()
        .flat_map(|block| block.iter().copied())
        .collect())
}

pub fn full_decrypt(db_path: &Path, out_path: &Path, enc_key: &[u8; 32]) -> Result<()> {
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut input = std::fs::File::open(db_path)?;
    let file_size = input.metadata()?.len() as usize;
    if file_size == 0 {
        bail!("数据库文件为空: {}", db_path.display());
    }

    let mut output = std::fs::File::create(out_path)?;
    let total_pages = file_size.div_ceil(PAGE_SZ);
    let mut page_buf = vec![0u8; PAGE_SZ];

    for pgno in 1..=total_pages {
        let page_start = (pgno - 1) * PAGE_SZ;
        let bytes_remaining = file_size.saturating_sub(page_start);
        read_page(&mut input, &mut page_buf, bytes_remaining)?;
        let dec = decrypt_page(enc_key, &page_buf, pgno as u32)?;
        output.write_all(&dec)?;
    }

    Ok(())
}

fn read_page(
    input: &mut impl Read,
    page_buf: &mut [u8],
    bytes_remaining: usize,
) -> std::io::Result<usize> {
    let expected = bytes_remaining.min(PAGE_SZ);
    input.read_exact(&mut page_buf[..expected])?;
    if expected < PAGE_SZ {
        page_buf[expected..].fill(0);
    }
    Ok(expected)
}

pub(crate) fn hex_to_32bytes(hex: &str) -> Result<[u8; 32]> {
    if hex.len() != 64 {
        bail!("密钥长度应为 64 个 hex 字符，实际 {}", hex.len());
    }
    let mut out = [0u8; 32];
    for (idx, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let s = std::str::from_utf8(chunk)?;
        out[idx] = u8::from_str_radix(s, 16)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::hex_to_32bytes;

    #[test]
    fn parses_32_byte_hex_key() {
        let key = "01".repeat(32);
        let parsed = hex_to_32bytes(&key).unwrap();
        assert_eq!(parsed, [1u8; 32]);
    }
}
