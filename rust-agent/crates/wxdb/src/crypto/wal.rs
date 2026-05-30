use anyhow::Result;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use super::{decrypt_page, PAGE_SZ};

const WAL_HDR_SZ: usize = 32;
const WAL_FRAME_HDR: usize = 24;

pub fn apply_wal(wal_path: &Path, out_path: &Path, enc_key: &[u8; 32]) -> Result<()> {
    if !wal_path.exists() {
        return Ok(());
    }

    let wal_data = std::fs::read(wal_path)?;
    if wal_data.len() <= WAL_HDR_SZ {
        return Ok(());
    }

    let s1 = u32::from_be_bytes(wal_data[16..20].try_into().unwrap());
    let s2 = u32::from_be_bytes(wal_data[20..24].try_into().unwrap());
    let frame_size = WAL_FRAME_HDR + PAGE_SZ;
    let frame_area = &wal_data[WAL_HDR_SZ..];
    let mut db_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(out_path)?;

    let mut pos = 0usize;
    while pos + frame_size <= frame_area.len() {
        let fh = &frame_area[pos..pos + WAL_FRAME_HDR];
        let page_data = &frame_area[pos + WAL_FRAME_HDR..pos + frame_size];
        let pgno = u32::from_be_bytes(fh[0..4].try_into().unwrap());
        let fs1 = u32::from_be_bytes(fh[8..12].try_into().unwrap());
        let fs2 = u32::from_be_bytes(fh[12..16].try_into().unwrap());
        pos += frame_size;

        if pgno == 0 || pgno > 1_000_000 || fs1 != s1 || fs2 != s2 {
            continue;
        }

        let mut page_buf = page_data.to_vec();
        if page_buf.len() < PAGE_SZ {
            page_buf.resize(PAGE_SZ, 0);
        }
        let dec = decrypt_page(enc_key, &page_buf, if pgno == 1 { 2 } else { pgno })?;
        let file_offset = (pgno as u64 - 1) * PAGE_SZ as u64;
        db_file.seek(SeekFrom::Start(file_offset))?;
        db_file.write_all(&dec)?;
    }

    Ok(())
}
