use anyhow::Result;
use std::path::Path;
use windows::Win32::Foundation::{CloseHandle, HANDLE};
use windows::Win32::System::Diagnostics::Debug::ReadProcessMemory;
use windows::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, Process32First, Process32Next, PROCESSENTRY32, TH32CS_SNAPPROCESS,
};
use windows::Win32::System::Memory::{
    VirtualQueryEx, MEMORY_BASIC_INFORMATION, MEM_COMMIT, PAGE_EXECUTE_READWRITE,
    PAGE_EXECUTE_WRITECOPY, PAGE_GUARD, PAGE_NOCACHE, PAGE_READWRITE, PAGE_WRITECOMBINE,
    PAGE_WRITECOPY,
};
use windows::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_INFORMATION, PROCESS_VM_READ};

use super::{collect_db_salts, KeyEntry};

const HEX_PATTERN_LEN: usize = 96;
const CHUNK_SIZE: usize = 2 * 1024 * 1024;

pub fn scan_keys(db_dir: &Path) -> Result<Vec<KeyEntry>> {
    let db_salts = collect_db_salts(db_dir);
    let pids = find_wechat_pids();
    if pids.is_empty() {
        anyhow::bail!("找不到 Weixin.exe 进程，请确认微信正在运行");
    }
    let mut raw_keys = Vec::new();
    let mut open_errors = Vec::new();
    for pid in pids {
        let process = match open_readable_process(pid) {
            Ok(process) => process,
            Err(error) => {
                open_errors.push(format!("pid {pid}: {error}"));
                continue;
            }
        };
        raw_keys.extend(scan_memory(process));
        unsafe {
            let _ = CloseHandle(process);
        }
    }

    if raw_keys.is_empty() && !open_errors.is_empty() {
        anyhow::bail!(
            "OpenProcess 全部失败；如微信权限较高，请用相同或管理员权限运行: {}",
            open_errors.join("; ")
        );
    }

    let mut entries = Vec::new();
    for (key_hex, salt_hex) in &raw_keys {
        for (db_salt, db_name) in &db_salts {
            if salt_hex == db_salt {
                entries.push(KeyEntry {
                    db_name: db_name.clone(),
                    enc_key: key_hex.clone(),
                    salt: salt_hex.clone(),
                });
                break;
            }
        }
    }
    Ok(entries)
}

fn open_readable_process(pid: u32) -> windows::core::Result<HANDLE> {
    let access_modes = [PROCESS_VM_READ | PROCESS_QUERY_INFORMATION, PROCESS_VM_READ];
    let mut last_error = None;
    for access in access_modes {
        match unsafe { OpenProcess(access, false, pid) } {
            Ok(handle) => return Ok(handle),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(windows::core::Error::from_win32))
}

fn find_wechat_pids() -> Vec<u32> {
    let Ok(snap) = (unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) }) else {
        return Vec::new();
    };
    let mut entry = PROCESSENTRY32 {
        dwSize: std::mem::size_of::<PROCESSENTRY32>() as u32,
        ..Default::default()
    };
    let mut pids = Vec::new();

    unsafe {
        if Process32First(snap, &mut entry).is_err() {
            let _ = CloseHandle(snap);
            return pids;
        }
        loop {
            let name = std::ffi::CStr::from_ptr(entry.szExeFile.as_ptr()).to_string_lossy();
            if name.eq_ignore_ascii_case("Weixin.exe") {
                pids.push(entry.th32ProcessID);
            }
            if Process32Next(snap, &mut entry).is_err() {
                break;
            }
        }
        let _ = CloseHandle(snap);
    }
    pids
}

fn scan_memory(process: HANDLE) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut addr: usize = 0;

    loop {
        let mut mbi = MEMORY_BASIC_INFORMATION::default();
        let ret = unsafe {
            VirtualQueryEx(
                process,
                Some(addr as *const _),
                &mut mbi,
                std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
            )
        };
        if ret == 0 {
            break;
        }

        let region_size = mbi.RegionSize;
        let base = mbi.BaseAddress as usize;
        if mbi.State == MEM_COMMIT && is_writable_readable_page(mbi.Protect.0) {
            scan_region(process, base, region_size, &mut results);
        }

        addr = base.saturating_add(region_size);
        if addr == 0 {
            break;
        }
    }

    results
}

fn is_writable_readable_page(protect: u32) -> bool {
    let base = protect & !(PAGE_GUARD.0 | PAGE_NOCACHE.0 | PAGE_WRITECOMBINE.0);
    matches!(
        base,
        x if x == PAGE_READWRITE.0
            || x == PAGE_WRITECOPY.0
            || x == PAGE_EXECUTE_READWRITE.0
            || x == PAGE_EXECUTE_WRITECOPY.0
    )
}

fn scan_region(process: HANDLE, base: usize, size: usize, results: &mut Vec<(String, String)>) {
    let overlap = HEX_PATTERN_LEN + 3;
    let mut offset = 0usize;
    while offset < size {
        let chunk_size = std::cmp::min(CHUNK_SIZE, size - offset);
        let addr = base + offset;
        let mut buf = vec![0u8; chunk_size];
        let mut bytes_read: usize = 0;
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
            search_pattern(&buf, results);
        }
        offset += if chunk_size > overlap {
            chunk_size - overlap
        } else {
            chunk_size
        };
    }
}

fn search_pattern(buf: &[u8], results: &mut Vec<(String, String)>) {
    let total = HEX_PATTERN_LEN + 3;
    if buf.len() < total {
        return;
    }
    let mut idx = 0;
    while idx + total <= buf.len() {
        if buf[idx] != b'x' || buf[idx + 1] != b'\'' {
            idx += 1;
            continue;
        }
        let hex_start = idx + 2;
        if !buf[hex_start..hex_start + HEX_PATTERN_LEN]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit())
            || buf[hex_start + HEX_PATTERN_LEN] != b'\''
        {
            idx += 1;
            continue;
        }
        let key_hex = String::from_utf8_lossy(&buf[hex_start..hex_start + 64]).to_lowercase();
        let salt_hex = String::from_utf8_lossy(&buf[hex_start + 64..hex_start + 96]).to_lowercase();
        if !results
            .iter()
            .any(|(key, salt)| key == &key_hex && salt == &salt_hex)
        {
            results.push((key_hex, salt_hex));
        }
        idx += total;
    }
}
