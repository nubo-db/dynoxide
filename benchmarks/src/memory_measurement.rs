/// Cross-platform RSS (Resident Set Size) measurement.
///
/// - macOS: `libc::getrusage()` with `ru_maxrss` in bytes
/// - Linux: reads `/proc/self/status` VmRSS (in KB, converted to bytes)
///
/// Returns the current RSS in bytes for this process.
#[cfg(target_os = "macos")]
pub fn current_rss_bytes() -> u64 {
    use std::mem::MaybeUninit;

    // On macOS, mach_task_basic_info gives current RSS.
    // Fallback: getrusage gives peak only. Use task_info for current.
    unsafe {
        let mut info = MaybeUninit::<libc::mach_task_basic_info_data_t>::uninit();
        let mut count = (std::mem::size_of::<libc::mach_task_basic_info_data_t>()
            / std::mem::size_of::<libc::natural_t>())
            as libc::mach_msg_type_number_t;

        #[allow(deprecated)] // libc::mach_task_self is deprecated in favor of mach2 crate
        let task = libc::mach_task_self();

        let kr = libc::task_info(
            task,
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr() as libc::task_info_t,
            &mut count,
        );

        if kr == libc::KERN_SUCCESS {
            let info = info.assume_init();
            info.resident_size
        } else {
            // Fallback to getrusage (gives peak, not current)
            peak_rss_bytes()
        }
    }
}

#[cfg(target_os = "linux")]
pub fn current_rss_bytes() -> u64 {
    // Read /proc/self/status for VmRSS
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb_str = rest.trim().trim_end_matches(" kB").trim();
                if let Ok(kb) = kb_str.parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn current_rss_bytes() -> u64 {
    0 // Unsupported platform
}

/// Returns the peak RSS (max resident set size) in bytes for this process.
pub fn peak_rss_bytes() -> u64 {
    unsafe {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) == 0 {
            let usage = usage.assume_init();
            let max_rss = usage.ru_maxrss as u64;

            // macOS reports ru_maxrss in bytes, Linux reports in KB
            if cfg!(target_os = "macos") {
                max_rss
            } else {
                max_rss * 1024
            }
        } else {
            0
        }
    }
}

/// Returns the disk usage in bytes for a SQLite database directory.
/// Sums the sizes of *.db, *.db-wal, and *.db-shm files.
pub fn disk_usage_bytes(db_dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(db_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && (name.ends_with(".db") || name.ends_with(".db-wal") || name.ends_with(".db-shm"))
                && let Ok(meta) = std::fs::metadata(&path)
            {
                total += meta.len();
            }
        }
    }
    total
}

/// Format bytes as a human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
