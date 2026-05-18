use std::os::unix::io::RawFd;

#[cfg(has_utempter)]
extern "C" {
    fn utempter_add_record(master_fd: libc::c_int, host: *const libc::c_char) -> libc::c_int;
    fn utempter_remove_record(master_fd: libc::c_int) -> libc::c_int;
    fn utempter_remove_added_record() -> libc::c_int;
}

/// Write a USER_PROCESS utmp entry for the PTY identified by `master_fd`.
/// `host` is placed in the ut_host field; use the local hostname for local sessions.
/// No-op if termd was built without libutempter.
pub fn add_record(master_fd: RawFd, host: &str) {
    #[cfg(has_utempter)]
    {
        use std::ffi::CString;
        if let Ok(c_host) = CString::new(host) {
            unsafe { utempter_add_record(master_fd, c_host.as_ptr()); }
        }
    }
    #[cfg(not(has_utempter))]
    { let _ = (master_fd, host); }
}

/// Write a DEAD_PROCESS utmp entry, closing out the session for `master_fd`.
/// No-op if termd was built without libutempter.
pub fn remove_record(master_fd: RawFd) {
    #[cfg(has_utempter)]
    unsafe { utempter_remove_record(master_fd); }
    #[cfg(not(has_utempter))]
    { let _ = master_fd; }
}

/// Remove all utmp entries added by this process.
/// Called at graceful shutdown as a belt-and-suspenders cleanup.
/// No-op if termd was built without libutempter.
pub fn remove_all_records() {
    #[cfg(has_utempter)]
    unsafe { utempter_remove_added_record(); }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_do_not_panic() {
        // fd 0 (stdin) is not a PTY master — utempter will fail internally,
        // but the wrapper must not panic regardless.
        add_record(0, "localhost");
        remove_record(0);
    }

    #[test]
    fn remove_all_records_does_not_panic() {
        remove_all_records();
    }
}
