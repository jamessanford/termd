# SIGTERM/SIGINT Graceful Shutdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Handle SIGTERM and SIGINT in `termd start` so utmp entries are cleaned up and active gRPC streams are drained gracefully before the process exits.

**Architecture:** On signal, a spawned tokio task calls `registry.destroy_all()` (SIGHUP all children) then broadcasts a shutdown signal to both tonic servers via a broadcast channel. The servers stop accepting new connections and drain existing handlers (up to 5 s). After drain, `utmp::remove_all_records()` cleans up any residual utmp entries and the process exits.

**Tech Stack:** `tokio::signal::unix`, `tokio::sync::broadcast`, `tokio::time::timeout`, tonic `serve_with_incoming_shutdown`, libutempter `utempter_remove_added_record`

---

## File Map

| Action | Path | Purpose |
|--------|------|---------|
| Modify | `src/utmp.rs` | Add `remove_all_records()` and its `extern "C"` declaration |
| Modify | `src/pty.rs` | Add `PtyRegistry::destroy_all()` after existing `destroy()` |
| Modify | `src/server.rs` | Update `serve()` with signal handling and graceful shutdown |
| Modify | `tests/integration.rs` | Add `test_destroy_all_empties_registry` |

---

## Task 1: Add `remove_all_records()` to `src/utmp.rs`

**Files:**
- Modify: `src/utmp.rs`

- [ ] **Step 1: Write the failing test**

In `src/utmp.rs`, add one test to the existing `tests` module:

```rust
#[test]
fn remove_all_records_does_not_panic() {
    remove_all_records();
}
```

The full `tests` module after the addition:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_do_not_panic() {
        add_record(0, "localhost");
        remove_record(0);
    }

    #[test]
    fn remove_all_records_does_not_panic() {
        remove_all_records();
    }
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cargo test --lib utmp::tests::remove_all_records_does_not_panic 2>&1
```

Expected: compile error — `remove_all_records` is not defined.

- [ ] **Step 3: Implement `remove_all_records()`**

Add `utempter_remove_added_record` to the existing `extern "C"` block, and add the public function. The full updated `src/utmp.rs`:

```rust
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
        add_record(0, "localhost");
        remove_record(0);
    }

    #[test]
    fn remove_all_records_does_not_panic() {
        remove_all_records();
    }
}
```

- [ ] **Step 4: Run test to confirm it passes**

```bash
cargo test --lib utmp 2>&1
```

Expected:
```
test utmp::tests::add_and_remove_do_not_panic ... ok
test utmp::tests::remove_all_records_does_not_panic ... ok
```

- [ ] **Step 5: Commit**

```bash
git add src/utmp.rs
git commit -m "feat(utmp): add remove_all_records for process-exit cleanup"
```

---

## Task 2: Add `PtyRegistry::destroy_all()` to `src/pty.rs`

**Files:**
- Modify: `src/pty.rs`
- Modify: `tests/integration.rs`

- [ ] **Step 1: Write the failing test**

In `tests/integration.rs`, add this test. It uses `PtyRegistry` directly — no gRPC server needed. Add it alongside the existing tests:

```rust
#[test]
fn test_destroy_all_empties_registry() {
    let registry = termd::pty::PtyRegistry::new();
    registry.create(80, 24, None).unwrap();
    registry.create(80, 24, None).unwrap();
    assert_eq!(registry.list().len(), 2);
    registry.destroy_all();
    assert_eq!(registry.list().len(), 0);
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cargo test test_destroy_all_empties_registry 2>&1
```

Expected: compile error — `destroy_all` is not defined on `PtyRegistry`.

- [ ] **Step 3: Implement `PtyRegistry::destroy_all()`**

In `src/pty.rs`, add `destroy_all` immediately after the existing `destroy` method (currently ending at line 358). Insert between `destroy` and `get`:

```rust
    pub fn destroy_all(&self) {
        let ids: Vec<String> = self.ptys.read().unwrap().keys().cloned().collect();
        for id in ids {
            let _ = self.destroy(&id);
        }
    }
```

The surrounding context for reference:

```rust
    pub fn destroy(&self, id: &str) -> Result<()> {
        let handle = self.ptys.write().unwrap().remove(id)
            .ok_or_else(|| anyhow!("PTY {id} not found"))?;
        let _ = kill(handle.child_pid, Signal::SIGHUP);
        // handle drops at end of scope: wakeup_write closes → reader sees POLLHUP and exits.
        // If callers hold Arc<PtyHandle> clones (e.g. an in-flight refresh), wakeup_write
        // stays open until the last clone drops — POLLHUP fires then, not immediately on return.
        Ok(())
    }

    pub fn destroy_all(&self) {
        let ids: Vec<String> = self.ptys.read().unwrap().keys().cloned().collect();
        for id in ids {
            let _ = self.destroy(&id);
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<PtyHandle>> {
        self.ptys.read().unwrap().get(id).cloned()
    }
```

- [ ] **Step 4: Run test to confirm it passes**

```bash
cargo test test_destroy_all_empties_registry 2>&1
```

Expected:
```
test test_destroy_all_empties_registry ... ok
```

- [ ] **Step 5: Run full suite to confirm nothing broke**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/pty.rs tests/integration.rs
git commit -m "feat(pty): add PtyRegistry::destroy_all for graceful shutdown"
```

---

## Task 3: Update `serve()` in `src/server.rs`

**Files:**
- Modify: `src/server.rs`

- [ ] **Step 1: Replace the `serve()` function**

The current `serve()` (lines 170–207) uses `serve_with_incoming` with no signal handling. Replace it entirely with the following. Everything above line 170 in `server.rs` is unchanged.

```rust
pub async fn serve(
    registry: Arc<PtyRegistry>,
    unix_path: &std::path::Path,
    tcp_addr: std::net::SocketAddr,
    log_grpc: bool,
) -> anyhow::Result<()> {
    use tokio::net::UnixListener;
    use tonic::transport::Server;
    use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
    use tokio::signal::unix::{signal, SignalKind};
    use tokio::sync::broadcast;

    // Remove stale socket file if present
    let _ = std::fs::remove_file(unix_path);

    let unix_listener = UnixListener::bind(unix_path)?;
    let tcp_listener = match tokio::net::TcpListener::bind(tcp_addr).await {
        Ok(l) => l,
        Err(e) => {
            let _ = std::fs::remove_file(unix_path);
            return Err(e.into());
        }
    };

    tracing::info!(unix = ?unix_path, tcp = %tcp_addr, "termd listening");

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let mut shutdown_rx1 = shutdown_tx.subscribe();
    let mut shutdown_rx2 = shutdown_tx.subscribe();

    let registry_for_shutdown = registry.clone();
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint  = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
            _ = sigint.recv()  => tracing::info!("SIGINT received, shutting down"),
        }
        registry_for_shutdown.destroy_all();
        let _ = shutdown_tx.send(());
    });

    let svc_unix = make_service(registry.clone(), log_grpc);
    let svc_tcp  = make_service(registry, log_grpc);

    if tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::try_join!(
            Server::builder()
                .add_service(svc_unix)
                .serve_with_incoming_shutdown(
                    UnixListenerStream::new(unix_listener),
                    async move { let _ = shutdown_rx1.recv().await; },
                ),
            Server::builder()
                .add_service(svc_tcp)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(tcp_listener),
                    async move { let _ = shutdown_rx2.recv().await; },
                ),
        ),
    ).await.is_err() {
        tracing::warn!("shutdown drain timed out after 5s, forcing exit");
    }

    crate::utmp::remove_all_records();
    Ok(())
}
```

- [ ] **Step 2: Run the full test suite**

```bash
cargo build 2>&1 && cargo test 2>&1
```

Expected: clean build, all tests pass. The integration tests start a real server — they exercise the new code path but terminate the server via `drop` rather than signal, which is fine.

- [ ] **Step 3: Manual smoke test**

Start the server in one terminal:
```bash
cargo run -- start
```

In a second terminal, create a PTY and verify it appears in utmp:
```bash
# Create a PTY via grpc (or let the server idle)
who    # should show a termd session if a PTY was created
```

Send SIGTERM to the server (use the PID printed at startup, or Ctrl+C for SIGINT):
```bash
kill -TERM <pid>   # or press Ctrl+C in the server terminal
```

Expected:
- Server logs "SIGTERM received, shutting down" (or SIGINT)
- Server exits cleanly (exit code 0)
- `who` no longer shows any termd sessions

- [ ] **Step 4: Commit**

```bash
git add src/server.rs
git commit -m "feat(server): handle SIGTERM/SIGINT with graceful shutdown and utmp cleanup"
```
