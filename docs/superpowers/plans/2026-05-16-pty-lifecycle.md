# PTY Lifecycle Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 50 ms poll timeout with a wakeup pipe, move child ownership to the reader thread, broadcast exit notifications, and add `attach --debug`.

**Architecture:** A `pipe2(O_CLOEXEC)` pair is created per PTY. The write end lives in `PtyHandle`; closing it signals the reader via `POLLHUP`. `reader_thread` polls both the PTY master fd and the pipe read fd indefinitely, eliminating the timeout. After the main loop exits (EIO or POLLHUP), the reader reaps the child, formats an exit message, and broadcasts it before returning. The `--debug` flag is a trivial branch in `attach`.

**Tech Stack:** Rust async (tokio), `libc` crate (pipe2, poll, read/write), `nix` 0.31 (signal, pty), `std::os::fd::OwnedFd` (auto-close on drop), `broadcast::Sender/Receiver`.

**Spec:** `docs/superpowers/specs/2026-05-16-pty-lifecycle-design.md`

---

## Files

- **Modify:** `src/pty.rs` — wakeup pipe, PtyHandle restructure, reader_thread overhaul, exit notification
- **Modify:** `src/main.rs` — `attach --debug` flag
- **Modify:** `tests/integration.rs` — two new tests (broadcast closes on destroy, exit notification)

---

### Task 1: Write failing tests

**Files:**
- Modify: `tests/integration.rs`

- [ ] **Step 1: Add test for broadcast closing after destroy**

In `tests/integration.rs`, add after `test_destroy_removes_pty`:

```rust
#[tokio::test]
async fn test_destroy_closes_broadcast() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let id = handle.info().id.clone();
    let mut rx = handle.subscribe();
    drop(handle);

    registry.destroy(&id).unwrap();

    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Err(RecvError::Closed) => return true,
                Err(RecvError::Lagged(_)) | Ok(_) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(closed, "broadcast should close within 5s of destroy");
}
```

- [ ] **Step 2: Add test for exit notification**

Add after the test above:

```rust
#[tokio::test]
async fn test_exit_notification_broadcast() {
    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.subscribe();

    handle.write(b"exit\n").unwrap();

    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(chunk) => {
                    if chunk.data.windows(9).any(|w| w == b"[Command ") {
                        return true;
                    }
                }
                Err(_) => return false,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "should receive exit notification after shell exits");
}
```

- [ ] **Step 3: Run both new tests to see their current state**

```bash
cargo test --test integration test_destroy_closes_broadcast test_exit_notification_broadcast 2>&1
```

Expected: `test_destroy_closes_broadcast` may pass or timeout (current code eventually closes but has no pipe), `test_exit_notification_broadcast` fails (no notification sent yet). If `test_destroy_closes_broadcast` is flaky, that's OK — it will be deterministic after Task 3.

- [ ] **Step 4: Commit**

```bash
git add tests/integration.rs
git commit -m "test: add destroy-closes-broadcast and exit-notification tests"
```

---

### Task 2: Restructure PtyHandle — add wakeup pipe, remove child field

**Files:**
- Modify: `src/pty.rs:1-60` (imports, struct)
- Modify: `src/pty.rs:129-250` (`create`, `destroy`, `refresh`)

- [ ] **Step 1: Add OwnedFd import**

Find the existing import line:
```rust
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
```
Replace with:
```rust
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::fd::OwnedFd;
```

- [ ] **Step 2: Update PtyHandle struct — add wakeup_write, remove child**

Find:
```rust
pub struct PtyHandle {
    id: String,
    pts_name: String,
    created_at: SystemTime,
    hostname: String,
    cols: AtomicU32,
    rows: AtomicU32,
    title: Arc<Mutex<String>>,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    writer: Mutex<File>,
    refresh_tx: std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    child_pid: u32,
    child: Mutex<Option<std::process::Child>>,
}
```
Replace with:
```rust
pub struct PtyHandle {
    id: String,
    pts_name: String,
    created_at: SystemTime,
    hostname: String,
    cols: AtomicU32,
    rows: AtomicU32,
    title: Arc<Mutex<String>>,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    writer: Mutex<File>,
    refresh_tx: std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    child_pid: u32,
    wakeup_write: OwnedFd,
}
```

- [ ] **Step 3: Update PtyHandle::refresh() to signal the pipe**

Find:
```rust
    pub async fn refresh(&self) -> Result<RefreshData> {
        let (tx, rx) = oneshot::channel();
        self.refresh_tx.send(tx).map_err(|_| anyhow!("PTY reader thread is dead"))?;
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped refresh response"))?
    }
```
Replace with:
```rust
    pub async fn refresh(&self) -> Result<RefreshData> {
        let (tx, rx) = oneshot::channel();
        self.refresh_tx.send(tx).map_err(|_| anyhow!("PTY reader thread is dead"))?;
        // Wake the reader immediately instead of waiting up to 50 ms for the poll timeout
        unsafe { libc::write(self.wakeup_write.as_raw_fd(), [1u8].as_ptr() as *const libc::c_void, 1) };
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped refresh response"))?
    }
```

- [ ] **Step 4: Update PtyRegistry::create() — create pipe, pass child to reader**

In `PtyRegistry::create()`, find the section after `let child = cmd.spawn()...`:

```rust
        let (tx, _) = broadcast::channel::<Arc<PtyChunk>>(512);
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let generation = Arc::new(AtomicU64::new(0));

        let child_pid = child.id();
        let handle = Arc::new(PtyHandle {
            id: id.clone(),
            pts_name,
            created_at: SystemTime::now(),
            hostname,
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            title: title.clone(),
            tx: tx.clone(),
            writer: Mutex::new(unsafe { File::from_raw_fd(master_fd) }),
            refresh_tx,
            child_pid,
            child: Mutex::new(Some(child)),
        });

        // Spawn dedicated reader thread — owns all libghostty state
        let master_reader = unsafe { File::from_raw_fd(master_reader_fd) };
        let title_for_thread = title.clone();
        std::thread::Builder::new()
            .name(format!("pty-reader-{id}"))
            .spawn(move || reader_thread(master_reader, tx, generation, refresh_rx, title_for_thread, cols, rows))
            .context("spawn reader thread")?;
```

Replace with:

```rust
        let (tx, _) = broadcast::channel::<Arc<PtyChunk>>(512);
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let generation = Arc::new(AtomicU64::new(0));

        // Create wakeup pipe: reader polls on read end; refresh() writes to write end.
        // O_CLOEXEC ensures child processes don't inherit these fds.
        let mut pipe_fds = [0i32; 2];
        let rc = unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) };
        if rc < 0 {
            return Err(std::io::Error::last_os_error()).context("pipe2 for wakeup");
        }
        let wakeup_read = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        let wakeup_write = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };

        let child_pid = child.id();
        let handle = Arc::new(PtyHandle {
            id: id.clone(),
            pts_name,
            created_at: SystemTime::now(),
            hostname,
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            title: title.clone(),
            tx: tx.clone(),
            writer: Mutex::new(unsafe { File::from_raw_fd(master_fd) }),
            refresh_tx,
            child_pid,
            wakeup_write,
        });

        // Spawn dedicated reader thread — owns libghostty state and child process
        let master_reader = unsafe { File::from_raw_fd(master_reader_fd) };
        let title_for_thread = title.clone();
        std::thread::Builder::new()
            .name(format!("pty-reader-{id}"))
            .spawn(move || reader_thread(master_reader, tx, generation, refresh_rx, wakeup_read, child, title_for_thread, cols, rows))
            .context("spawn reader thread")?;
```

- [ ] **Step 5: Update PtyRegistry::destroy() — remove child.wait() thread**

Find:
```rust
    pub fn destroy(&self, id: &str) -> Result<()> {
        let handle = self.ptys.write().unwrap().remove(id)
            .ok_or_else(|| anyhow!("PTY {id} not found"))?;
        let _ = kill(Pid::from_raw(handle.child_pid as i32), Signal::SIGHUP);
        if let Some(mut child) = handle.child.lock().unwrap().take() {
            std::thread::spawn(move || { let _ = child.wait(); });
        }
        Ok(())
    }
```
Replace with:
```rust
    pub fn destroy(&self, id: &str) -> Result<()> {
        let handle = self.ptys.write().unwrap().remove(id)
            .ok_or_else(|| anyhow!("PTY {id} not found"))?;
        let _ = kill(Pid::from_raw(handle.child_pid as i32), Signal::SIGHUP);
        // handle drops here: wakeup_write closes → reader sees POLLHUP and exits
        Ok(())
    }
```

- [ ] **Step 6: Verify it compiles (will fail until reader_thread signature is updated)**

```bash
cargo build 2>&1 | head -40
```

Expected: compile errors about `reader_thread` argument count mismatch and missing `child` field. That's fine — Task 3 fixes it.

---

### Task 3: Update reader_thread — new signature and poll loop

**Files:**
- Modify: `src/pty.rs` — `reader_thread` function (~line 352)

- [ ] **Step 1: Update reader_thread signature**

Find:
```rust
fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    generation: Arc<AtomicU64>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
    title: Arc<Mutex<String>>,
    init_cols: u32,
    init_rows: u32,
) {
```
Replace with:
```rust
fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    generation: Arc<AtomicU64>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
    wakeup_read: OwnedFd,
    mut child: std::process::Child,
    title: Arc<Mutex<String>>,
    init_cols: u32,
    init_rows: u32,
) {
```

- [ ] **Step 2: Replace the poll loop**

Find the poll loop section (starting at `let master_fd = master.as_raw_fd();` and ending at the closing `}` of `reader_thread`):

```rust
    // master_fd is only valid as long as master (the owning File) is alive
    let master_fd = master.as_raw_fd();

    let mut buf = [0u8; 4096];
    loop {
        // Drain any pending refresh requests before waiting for PTY data
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_refresh(&mut terminal, &mut render_state, &mut row_iter_obj, &mut cell_iter_obj, gen);
            let _ = reply_tx.send(result);
        }

        // Use poll() with a 50ms timeout so we can service refresh requests
        // even when the PTY is idle (no new output).
        let mut pfd = libc::pollfd {
            fd: master_fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let poll_ret = unsafe { libc::poll(&mut pfd as *mut libc::pollfd, 1, 50) };

        if poll_ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR — retry
            }
            tracing::debug!("PTY reader poll error: {err}");
            break;
        }

        if poll_ret == 0 {
            // Timeout — no data yet; loop back to drain refresh requests
            continue;
        }

        // Data available (or HUP/ERR)
        let n = match master.read(&mut buf) {
            // On Linux, PTY masters return EIO rather than Ok(0) when the child exits.
            Ok(0) => {
                // EOF — shell exited. Stay alive for refresh requests until refresh_rx closes.
                tracing::debug!("PTY reader: EOF on master fd");
                loop {
                    match refresh_rx.recv() {
                        Ok(reply_tx) => {
                            let gen = generation.load(Ordering::Relaxed);
                            let result = do_refresh(&mut terminal, &mut render_state, &mut row_iter_obj, &mut cell_iter_obj, gen);
                            let _ = reply_tx.send(result);
                        }
                        Err(_) => return, // PtyHandle dropped (destroyed)
                    }
                }
            }
            Err(e) => {
                tracing::debug!("PTY reader error: {e}");
                // EOF/error — shell exited. Stay alive for refresh requests until refresh_rx closes.
                loop {
                    match refresh_rx.recv() {
                        Ok(reply_tx) => {
                            let gen = generation.load(Ordering::Relaxed);
                            let result = do_refresh(&mut terminal, &mut render_state, &mut row_iter_obj, &mut cell_iter_obj, gen);
                            let _ = reply_tx.send(result);
                        }
                        Err(_) => return, // PtyHandle dropped (destroyed)
                    }
                }
            }
            Ok(n) => n,
        };

        terminal.vt_write(&buf[..n]);
        let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
        let chunk = Arc::new(PtyChunk {
            generation: gen,
            data: Bytes::copy_from_slice(&buf[..n]),
        });
        let _ = tx.send(chunk); // ignore SendError (no subscribers is fine)
    }
}
```

Replace with:

```rust
    let master_fd = master.as_raw_fd();
    let wakeup_fd = wakeup_read.as_raw_fd();
    let mut buf = [0u8; 4096];

    'main: loop {
        // Drain pending refresh requests first (wakeup pipe already signalled us)
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_refresh(
                &mut terminal, &mut render_state,
                &mut row_iter_obj, &mut cell_iter_obj, gen,
            );
            let _ = reply_tx.send(result);
        }

        // Poll indefinitely on the PTY master and the wakeup pipe
        let mut pfds = [
            libc::pollfd { fd: master_fd, events: libc::POLLIN, revents: 0 },
            libc::pollfd { fd: wakeup_fd, events: libc::POLLIN, revents: 0 },
        ];
        let poll_ret = unsafe { libc::poll(pfds.as_mut_ptr(), 2, -1) };

        if poll_ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR — retry
            }
            tracing::debug!("PTY reader poll error: {err}");
            break;
        }

        // Handle wakeup pipe first (refresh signal or PtyHandle drop)
        if pfds[1].revents & libc::POLLIN != 0 {
            // Drain bytes so the pipe doesn't fill up
            let mut drain = [0u8; 64];
            unsafe { libc::read(wakeup_fd, drain.as_mut_ptr() as *mut libc::c_void, 64) };
            // Refresh requests will be drained at the top of the next iteration
        }
        if pfds[1].revents & libc::POLLHUP != 0 {
            // Write end closed — PtyHandle was dropped (destroy called)
            tracing::debug!("PTY reader: wakeup pipe closed, exiting");
            break 'main;
        }

        // Handle PTY data
        if pfds[0].revents & libc::POLLIN != 0 {
            match master.read(&mut buf) {
                Ok(0) => {
                    tracing::debug!("PTY reader: EOF on master fd");
                    break 'main;
                }
                Err(e) => {
                    tracing::debug!("PTY reader error: {e}");
                    break 'main;
                }
                Ok(n) => {
                    terminal.vt_write(&buf[..n]);
                    let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
                    let chunk = Arc::new(PtyChunk {
                        generation: gen,
                        data: Bytes::copy_from_slice(&buf[..n]),
                    });
                    let _ = tx.send(chunk);
                }
            }
        }
    }

    // Exit notification and cleanup are added in Task 4
    drop(wakeup_read);
}
```

- [ ] **Step 3: Build and verify it compiles**

```bash
cargo build 2>&1
```

Expected: clean build. If there are unused variable warnings for `child`, that's expected (Task 4 uses it).

- [ ] **Step 4: Run all existing tests**

```bash
cargo test 2>&1
```

Expected: all previously passing tests still pass. `test_destroy_closes_broadcast` should now pass reliably (POLLHUP path). `test_exit_notification_broadcast` still fails.

- [ ] **Step 5: Commit**

```bash
git add src/pty.rs
git commit -m "feat: replace poll timeout with wakeup pipe in reader_thread"
```

---

### Task 4: Add exit notification

**Files:**
- Modify: `src/pty.rs` — end of `reader_thread`

- [ ] **Step 1: Replace the placeholder at the end of reader_thread**

Find the end of `reader_thread` (added in Task 3):
```rust
    // Exit notification and cleanup are added in Task 4
    drop(wakeup_read);
}
```

Replace with:
```rust
    // Reap child and broadcast exit notification
    let status = child.try_wait().ok().flatten().or_else(|| child.wait().ok());
    let exit_msg = {
        let title = title.lock().unwrap().clone();
        match status {
            Some(s) => {
                if let Some(code) = s.code() {
                    format!("\r\n[Command {} exited with code {}]\r\n", title, code)
                } else {
                    format!("\r\n[Command {} was killed]\r\n", title)
                }
            }
            None => format!("\r\n[Command {} terminated]\r\n", title),
        }
    };
    let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
    let _ = tx.send(Arc::new(PtyChunk {
        generation: gen,
        data: Bytes::from(exit_msg.into_bytes()),
    }));

    // Drain any refresh requests that arrived just before exit
    while let Ok(reply_tx) = refresh_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_refresh(
            &mut terminal, &mut render_state,
            &mut row_iter_obj, &mut cell_iter_obj, gen,
        );
        let _ = reply_tx.send(result);
    }

    drop(wakeup_read); // closes read end; wakeup_write already closed (PtyHandle dropped)
}
```

- [ ] **Step 2: Build**

```bash
cargo build 2>&1
```

Expected: clean build.

- [ ] **Step 3: Run all tests**

```bash
cargo test 2>&1
```

Expected: all tests pass including `test_exit_notification_broadcast` and `test_destroy_closes_broadcast`.

- [ ] **Step 4: Commit**

```bash
git add src/pty.rs
git commit -m "feat: broadcast exit notification when PTY shell exits"
```

---

### Task 5: Add `attach --debug` flag

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Add debug field to Cmd::Attach**

Find:
```rust
    /// Attach to a running PTY, streaming output to stdout and forwarding stdin
    Attach {
        /// PTY ID to attach to (from `termd list`)
        pty_id: String,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
```
Replace with:
```rust
    /// Attach to a running PTY, streaming output to stdout and forwarding stdin
    Attach {
        /// PTY ID to attach to (from `termd list`)
        pty_id: String,
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Print message metadata to stderr instead of writing data to stdout
        #[arg(long)]
        debug: bool,
    },
```

- [ ] **Step 2: Thread debug through the Attach match arm**

Find the beginning of the `Cmd::Attach` match arm:
```rust
        Cmd::Attach { pty_id, socket } => {
```
Replace with:
```rust
        Cmd::Attach { pty_id, socket, debug } => {
```

- [ ] **Step 3: Log RefreshResponse metadata in debug mode**

Find the section where the refresh response is received and painted:
```rust
            // Paint refresh and replay buffered chunks that post-date it
            let mut stdout = tokio::io::stdout();
            stdout.write_all(&refresh_bytes).await?;
            for (gen, data) in buffered {
                if gen > refresh_gen {
                    stdout.write_all(&data).await?;
                }
            }
            stdout.flush().await?;
```
Replace with:
```rust
            // Paint refresh and replay buffered chunks that post-date it
            let mut stdout = tokio::io::stdout();
            if debug {
                eprintln!("[Refresh gen={} len={}]", refresh_gen, refresh_bytes.len());
            } else {
                stdout.write_all(&refresh_bytes).await?;
            }
            for (gen, data) in &buffered {
                if *gen > refresh_gen {
                    if debug {
                        eprintln!("[Buffered gen={} len={}]", *gen, data.len());
                    } else {
                        stdout.write_all(data).await?;
                    }
                }
            }
            stdout.flush().await?;
```

- [ ] **Step 4: Branch StreamData writes in the main receive loop**

Find inside the main receive loop:
```rust
                            if let Some(Response::Stream(s)) = r.response {
                                    if s.generation > refresh_gen {
                                        if stdout.write_all(&s.data).await.is_err() { break; }
                                        let _ = stdout.flush().await;
                                    }
                                }
```
Replace with:
```rust
                            if let Some(Response::Stream(s)) = r.response {
                                if s.generation > refresh_gen {
                                    if debug {
                                        eprintln!("[Stream gen={} len={}]", s.generation, s.data.len());
                                    } else {
                                        if stdout.write_all(&s.data).await.is_err() { break; }
                                        let _ = stdout.flush().await;
                                    }
                                }
                            }
```

- [ ] **Step 5: Build and run full test suite**

```bash
cargo build 2>&1 && cargo test 2>&1
```

Expected: clean build, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "feat: add attach --debug flag to print message metadata"
```

---

## Smoke Test (manual)

After all tasks:

```bash
# Terminal 1 — start daemon
cargo run -- start

# Terminal 2 — create and attach in debug mode
PTY=$(cargo run -- create)
cargo run -- attach --debug $PTY
# Expected: [Refresh gen=N len=M] lines printed to stderr, no terminal painting

# Terminal 3 — destroy to verify exit notification
cargo run -- destroy $PTY
# Terminal 2 should print: [Stream gen=N len=M] for the exit notification chunk
```
