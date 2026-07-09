# Reader-Thread Restructure and Stream-Lag Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move PTY writes into the reader thread behind a single `ReaderRequest` channel (reader becomes sole master-fd owner, with EAGAIN buffering), and add explicit `DATA_LOST` lag recovery so a slow attach client repairs its screen via refresh.

**Architecture:** `src/pty.rs` becomes a `src/pty/` directory module: `mod.rs` (registry, handle, `PtyShared`), `reader.rs` (`Reader` struct, `ReaderRequest`, write buffering), `snapshot.rs` (`do_refresh`/`do_scrollback`). State mirrored between handle and thread consolidates into `Arc<PtyShared>`; the three request channels plus the new write path collapse into one unbounded `std::sync::mpsc` channel; `ClosedNotifier` becomes `impl Drop for Reader`. Lag is surfaced as a synthetic `DataLost` metadata event from the (extracted) forwarding task; the client answers with a refresh request.

**Tech Stack:** Rust, tokio (current_thread), tonic/prost, libghostty-vt, nix/libc.

**Spec:** `docs/superpowers/specs/2026-06-09-reader-thread-and-lag-recovery-design.md`

**Conventions for this plan:**
- "Move verbatim" = cut the named item from its current location and paste unchanged; only visibility/imports change as stated. The moved test suites are the regression net — they must pass without edits to their bodies.
- All `cargo` commands run from the repo root. The full suite spawns real shells (`$SHELL`/`/bin/sh`), as existing tests already do.
- Line numbers refer to the file state at the start of each task (verify with a quick read before editing; earlier tasks shift later line numbers).

---

### Task 1: Convert pty.rs to a directory module; extract snapshot.rs

**Files:**
- Move: `src/pty.rs` → `src/pty/mod.rs`
- Create: `src/pty/snapshot.rs`

- [ ] **Step 1: Baseline — run the full suite**

Run: `cargo test`
Expected: PASS (all). Note the test count; it must not shrink in later tasks.

- [ ] **Step 2: Move the file**

```bash
mkdir src/pty
git mv src/pty.rs src/pty/mod.rs
```

`src/lib.rs` needs no change (`pub mod pty;` resolves to the directory module).

- [ ] **Step 3: Create `src/pty/snapshot.rs` with the formatter code**

Move verbatim from `mod.rs` into `snapshot.rs`:
- `fn do_refresh` (pty.rs:526-649, including its leading "Note: on-demand refresh…" comment) — change signature to `pub(crate) fn do_refresh`
- `fn do_scrollback` (pty.rs:651-756) — change to `pub(crate) fn do_scrollback`
- `mod scrollback_tests` (pty.rs:1513-1743, contains both scrollback and do_refresh tests)

File header for `snapshot.rs`:

```rust
use std::collections::HashMap;

use anyhow::Result;
use bytes::Bytes;
use libghostty_vt::{Terminal, RenderState, ffi};
use libghostty_vt::fmt::{Format, Formatter, FormatterOptions};
use libghostty_vt::render::CursorVisualStyle;
use libghostty_vt::screen::TrackedGridRef;
use libghostty_vt::selection::Selection;
use libghostty_vt::terminal::{Point, PointCoordinate, PointSpace};

use super::{RefreshData, ScrollbackData, ScrollbackOp};
```

Inside the moved `mod scrollback_tests`, add one import after `use super::*;`:

```rust
    use libghostty_vt::TerminalOptions;
```

- [ ] **Step 4: Wire up `mod.rs`**

In `src/pty/mod.rs`, add near the top:

```rust
mod snapshot;
use snapshot::{do_refresh, do_scrollback};
```

Remove the now-unused imports from `mod.rs` (`RenderState`, `CursorVisualStyle`, `Format`/`Formatter`/`FormatterOptions`, `Selection`, `Point`/`PointCoordinate`/`PointSpace`, `ffi`); keep what the remaining code still uses (`Terminal`, `TerminalOptions`, `Screen`, `TrackedGridRef` are still used by `reader_thread`). Let `cargo build` warnings be the guide: the build must end with zero warnings.

- [ ] **Step 5: Verify and commit**

Run: `cargo test`
Expected: PASS, same test count as Step 1.

```bash
git add -A src/pty src/pty.rs
git commit -m "refactor: split pty.rs into pty/ module, extract snapshot.rs"
```

---

### Task 2: Extract reader.rs (verbatim move)

**Files:**
- Create: `src/pty/reader.rs`
- Modify: `src/pty/mod.rs`

- [ ] **Step 1: Create `src/pty/reader.rs` with the reader-thread code**

Move verbatim from `mod.rs`:
- `struct ScrollbackReq` (+ doc comment) — fields and struct become `pub(super)`
- `const REFRESH_STALL_TIMEOUT_MS`
- `fn flush_refreshes` (+ doc comment)
- `fn process_read` (+ doc comment)
- `impl PtyInfo { fn closed }` — method becomes `pub(super) fn closed`
- `struct ClosedNotifier` + `impl Drop for ClosedNotifier` (+ doc comments)
- `fn reader_thread` (+ `#[allow(clippy::too_many_arguments)]` and comment) — becomes `pub(super) fn reader_thread`
- `mod boundary_tests` and `mod process_read_tests`

File header for `reader.rs`:

```rust
use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    os::fd::OwnedFd,
    os::unix::io::AsRawFd,
    sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}},
    time::SystemTime,
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use libghostty_vt::{Terminal, TerminalOptions};
use libghostty_vt::screen::Screen;
use libghostty_vt::screen::TrackedGridRef;
use tokio::sync::{broadcast, oneshot};

use super::snapshot::{do_refresh, do_scrollback};
use super::{
    MetadataReason, PtyChunk, PtyEvent, PtyInfo, PtyMetadata,
    RefreshData, ScrollbackData, ScrollbackOp,
};
```

In the moved test modules, the existing `use super::*;` now resolves against reader.rs — both suites only use items that the header above re-exports (`Terminal`, `TerminalOptions`, `broadcast`, `oneshot`, `AtomicU64`, `Bytes`, `PtyEvent`); no body edits.

- [ ] **Step 2: Wire up `mod.rs`**

```rust
mod reader;
use reader::{reader_thread, ScrollbackReq};
```

Remove imports `mod.rs` no longer needs after the move (`Terminal`, `TerminalOptions`, `Screen`, `TrackedGridRef`, `Read` from std::io — `Write` is still used by `PtyHandle::write`). Zero-warning build is the check.

- [ ] **Step 3: Verify and commit**

Run: `cargo test`
Expected: PASS, same count.

```bash
git add -A src/pty
git commit -m "refactor: extract reader thread into pty/reader.rs"
```

---

### Task 3: Introduce PtyShared

**Files:**
- Modify: `src/pty/mod.rs` (PtyShared, PtyHandle, create())
- Modify: `src/pty/reader.rs` (reader_thread signature, ClosedNotifier, TitleChanged)

- [ ] **Step 1: Write the failing test**

Append to the existing `mod subscriber_tests` in `src/pty/mod.rs`:

```rust
    #[test]
    fn shared_info_reflects_title_and_dims() {
        let h = make_handle();
        h.set_title("new-title");
        let info = h.shared().info();
        assert_eq!(info.title, "new-title");
        assert_eq!((info.cols, info.rows), (80, 24));
        assert!(info.subscribers.is_some());
    }
```

Run: `cargo test --lib shared_info_reflects -- --nocapture`
Expected: FAIL to compile — `shared()` and `PtyShared` don't exist.

- [ ] **Step 2: Define PtyShared in `mod.rs`**

```rust
/// State shared between the PtyHandle (tokio side) and the Reader thread.
/// Everything here is cheap to read concurrently; the libghostty Terminal and
/// the master fd deliberately do NOT live here — they are exclusive to the
/// reader thread.
pub(crate) struct PtyShared {
    pub(crate) id:                 u64,
    pub(crate) hostname:           String,
    pub(crate) pts_name:           String,
    pub(crate) created_at:         SystemTime,
    pub(crate) cols:               AtomicU32,
    pub(crate) rows:               AtomicU32,
    pub(crate) title:              Mutex<String>,
    pub(crate) generation:         AtomicU64,
    pub(crate) subscribers:        RwLock<HashMap<String, SubscriberInfo>>,
    pub(crate) last_subscribed_at: Mutex<Option<SystemTime>>,
    // Assigned once at registration (next available slot, 0-based) for stable
    // list ordering; never changes for the life of the handle.
    pub(crate) sort_order:         AtomicU32,
}

impl PtyShared {
    pub(crate) fn info(&self) -> PtyInfo {
        let subscribers = {
            let map = self.subscribers.read().unwrap();
            let mut v: Vec<(String, SubscriberInfo)> =
                map.iter().map(|(id, s)| (id.clone(), s.clone())).collect();
            v.sort_by_key(|(_, s)| s.created_at);
            v
        };  // read-lock released here
        PtyInfo {
            id:                 self.id,
            hostname:           self.hostname.clone(),
            pts_name:           self.pts_name.clone(),
            cols:               self.cols.load(Ordering::Relaxed),
            rows:               self.rows.load(Ordering::Relaxed),
            title:              self.title.lock().unwrap().clone(),
            created_at:         self.created_at,
            last_subscribed_at: *self.last_subscribed_at.lock().unwrap(),
            subscribers:        Some(subscribers),
            sort_order:         self.sort_order.load(Ordering::Relaxed),
        }
    }
}
```

(The `PtyHandle::info` body moves here; the sort/clone logic is identical.)

- [ ] **Step 3: Shrink PtyHandle to delegate**

```rust
pub struct PtyHandle {
    shared: Arc<PtyShared>,
    tx: broadcast::Sender<PtyEvent>,
    writer: Mutex<File>,
    refresh_tx:    std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    scrollback_tx: std::sync::mpsc::SyncSender<ScrollbackReq>,
    resize_tx:     std::sync::mpsc::SyncSender<(u32, u32)>,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    child_pid: Pid,
    wakeup_write: OwnedFd,
}
```

Delegating methods (replace the old field accesses; bodies of `subscribe`, `meta_subscribe`, `broadcast_metadata`, `write`, `refresh`, `scrollback`, `close_scrollback` are unchanged):

```rust
    pub fn info(&self) -> PtyInfo { self.shared.info() }
    pub(crate) fn shared(&self) -> Arc<PtyShared> { self.shared.clone() }
    pub fn id(&self) -> u64 { self.shared.id }
    pub fn current_generation(&self) -> u64 { self.shared.generation.load(Ordering::Relaxed) }
    pub fn set_title(&self, title: &str) { *self.shared.title.lock().unwrap() = title.to_string(); }
    pub fn touch_last_subscribed(&self) {
        *self.shared.last_subscribed_at.lock().unwrap() = Some(SystemTime::now());
    }
    pub fn upsert_subscriber(&self, subscriber_id: &str, info: SubscriberInfo) {
        let mut map = self.shared.subscribers.write().unwrap();
        map.entry(subscriber_id.to_owned())
            .and_modify(|e| {
                e.hostname = info.hostname.clone();
                e.cols     = info.cols;
                e.rows     = info.rows;
                // created_at intentionally not updated — preserve original
            })
            .or_insert(info);
    }
    pub fn remove_subscriber(&self, subscriber_id: &str) {
        self.shared.subscribers.write().unwrap().remove(subscriber_id);
    }
```

In `resize()`, replace `self.cols.store/self.rows.store` with `self.shared.cols.store/self.shared.rows.store` and `self.generation.load` with `self.shared.generation.load`. In `create()`'s sort_order assignment block, `h.sort_order` becomes `h.shared.sort_order`.

- [ ] **Step 4: Build PtyShared in create() and pass it to the reader**

In `PtyRegistry::create`, replace the `generation`/`title` locals and the `*_for_thread` clones with:

```rust
        let (tx, _) = broadcast::channel::<PtyEvent>(512);
        let (meta_tx, _) = broadcast::channel::<Arc<PtyMetadata>>(64);
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let (scrollback_tx, scrollback_rx) =
            std::sync::mpsc::sync_channel::<ScrollbackReq>(8);
        let (resize_tx, resize_rx) = std::sync::mpsc::sync_channel::<(u32, u32)>(8);

        let (wakeup_read, wakeup_write) = wakeup_pipe().context("wakeup pipe")?;

        let child = cmd.spawn().context("spawn shell")?;
        let child_pid = Pid::from_raw(child.id() as i32);
        crate::utmp::add_record(master.as_raw_fd(), &hostname);

        let shared = Arc::new(PtyShared {
            id,
            hostname,
            title: Mutex::new(pts_name.clone()),
            pts_name,
            created_at: SystemTime::now(),
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            generation: AtomicU64::new(0),
            subscribers: RwLock::new(HashMap::new()),
            last_subscribed_at: Mutex::new(None),
            sort_order: AtomicU32::new(0), // real value assigned under the registry lock below
        });

        let handle = Arc::new(PtyHandle {
            shared: shared.clone(),
            tx: tx.clone(),
            writer: Mutex::new(File::from(master)),
            refresh_tx,
            scrollback_tx,
            resize_tx,
            meta_tx: meta_tx.clone(),
            child_pid,
            wakeup_write,
        });

        let master_reader = File::from(master_reader);
        std::thread::Builder::new()
            .name(format!("pty-reader-{id:016x}"))
            .spawn(move || reader_thread(
                master_reader, tx, refresh_rx, scrollback_rx, resize_rx,
                wakeup_read, child, meta_tx, shared,
            ))
            .context("spawn reader thread")?;
```

- [ ] **Step 5: Update reader.rs to consume PtyShared**

New signature (15 params → 9):

```rust
#[allow(clippy::too_many_arguments)]
pub(super) fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<PtyEvent>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
    scrollback_rx: std::sync::mpsc::Receiver<ScrollbackReq>,
    resize_rx: std::sync::mpsc::Receiver<(u32, u32)>,
    wakeup_read: OwnedFd,
    mut child: std::process::Child,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    shared: Arc<PtyShared>,
)
```

Body changes (everything else stays):
- `Terminal::new` dims: `cols: shared.cols.load(Ordering::Relaxed) as u16` (same for rows); `init_cols`/`init_rows` locals are gone — `current_cols`/`current_rows` initialize from the same loads.
- Title callback: `let shared_cb = shared.clone();` and the closure body writes `*shared_cb.title.lock().unwrap() = t.to_string();`.
- `prev_title` initializes to `shared.pts_name.clone()`.
- Every `&generation` becomes `&shared.generation` (the `flush_refreshes`/`process_read` free-function signatures are unchanged).
- `ClosedNotifier` fields become `{ meta_tx: broadcast::Sender<Arc<PtyMetadata>>, shared: Arc<PtyShared>, exit_code: Option<i32> }`; its `Drop` reads `self.shared.generation`, `self.shared.id`, `self.shared.created_at`.
- The TitleChanged emission replaces its hand-built `PtyInfo { ... subscribers: None, sort_order: 0 ... }` with:

```rust
        let current_title = shared.title.lock().unwrap().clone();
        if current_title != prev_title {
            prev_title = current_title;
            let _ = meta_tx.send(Arc::new(PtyMetadata {
                reason: MetadataReason::TitleChanged,
                exit_code: None,
                generation: gen,
                info: shared.info(),
            }));
        }
```

- The exit-message block reads the title via `shared.title.lock().unwrap().clone()`.
- Add `use super::PtyShared;` to the reader.rs import list; remove now-unused `Mutex`/`SystemTime` imports if the build warns.

- [ ] **Step 6: Verify and commit**

Run: `cargo test`
Expected: PASS, including the new `shared_info_reflects_title_and_dims`. The fabricated-fields assertion check: `grep -n "subscribers: None" src/pty/reader.rs` finds only `PtyInfo::closed`.

```bash
git add -A src/pty
git commit -m "refactor: consolidate handle/reader shared state into PtyShared"
```

---

### Task 4: Reader struct + single ReaderRequest channel (resize moves into the reader)

**Files:**
- Modify: `src/pty/reader.rs` (Reader struct, ReaderRequest, delete ScrollbackReq/ClosedNotifier)
- Modify: `src/pty/mod.rs` (PtyHandle request methods, create())
- Modify: `src/commands.rs` (`handle_resize` async, subscribe refit)
- Modify: `src/server.rs` (dispatch awaits resize)
- Modify: `tests/integration.rs:358,387` (`.await` on resize)

- [ ] **Step 1: Write the failing test**

Append to `mod subscriber_tests` in `src/pty/mod.rs` (the module already has tokio in dev-deps via integration tests; unit async tests use `#[tokio::test]`):

```rust
    #[tokio::test]
    async fn resize_via_request_updates_state_and_broadcasts() {
        let reg = PtyRegistry::new();
        let h = reg.create(80, 24, None).unwrap();
        h.upsert_subscriber("sub-a", make_info("host1"));
        let mut meta_rx = h.meta_subscribe();

        h.resize(100, 30).await.unwrap();

        let info = h.info();
        assert_eq!((info.cols, info.rows), (100, 30));

        // The reader broadcast Resize metadata before replying, so it's queued.
        let meta = tokio::time::timeout(std::time::Duration::from_secs(5), meta_rx.recv())
            .await.expect("timed out").expect("meta channel closed");
        assert!(matches!(meta.reason, MetadataReason::Resize));
        assert_eq!((meta.info.cols, meta.info.rows), (100, 30));
        // Full info, not a fabricated stub: the subscriber map came through.
        assert_eq!(meta.info.subscribers.as_ref().unwrap().len(), 1);

        // The refresh path sees the new dimensions.
        let r = h.refresh().await.unwrap();
        assert_eq!((r.cols, r.rows), (100, 30));
        let _ = reg.destroy(h.id());
    }
```

Run: `cargo test --lib resize_via_request`
Expected: FAIL to compile — `resize` is not async (no `.await`).

- [ ] **Step 2: Define ReaderRequest and the Reader struct in reader.rs**

Delete `struct ScrollbackReq`. Add:

```rust
/// One request to the reader thread. Sent on an unbounded std mpsc channel
/// (sends from the tokio thread never block), followed by a one-byte write to
/// the wakeup pipe so a poll()-parked reader services it promptly. Requests
/// are handled strictly in channel order.
pub(crate) enum ReaderRequest {
    Resize {
        cols: u32,
        rows: u32,
        reply: oneshot::Sender<Result<()>>,
    },
    Refresh {
        reply: oneshot::Sender<Result<RefreshData>>,
    },
    Scrollback {
        subscriber_id: String,
        op: ScrollbackOp,
        amount: i32,
        row_count: u32,
        reply: oneshot::Sender<Result<ScrollbackData>>,
    },
}

/// Owns everything single-threaded about a PTY: the libghostty Terminal, the
/// master fd, scrollback pins, and the deferred-refresh state. Constructed and
/// driven on the dedicated pty-reader thread (Terminal is !Send).
pub(super) struct Reader {
    terminal: Terminal<'static, 'static>,
    master: File,
    wakeup_read: OwnedFd,
    req_rx: std::sync::mpsc::Receiver<ReaderRequest>,
    tx: broadcast::Sender<PtyEvent>,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    shared: Arc<PtyShared>,
    child: std::process::Child,
    // One scrollback pin per subscriber; the pin marks the viewport's top row and
    // libghostty keeps it on its content across appends and eviction.
    pins: HashMap<String, TrackedGridRef>,
    // Refreshes are deferred until the VT parser is at a ground boundary, so a
    // snapshot never pins a generation in the middle of an escape sequence.
    pending_replies: Vec<oneshot::Sender<Result<RefreshData>>>,
    pending_broadcast_refresh: bool,
    prev_title: String,
    prev_screen: Screen,
    // Set by cleanup() on a normal exit; Drop reports it (None after a panic).
    exit_code: Option<i32>,
}
```

- [ ] **Step 3: Implement Reader::new, run, handle_request, apply_resize, cleanup, Drop**

`reader_thread` and `ClosedNotifier` are deleted; their logic redistributes as follows. The `'main` loop body is the existing one with `self.` prefixes — only the regions shown here change structurally.

```rust
impl Reader {
    pub(super) fn new(
        master: File,
        wakeup_read: OwnedFd,
        req_rx: std::sync::mpsc::Receiver<ReaderRequest>,
        tx: broadcast::Sender<PtyEvent>,
        meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
        shared: Arc<PtyShared>,
        child: std::process::Child,
    ) -> Result<Self> {
        let mut terminal = Terminal::new(TerminalOptions {
            cols: shared.cols.load(Ordering::Relaxed) as u16,
            rows: shared.rows.load(Ordering::Relaxed) as u16,
            // Byte budget for scrollback page memory, NOT a line count. (existing
            // comment from reader_thread moves here verbatim)
            max_scrollback: 16_000_000,
        })?;
        let shared_cb = shared.clone();
        terminal.on_title_changed(move |term| {
            if let Ok(t) = term.title() {
                *shared_cb.title.lock().unwrap() = t.to_string();
            }
        })?;
        // Initialize prev_title to match the initial title (pts_name), so we don't
        // emit a spurious TitleChanged before the shell sets any title.
        let prev_title = shared.pts_name.clone();
        Ok(Self {
            terminal, master, wakeup_read, req_rx, tx, meta_tx, shared, child,
            pins: HashMap::new(),
            pending_replies: Vec::new(),
            pending_broadcast_refresh: false,
            prev_title,
            prev_screen: Screen::Primary,
            exit_code: None,
        })
    }

    pub(super) fn run(mut self) {
        let master_fd = self.master.as_raw_fd();
        let wakeup_fd = self.wakeup_read.as_raw_fd();
        let mut buf = [0u8; 4096];

        'main: loop {
            // Drain the wakeup pipe, then queued requests, before waiting for PTY data.
            let mut wake_byte = [0u8; 64];
            unsafe { libc::read(wakeup_fd, wake_byte.as_mut_ptr() as *mut libc::c_void, wake_byte.len()) };
            while let Ok(req) = self.req_rx.try_recv() {
                self.handle_request(req);
            }
            self.flush_refreshes_if_at_boundary();

            // ... existing poll() setup, EINTR/POLLHUP/stall-timeout handling,
            // and the master-read drain loop, verbatim with `self.` prefixes.
            // `terminal` → `self.terminal`, `pending_replies` → `self.pending_replies`,
            // `&generation` → `&self.shared.generation`, `master.read` → `self.master.read`.

            let gen = process_read(
                &mut self.terminal, Bytes::from(batch), &self.shared.generation, &self.tx,
                &mut self.pending_replies, &mut self.pending_broadcast_refresh,
            );

            // Screen / title checks: verbatim, with the TitleChanged emission from
            // Task 3 (shared.info()) and `self.prev_screen` / `self.prev_title`.

            self.flush_refreshes_if_at_boundary();
        }

        self.cleanup();
        // `self` drops here: Drop emits the Closed metadata.
    }

    /// If the parser is at a ground boundary, service deferred refreshes now;
    /// otherwise they wait for the batch that completes the open sequence.
    fn flush_refreshes_if_at_boundary(&mut self) {
        if (!self.pending_replies.is_empty() || self.pending_broadcast_refresh)
            && self.terminal.vt_at_boundary()
        {
            flush_refreshes(
                &self.terminal, &self.shared.generation, &self.tx,
                &mut self.pending_replies, &mut self.pending_broadcast_refresh, false,
            );
        }
    }

    fn handle_request(&mut self, req: ReaderRequest) {
        match req {
            ReaderRequest::Resize { cols, rows, reply } => {
                let _ = reply.send(self.apply_resize(cols, rows));
            }
            ReaderRequest::Refresh { reply } => {
                // Queued; serviced only at a VT ground boundary (or stall timeout).
                self.pending_replies.push(reply);
            }
            ReaderRequest::Scrollback { subscriber_id, op, amount, row_count, reply } => {
                let gen = self.shared.generation.load(Ordering::Relaxed);
                let cols = self.shared.cols.load(Ordering::Relaxed);
                let _ = reply.send(do_scrollback(
                    &mut self.terminal, &mut self.pins, &subscriber_id,
                    op, amount, row_count, gen, cols,
                ));
            }
        }
    }

    /// Kernel resize, VT resize, shared-dims update, and the Resize broadcast all
    /// happen here, on one thread, in one order — there is no window where the
    /// libghostty Terminal disagrees with the real PTY size.
    fn apply_resize(&mut self, cols: u32, rows: u32) -> Result<()> {
        use nix::pty::Winsize;
        let ws = Winsize {
            ws_col: cols as u16,
            ws_row: rows as u16,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let ret = unsafe { libc::ioctl(self.master.as_raw_fd(), libc::TIOCSWINSZ, &ws as *const Winsize) };
        if ret < 0 {
            return Err(anyhow!("TIOCSWINSZ failed: {}", std::io::Error::last_os_error()));
        }
        self.terminal.resize(cols as u16, rows as u16, 0, 0)?;
        self.shared.cols.store(cols, Ordering::Relaxed);
        self.shared.rows.store(rows, Ordering::Relaxed);
        // Defer the redraw to a ground boundary (serviced by the run loop).
        self.pending_broadcast_refresh = true;
        let _ = self.meta_tx.send(Arc::new(PtyMetadata {
            reason: MetadataReason::Resize,
            exit_code: None,
            generation: self.shared.generation.load(Ordering::Relaxed),
            info: self.shared.info(),
        }));
        Ok(())
    }

    /// Post-loop teardown: reap the child, announce the exit on the data stream,
    /// then serve everything still queued against the final terminal state.
    fn cleanup(&mut self) {
        let status = self.child.try_wait().ok().flatten().or_else(|| self.child.wait().ok());
        crate::utmp::remove_record(self.master.as_raw_fd());
        let exit_msg = {
            let title = self.shared.title.lock().unwrap().clone();
            match &status {
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
        let gen = self.shared.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.tx.send(PtyEvent::Data(PtyChunk {
            generation: gen,
            data: Bytes::from(exit_msg.into_bytes()),
        }));
        self.exit_code = status.as_ref().and_then(|s| s.code());

        // Refreshes deferred at exit: the terminal is final, render directly
        // without waiting for a boundary.
        for reply in self.pending_replies.drain(..) {
            let gen = self.shared.generation.load(Ordering::Relaxed);
            let _ = reply.send(do_refresh(&self.terminal, gen));
        }
        // One drain for everything still queued — replaces the per-channel loops.
        while let Ok(req) = self.req_rx.try_recv() {
            let gen = self.shared.generation.load(Ordering::Relaxed);
            match req {
                ReaderRequest::Refresh { reply } => {
                    let _ = reply.send(do_refresh(&self.terminal, gen));
                }
                ReaderRequest::Scrollback { subscriber_id, op, amount, row_count, reply } => {
                    let cols = self.shared.cols.load(Ordering::Relaxed);
                    let _ = reply.send(do_scrollback(
                        &mut self.terminal, &mut self.pins, &subscriber_id,
                        op, amount, row_count, gen, cols,
                    ));
                }
                ReaderRequest::Resize { reply, .. } => {
                    let _ = reply.send(Err(anyhow!("PTY closed")));
                }
            }
        }
    }
}

/// Sole emitter of the Closed metadata — fires on a clean return from run()
/// (after cleanup() set exit_code) and on an unwind (exit_code None), so a
/// panicking reader still tells attached clients the PTY is gone. Replaces the
/// old ClosedNotifier mirror struct. Deliberately touches neither the
/// libghostty Terminal (unsafe mid-panic) nor utmp (leak on panic tolerated;
/// the normal path removes it in cleanup()).
impl Drop for Reader {
    fn drop(&mut self) {
        let _ = self.meta_tx.send(Arc::new(PtyMetadata {
            reason:     MetadataReason::Closed,
            exit_code:  self.exit_code,
            generation: self.shared.generation.load(Ordering::Relaxed),
            info:       PtyInfo::closed(self.shared.id, self.shared.created_at),
        }));
    }
}
```

Notes for the verbatim regions of `run()`:
- The old `resize_rx`/`refresh_rx`/`scrollback_rx` `try_recv` loops at the top of the old `'main` loop are **replaced** by the single `handle_request` drain shown above. The boundary-gated flush that followed them is now `flush_refreshes_if_at_boundary()`.
- `current_cols`/`current_rows` locals are deleted; the only consumer left (the old in-loop `do_scrollback` call) now lives in `handle_request` and reads `self.shared.cols`.
- The EOF/error/`POLLHUP` `break 'main` paths are unchanged.
- The `drop(wakeup_read)` comment line at the end of the old function is obsolete (field drop) — delete it.

- [ ] **Step 4: Rewrite PtyHandle request methods in mod.rs**

Replace `refresh_tx`/`scrollback_tx`/`resize_tx` fields with `req_tx: std::sync::mpsc::Sender<ReaderRequest>`, update the `use reader::…` line to `use reader::{Reader, ReaderRequest};`, and replace the `resize`/`refresh`/`scrollback`/`close_scrollback` methods:

```rust
    /// One-byte poke so a poll()-parked reader notices a queued request promptly.
    fn wake(&self) {
        let wfd = self.wakeup_write.as_raw_fd();
        let ret = unsafe { libc::write(wfd, [1u8].as_ptr() as *const libc::c_void, 1) };
        if ret < 0 {
            tracing::debug!("wakeup write failed: {}", std::io::Error::last_os_error());
        }
    }

    fn request_resize(&self, cols: u32, rows: u32) -> Result<oneshot::Receiver<Result<()>>> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReaderRequest::Resize { cols, rows, reply: tx })
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
        Ok(rx)
    }

    pub async fn resize(&self, cols: u32, rows: u32) -> Result<()> {
        let rx = self.request_resize(cols, rows)?;
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped resize response"))?
    }

    /// Fire-and-forget resize for callers that don't care about the outcome
    /// (subscriber refit). Dropping the reply receiver is fine — the reader's
    /// reply send becomes a no-op.
    pub fn resize_detached(&self, cols: u32, rows: u32) {
        let _ = self.request_resize(cols, rows);
    }

    pub async fn refresh(&self) -> Result<RefreshData> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReaderRequest::Refresh { reply: tx })
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped refresh response"))?
    }

    pub async fn scrollback(
        &self,
        subscriber_id: &str,
        op: ScrollbackOp,
        amount: i32,
        row_count: u32,
    ) -> Result<ScrollbackData> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReaderRequest::Scrollback {
                subscriber_id: subscriber_id.to_owned(), op, amount, row_count, reply: tx,
            })
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped scrollback response"))?
    }

    /// Best-effort release of a subscriber's scrollback pin (teardown paths).
    pub fn close_scrollback(&self, subscriber_id: &str) {
        let (tx, _rx) = oneshot::channel();
        let _ = self.req_tx.send(ReaderRequest::Scrollback {
            subscriber_id: subscriber_id.to_owned(),
            op: ScrollbackOp::Close, amount: 0, row_count: 0, reply: tx,
        });
        self.wake();
    }
```

The old `resize()` body (ioctl + atomics + metadata broadcast) is deleted from the handle — `apply_resize` owns it now.

- [ ] **Step 5: Update create() channel setup and thread spawn**

Replace the three `sync_channel` lines with:

```rust
        let (req_tx, req_rx) = std::sync::mpsc::channel::<ReaderRequest>();
```

Handle construction uses `req_tx`. Thread spawn becomes:

```rust
        let master_reader = File::from(master_reader);
        let meta_tx_spawn = meta_tx.clone();
        let shared_spawn = shared.clone();
        std::thread::Builder::new()
            .name(format!("pty-reader-{id:016x}"))
            .spawn(move || {
                match Reader::new(
                    master_reader, wakeup_read, req_rx, tx, meta_tx, shared, child,
                ) {
                    Ok(r) => r.run(),
                    Err(e) => {
                        // Startup failure: there is no Reader (and so no Drop) yet.
                        // Emit Closed ourselves so the PTY doesn't sit listed forever
                        // with a dead reader behind it.
                        tracing::warn!("PTY reader failed to start: {e}");
                        let _ = meta_tx_spawn.send(Arc::new(PtyMetadata {
                            reason:     MetadataReason::Closed,
                            exit_code:  None,
                            generation: 0,
                            info:       PtyInfo::closed(shared_spawn.id, shared_spawn.created_at),
                        }));
                    }
                }
            })
            .context("spawn reader thread")?;
```

(`tx`/`meta_tx`/`shared` move into the closure; the handle already took its own clones. `PtyInfo::closed` is `pub(super)` in reader.rs, callable from mod.rs.)

- [ ] **Step 6: Update callers**

`src/commands.rs` — `handle_resize` becomes async:

```rust
pub async fn handle_resize(registry: &PtyRegistry, req: ResizeRequest) -> TerminalResponse {
    let id = req.pty_id;
    match registry.get(id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.resize(req.cols, req.rows).await {
            Ok(_) => ok_response(id),
            Err(e) => err_response(id, e.to_string()),
        },
    }
}
```

`src/commands.rs` — in `handle_subscribe`, the refit call becomes:

```rust
                if let Some((cols, rows)) = refit_target((snapshot.cols, snapshot.rows), best, allow_shrink) {
                    handle.resize_detached(cols, rows);
                }
```

`src/server.rs` — `dispatch_command`:

```rust
        Some(Command::Resize(r))       => commands::handle_resize(registry, r).await,
```

`tests/integration.rs:358` and `:387` — `handle.resize(120, 40).unwrap();` becomes `handle.resize(120, 40).await.unwrap();`.

- [ ] **Step 7: Verify and commit**

Run: `cargo test`
Expected: PASS — all moved suites, `resize_via_request_updates_state_and_broadcasts`, and the existing integration tests `test_resize_broadcasts_metadata` / `test_resize_broadcasts_refresh_event` (these prove the resize→metadata and resize→Refresh-broadcast behaviors survived the move into the reader).

```bash
git add -A src/pty src/commands.rs src/server.rs tests/integration.rs
git commit -m "refactor: single ReaderRequest channel; resize executes on the reader thread"
```

---

### Task 5: Write path — reader owns the only master fd, buffers on EAGAIN

**Files:**
- Modify: `src/pty/reader.rs` (Write variant, queue/flush functions + tests, POLLOUT)
- Modify: `src/pty/mod.rs` (drop `writer` and the master dup; `write()` becomes a send)
- Modify: `src/commands.rs` (`handle_write` passes through unchanged — verify only)

- [ ] **Step 1: Write the failing tests**

Append to `src/pty/reader.rs`:

```rust
#[cfg(test)]
mod write_buffer_tests {
    use super::*;
    use std::io::Read;

    // Non-blocking pipe pair standing in for the PTY master: identical EAGAIN
    // semantics when the kernel buffer fills.
    fn pipe_pair() -> (File, File) {
        let (r, w) = super::super::wakeup_pipe().unwrap();
        (File::from(r), File::from(w))
    }

    fn drain(read_end: &mut File) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match read_end.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&buf[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("pipe read failed: {e}"),
            }
        }
        out
    }

    #[test]
    fn queue_write_writes_directly_when_buffer_empty() {
        let (mut r, w) = pipe_pair();
        let mut pending = Vec::new();
        queue_write(&w, &mut pending, b"hello", MAX_PENDING_INPUT);
        assert!(pending.is_empty(), "small write must go straight through");
        assert_eq!(drain(&mut r), b"hello");
    }

    #[test]
    fn queue_write_buffers_tail_on_full_pipe_and_flush_preserves_order() {
        let (mut r, w) = pipe_pair();
        let mut pending = Vec::new();
        // Fill the pipe until a tail lands in the buffer.
        let chunk = [b'a'; 4096];
        while pending.is_empty() {
            queue_write(&w, &mut pending, &chunk, MAX_PENDING_INPUT);
        }
        // With bytes pending, later writes append instead of jumping the queue.
        queue_write(&w, &mut pending, b"WORLD", MAX_PENDING_INPUT);
        assert!(pending.ends_with(b"WORLD"));

        // Drain the pipe, flush, repeat: every byte arrives, in order.
        let mut received = drain(&mut r);
        while !pending.is_empty() {
            flush_pending(&w, &mut pending);
            received.extend_from_slice(&drain(&mut r));
        }
        assert!(received.ends_with(b"WORLD"), "buffered tail must arrive last");
        let body = &received[..received.len() - 5];
        assert!(body.iter().all(|&b| b == b'a'), "no reordering or corruption");
    }

    #[test]
    fn queue_write_drops_beyond_cap_keeps_buffer_intact() {
        let (_r, w) = pipe_pair();
        let mut pending = vec![b'x'; 10]; // non-empty => no direct-write path
        queue_write(&w, &mut pending, b"toolarge", 12); // 10 + 8 > 12: dropped
        assert_eq!(pending.len(), 10);
        queue_write(&w, &mut pending, b"ok", 12);       // 10 + 2 <= 12: appended
        assert_eq!(pending.len(), 12);
        assert!(pending.ends_with(b"ok"));
    }
}
```

Run: `cargo test --lib write_buffer`
Expected: FAIL to compile — `queue_write`, `flush_pending`, `MAX_PENDING_INPUT` don't exist. (`wakeup_pipe` is a private fn in the parent module; child modules can call it — no visibility change needed.)

- [ ] **Step 2: Implement the buffering functions in reader.rs**

```rust
/// Hard cap on buffered PTY input (~16x the kernel input queue). Past this the
/// child has stopped reading; dropping new input with a warning beats unbounded
/// growth — the loss now at least shows up in the logs, unlike the old silent
/// EAGAIN drop.
const MAX_PENDING_INPUT: usize = 1 << 20;

/// Write as much as the PTY accepts without blocking. Ok(n) may be short of
/// data.len() — that's EAGAIN, and the caller buffers the tail. Err is a real
/// error (PTY torn down), never WouldBlock.
fn write_some(master: &File, data: &[u8]) -> std::io::Result<usize> {
    use std::io::Write;
    let mut written = 0;
    while written < data.len() {
        match (&*master).write(&data[written..]) {
            Ok(0) => break,
            Ok(n) => written += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(written)
}

/// Queue PTY input: write immediately when nothing is pending (preserving
/// order), buffer the unwritten tail for the POLLOUT path.
fn queue_write(master: &File, pending_out: &mut Vec<u8>, data: &[u8], cap: usize) {
    let mut tail: &[u8] = data;
    if pending_out.is_empty() {
        match write_some(master, data) {
            Ok(n) => tail = &data[n..],
            Err(e) => {
                tracing::debug!("PTY write failed: {e}");
                return;
            }
        }
    }
    if tail.is_empty() {
        return;
    }
    if pending_out.len() + tail.len() > cap {
        tracing::warn!(
            pending = pending_out.len(),
            dropped = tail.len(),
            "PTY input buffer full; dropping write"
        );
        return;
    }
    pending_out.extend_from_slice(tail);
}

/// Drain as much pending input as the PTY accepts. On a real error the
/// remainder is discarded — the PTY is gone and the read side will surface
/// the same failure to the main loop.
fn flush_pending(master: &File, pending_out: &mut Vec<u8>) {
    match write_some(master, pending_out) {
        Ok(n) => { pending_out.drain(..n); }
        Err(e) => {
            tracing::debug!("PTY write failed: {e}");
            pending_out.clear();
        }
    }
}
```

Run: `cargo test --lib write_buffer`
Expected: PASS (3 tests).

- [ ] **Step 3: Wire Write into ReaderRequest and the poll loop**

Add the variant (first position — it's the hot path):

```rust
pub(crate) enum ReaderRequest {
    /// PTY input. Fire-and-forget: the reader writes it through, buffering the
    /// unwritten tail on EAGAIN and draining it via POLLOUT.
    Write(Bytes),
    // ... existing variants unchanged
}
```

`Reader` gains a field: `pending_out: Vec<u8>` (initialize to `Vec::new()` in `new()`).

In `handle_request`:

```rust
            ReaderRequest::Write(data) => {
                queue_write(&self.master, &mut self.pending_out, &data, MAX_PENDING_INPUT);
            }
```

In `run()`, after the request-drain loop, add the opportunistic flush:

```rust
            if !self.pending_out.is_empty() {
                flush_pending(&self.master, &mut self.pending_out);
            }
```

Change the poll setup so the master entry asks for POLLOUT while output is pending:

```rust
            let mut master_events = libc::POLLIN;
            if !self.pending_out.is_empty() {
                master_events |= libc::POLLOUT;
            }
            let mut pfds = [
                libc::pollfd { fd: master_fd, events: master_events, revents: 0 },
                libc::pollfd { fd: wakeup_fd, events: libc::POLLIN, revents: 0 },
            ];
```

And in the revents handling, flush **before** the existing not-readable `continue` gate:

```rust
            if pfds[0].revents & libc::POLLOUT != 0 {
                flush_pending(&self.master, &mut self.pending_out);
            }
            // Only read from PTY master if it actually has data ready. (existing
            // comment + POLLIN/POLLHUP/POLLERR gate unchanged below)
```

In `cleanup()`'s final drain, add:

```rust
                ReaderRequest::Write(_) => {} // child is gone; input has nowhere to go
```

- [ ] **Step 4: Single master fd — remove the dup and the handle's writer**

In `PtyRegistry::create` (`src/pty/mod.rs`):

Replace the dup-and-O_NONBLOCK block:

```rust
        // Dup master for the reader thread before transferring ownership to File
        let master_reader = dup_fd(master.as_raw_fd()).context("dup master fd for reader")?;
        // Set O_NONBLOCK so the reader can drain all available bytes in a loop
        let flags = unsafe { libc::fcntl(master_reader.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(master_reader.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error()).context("set O_NONBLOCK on master reader fd");
        }
```

with:

```rust
        // Single master fd, owned by the reader thread. O_NONBLOCK so the reader
        // drains reads in a loop, and so PTY-input writes fail fast with EAGAIN
        // into the pending buffer instead of ever blocking the thread.
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error()).context("set O_NONBLOCK on master fd");
        }
```

- Drop `master_reader.as_raw_fd()` from the FD_CLOEXEC loop's array.
- Remove `writer: Mutex::new(File::from(master)),` from the handle construction and the `writer` field from `PtyHandle`.
- The spawn block's `let master_reader = File::from(master_reader);` becomes `let master = File::from(master);` (after `utmp::add_record(master.as_raw_fd(), …)`, which stays before the conversion), and `Reader::new(master, …)`.
- `dup_fd` is still used for the slave stdout/stderr dups — it stays.

Replace `PtyHandle::write`:

```rust
    /// Queue input for the PTY. Fire-and-forget: the reader thread delivers it
    /// (buffering on EAGAIN); Err only means the reader is gone, which clients
    /// also learn via the Closed metadata.
    pub fn write(&self, data: &[u8]) -> Result<()> {
        self.req_tx
            .send(ReaderRequest::Write(Bytes::copy_from_slice(data)))
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
        Ok(())
    }
```

(Signature unchanged — `commands::handle_write` and the `handle.write(b"…")` calls in `tests/integration.rs` compile as-is.)

- [ ] **Step 5: Verify and commit**

Run: `cargo test`
Expected: PASS — write_buffer tests plus the existing integration tests that type into PTYs (`test_*` using `handle.write`) prove end-to-end input still works through the new path.

Run: `cargo clippy --all-targets`
Expected: no new warnings.

```bash
git add -A src/pty
git commit -m "feat: route PTY writes through the reader thread with EAGAIN buffering"
```

---

### Task 6: Proto + enum plumbing for DATA_LOST

**Files:**
- Modify: `proto/terminal.proto:120-125`
- Modify: `src/pty/mod.rs` (MetadataReason)
- Modify: `src/server.rs` (reason mapping)

- [ ] **Step 1: Add the proto value**

```proto
enum StreamMetadataReason {
  RESIZE              = 0;
  CLOSED              = 1;
  TITLE_CHANGED       = 2;
  SUBSCRIBERS_CHANGED = 3;
  // This subscriber lagged the server's broadcast and stream data was lost.
  // The client should request a Refresh to resynchronize its screen.
  DATA_LOST           = 4;
}
```

- [ ] **Step 2: Add the Rust variant and mapping**

`src/pty/mod.rs`:

```rust
#[derive(Clone, Debug)]
pub enum MetadataReason {
    Resize,
    Closed,
    TitleChanged,
    SubscribersChanged,
    /// Synthesized per-subscriber by the forwarding task on broadcast lag —
    /// never emitted by the reader thread.
    DataLost,
}
```

`src/server.rs`, in the `PtyEvent::Metadata` arm's reason match:

```rust
                                    MetadataReason::DataLost           => StreamMetadataReason::DataLost,
```

- [ ] **Step 3: Verify and commit**

Run: `cargo test`
Expected: PASS (build.rs regenerates the proto; exhaustive matches updated).

```bash
git add proto/terminal.proto src/pty/mod.rs src/server.rs
git commit -m "feat: add DATA_LOST stream metadata reason"
```

---

### Task 7: Extract the forwarding task; emit DataLost on lag

**Files:**
- Modify: `src/commands.rs` (forward_subscription, data_lost_event, handle_subscribe, tests)

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` in `src/commands.rs`:

```rust
    use std::sync::Arc;
    use std::time::Duration;
    use bytes::Bytes;
    use tokio::sync::broadcast;
    use crate::pty::{PtyChunk, PtyEvent, PtyMetadata, PtyShared};

    fn test_shared() -> Arc<PtyShared> {
        use std::sync::atomic::{AtomicU32, AtomicU64};
        Arc::new(PtyShared {
            id: 7,
            hostname: String::new(),
            pts_name: String::new(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            cols: AtomicU32::new(80),
            rows: AtomicU32::new(24),
            title: std::sync::Mutex::new(String::new()),
            generation: AtomicU64::new(0),
            subscribers: std::sync::RwLock::new(std::collections::HashMap::new()),
            last_subscribed_at: std::sync::Mutex::new(None),
            sort_order: AtomicU32::new(0),
        })
    }

    #[tokio::test]
    async fn forwarding_passes_data_through() {
        let (data_tx, data_rx) = broadcast::channel::<PtyEvent>(8);
        let (meta_tx, meta_rx) = broadcast::channel::<Arc<PtyMetadata>>(8);
        let (sub_tx, mut sub_rx) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(forward_subscription(9, test_shared(), data_rx, meta_rx, sub_tx));

        data_tx.send(PtyEvent::Data(PtyChunk { generation: 1, data: Bytes::from_static(b"hi") })).unwrap();
        let (pty_id, ev) = tokio::time::timeout(Duration::from_secs(5), sub_rx.recv())
            .await.expect("timed out").expect("channel closed");
        assert_eq!(pty_id, 9);
        assert!(matches!(ev, PtyEvent::Data(c) if c.data.as_ref() == b"hi"));

        drop(data_tx);
        drop(meta_tx);
        task.await.unwrap(); // both broadcasts closed => task exits
    }

    #[tokio::test]
    async fn forwarding_emits_data_lost_on_lag() {
        let (data_tx, data_rx) = broadcast::channel::<PtyEvent>(8);
        let (meta_tx, meta_rx) = broadcast::channel::<Arc<PtyMetadata>>(8);
        let (sub_tx, mut sub_rx) = tokio::sync::mpsc::channel(64);

        // Overflow the 8-slot ring before the task starts draining: the
        // receiver's first poll yields Lagged(12).
        for i in 0..20u64 {
            data_tx.send(PtyEvent::Data(PtyChunk {
                generation: i,
                data: Bytes::from_static(b"x"),
            })).unwrap();
        }
        let task = tokio::spawn(forward_subscription(7, test_shared(), data_rx, meta_rx, sub_tx));

        let mut saw_data_lost = false;
        for _ in 0..25 {
            match tokio::time::timeout(Duration::from_secs(5), sub_rx.recv()).await {
                Ok(Some((pty_id, PtyEvent::Metadata(m))))
                    if matches!(m.reason, MetadataReason::DataLost) =>
                {
                    assert_eq!(pty_id, 7);
                    assert_eq!(m.info.id, 7, "DataLost must carry real PtyShared info");
                    saw_data_lost = true;
                    break;
                }
                Ok(Some(_)) => continue,
                _ => break,
            }
        }
        assert!(saw_data_lost, "lagged subscriber must receive a DataLost metadata");

        drop(data_tx);
        drop(meta_tx);
        task.await.unwrap();
    }
```

Run: `cargo test --lib forwarding`
Expected: FAIL to compile — `forward_subscription` doesn't exist.

- [ ] **Step 2: Extract and implement**

In `src/commands.rs`, add (top-level, above `handle_subscribe`):

```rust
use std::sync::atomic::Ordering;
use tokio::sync::broadcast;
use crate::pty::PtyShared;   // PtyChunk is test-only; the test module imports it itself

/// The per-subscriber DataLost event: this subscriber fell behind the broadcast
/// and events were dropped. Carries real PtyInfo from the shared state (the
/// forwarding task holds Arc<PtyShared>, deliberately NOT Arc<PtyHandle> —
/// that would keep wakeup_write alive and break destroy-by-drop).
fn data_lost_event(shared: &PtyShared) -> PtyEvent {
    PtyEvent::Metadata(Arc::new(PtyMetadata {
        reason:     MetadataReason::DataLost,
        exit_code:  None,
        generation: shared.generation.load(Ordering::Relaxed),
        info:       shared.info(),
    }))
}

/// Forwards one PTY's data + metadata broadcasts to a connection's event queue,
/// tagging each with the pty_id. On broadcast lag the dropped events are gone
/// for good, so the subscriber is told explicitly (DataLost) and is expected to
/// recover by requesting a Refresh. Runs until both broadcasts close or the
/// connection's queue drops.
pub(crate) async fn forward_subscription(
    pty_id:  u64,
    shared:  Arc<PtyShared>,
    data_rx: broadcast::Receiver<PtyEvent>,
    meta_rx: broadcast::Receiver<Arc<PtyMetadata>>,
    tx:      tokio::sync::mpsc::Sender<(u64, PtyEvent)>,
) {
    use tokio_stream::{StreamExt, wrappers::{BroadcastStream, errors::BroadcastStreamRecvError}};
    let mut data_stream = BroadcastStream::new(data_rx);
    let mut meta_stream = BroadcastStream::new(meta_rx);
    loop {
        tokio::select! {
            item = data_stream.next() => match item {
                Some(Ok(event)) => {
                    if tx.send((pty_id, event)).await.is_err() { break; }
                }
                Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                    tracing::warn!(pty_id = format!("{:016x}", pty_id), skipped = n, "data broadcast lagged");
                    if tx.send((pty_id, data_lost_event(&shared))).await.is_err() { break; }
                }
                None => break,
            },
            item = meta_stream.next() => match item {
                Some(Ok(meta)) => {
                    if tx.send((pty_id, PtyEvent::Metadata(meta))).await.is_err() { break; }
                }
                Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                    tracing::warn!(pty_id = format!("{:016x}", pty_id), skipped = n, "meta broadcast lagged");
                    // A lagged-away Resize/TitleChanged is also repaired by a
                    // refresh (RefreshResponse carries cols/rows).
                    if tx.send((pty_id, data_lost_event(&shared))).await.is_err() { break; }
                }
                None => break,
            },
        }
    }
}
```

Replace the inline `tokio::spawn(async move { … })` block in `handle_subscribe` (the whole `let data_rx = … sub_tasks.insert(id, task);` body inside `if !subscribed_ids.contains(&id)`) with:

```rust
            if !subscribed_ids.contains(&id) {
                let task = tokio::spawn(forward_subscription(
                    id,
                    handle.shared(),
                    handle.subscribe(),
                    handle.meta_subscribe(),
                    sub_tx.clone(),
                ));
                sub_tasks.insert(id, task);
                subscribed_ids.insert(id);
            }
```

- [ ] **Step 3: Verify and commit**

Run: `cargo test --lib forwarding`
Expected: PASS (2 tests).

Run: `cargo test`
Expected: PASS — `tests/integration.rs` subscription tests prove the extracted task still forwards in the real server path.

```bash
git add src/commands.rs
git commit -m "feat: extract subscription forwarding; emit DataLost on broadcast lag"
```

---

### Task 8: Client refresh-on-DataLost

**Files:**
- Modify: `src/attach/mod.rs` (render loop in `run()`)

No unit test: the render loop reads real stdin/stdout and a live tonic stream (known, deferred testability concern — see spec). Verification is the full suite plus the manual check in Task 9.

- [ ] **Step 1: Add the refresh_pending flag**

In `run()`, directly after `let mut current_refresh_gen = refresh_gen;` (attach/mod.rs:717):

```rust
            // True while a lag-recovery Refresh request is in flight, so a
            // persistently slow link can't amplify lag into a refresh storm.
            let mut refresh_pending = false;
```

- [ ] **Step 2: Clear it on any refresh**

In the `Some(Response::Refresh(rf)) if rf.pty_id == current_pty_id` arm, after `current_refresh_gen = rf.generation;`:

```rust
                                    refresh_pending = false;
```

- [ ] **Step 3: React to DataLost**

In the `Some(Response::Metadata(m)) if m.pty_id == current_pty_id` arm, extend the reason chain (after the `Closed` branch):

```rust
                                    } else if m.reason == StreamMetadataReason::DataLost as i32 {
                                        // We lagged the server's broadcast and lost stream
                                        // bytes; the screen may be corrupt. Ask for a full
                                        // snapshot — every render mode recovers completely
                                        // from a Refresh, and the generation filter drops
                                        // any straggler chunks from before the snapshot.
                                        if !refresh_pending {
                                            refresh_pending = true;
                                            let _ = cmd_tx.send(TerminalCommand {
                                                command: Some(Command::Refresh(RefreshRequest {
                                                    pty_id: current_pty_id,
                                                })),
                                            }).await;
                                        }
                                    }
```

- [ ] **Step 4: Verify and commit**

Run: `cargo test`
Expected: PASS.

Run: `cargo build --release`
Expected: clean build (the client path is exercised manually in Task 9).

```bash
git add src/attach/mod.rs
git commit -m "feat: attach client requests a refresh on DataLost"
```

---

### Task 9: Final verification

**Files:**
- Possibly modify: `docs/*.md` (stale references)

- [ ] **Step 1: Full suite + lints**

Run: `cargo test && cargo clippy --all-targets -- -D warnings`
Expected: PASS / clean. Test count ≥ baseline from Task 1 plus the 7 new tests (1 PtyShared, 1 resize, 3 write-buffer, 2 forwarding).

- [ ] **Step 2: Sweep docs for stale references**

Run: `grep -rn "ClosedNotifier\|resize_tx\|refresh_tx\|scrollback_tx\|reader_thread" docs/ README.md src/`
Expected: no hits in `src/`. For hits in `docs/` (e.g. REAP.md or REFRESH.md naming `ClosedNotifier`/`reader_thread`), update the names to `Reader`/`ReaderRequest` in place — terminology only, no content rewrites.

- [ ] **Step 3: Manual smoke test**

```bash
./run-termd   # or: cargo run -- start &
cargo run -- attach            # auto-creates a PTY
# in the attached shell: run `vim`, resize the terminal window, `:q`,
# then `seq 200000` while the session stays responsive; C-a d to detach.
```

Expected: typing, resize-refit, title updates, and detach all behave as before. Optional lag check: temporarily change `broadcast::channel::<PtyEvent>(512)` to `(8)` in `create()`, run `seq 200000`, and confirm the screen ends in a clean repaint (the DataLost→Refresh path) rather than torn output; revert the constant afterward.

- [ ] **Step 4: Commit any doc fixes**

```bash
git add docs/
git commit -m "docs: update reader-thread terminology after restructure"
```

(Skip if Step 2 found nothing.)

---

## Self-review notes

- **Spec coverage:** module layout → Tasks 1-2; PtyShared → Task 3; ReaderRequest/ordering/unbounded channel → Task 4; resize-in-reader + async ack + detached refit → Task 4; write buffering/cap/POLLOUT/sole fd owner → Task 5; cleanup single-drain + Drop-emitted Closed + startup-failure Closed → Task 4; proto DATA_LOST → Task 6; forwarding extraction + both lag arms + Arc<PtyShared> invariant → Task 7; client refresh_pending → Task 8; spec test items 1-5 → Tasks 1-5 baselines, 5, 4, 7, 9 respectively.
- **Known churn accepted:** `reader_thread`'s signature changes in Task 3 and is then replaced in Task 4 — each commit builds green, and merging the tasks was judged worse (two structural changes in one review unit).
- **Type registry (cross-task consistency):** `PtyShared` (fields pub(crate)), `PtyHandle::shared() -> Arc<PtyShared>`, `ReaderRequest::{Write(Bytes), Resize{cols,rows,reply}, Refresh{reply}, Scrollback{subscriber_id,op,amount,row_count,reply}}`, `Reader::{new, run, cleanup, handle_request, apply_resize, flush_refreshes_if_at_boundary}`, free fns `write_some`/`queue_write`/`flush_pending`, consts `MAX_PENDING_INPUT`, `MetadataReason::DataLost`, `forward_subscription`, `data_lost_event`.
