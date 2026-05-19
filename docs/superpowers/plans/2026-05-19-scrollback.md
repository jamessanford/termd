# Scrollback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ScrollbackRequest`/`ScrollbackResponse` to the gRPC protocol so clients can retrieve VT-encoded scrollback history from a server-side PTY.

**Architecture:** The request flows through `proto/terminal.proto` → `src/server.rs` dispatch → `src/commands.rs` handler → `src/pty.rs` `PtyHandle::scrollback()`, which sends a request tuple over a new sync-channel to the reader thread. The reader thread calls `do_scrollback()` (using the libghostty Formatter) and replies via a oneshot. This is the exact same pattern as the existing `PtyHandle::refresh()` / `do_refresh()` pair.

**Tech Stack:** Rust, tonic/prost (gRPC), libghostty-vt (Formatter, Point::History, GridRef, Selection)

---

## File map

| File | Change |
|---|---|
| `proto/terminal.proto` | Add `ScrollbackRequest` message, `ScrollbackResponse` message, field 10 in `TerminalCommand`, field 7 in `TerminalResponse` |
| `src/pty.rs` | Add `ScrollbackData` struct, `scrollback_tx` field on `PtyHandle`, `PtyHandle::scrollback()` method, `do_scrollback()` function, drain loop in reader thread |
| `src/commands.rs` | Add `handle_scrollback()` async function |
| `src/server.rs` | Add `Command::Scrollback` arm in dispatch match |

---

### Task 1: Proto — add ScrollbackRequest and ScrollbackResponse

**Files:**
- Modify: `proto/terminal.proto`

- [ ] **Step 1: Add the two new messages**

Open `proto/terminal.proto`. After the `RefreshRequest` line, add:

```proto
message ScrollbackRequest {
  string pty_id     = 1;
  // Distance from the active-screen edge, in rows.
  // 0 = the row immediately above the active screen (most recent history).
  uint32 row_offset = 2;
  // Maximum number of scrollback rows to return.
  uint32 row_count  = 3;
}
```

After the `RefreshResponse` message block, add:

```proto
message ScrollbackResponse {
  string pty_id                = 1;
  uint64 generation            = 2;
  bytes  data                  = 3;
  uint32 total_scrollback_rows = 4;
}
```

- [ ] **Step 2: Wire into TerminalCommand and TerminalResponse**

In the `TerminalCommand` oneof, add after field 9:
```proto
    ScrollbackRequest scrollback = 10;
```

In the `TerminalResponse` oneof, add after field 6:
```proto
    ScrollbackResponse scrollback = 7;
```

- [ ] **Step 3: Build to confirm protobuf compiles**

```bash
cargo build 2>&1 | head -30
```

Expected: no errors. The generated code will now include `Command::Scrollback` and `Response::Scrollback` variants.

- [ ] **Step 4: Commit**

```bash
git add proto/terminal.proto
git commit -m "feat(proto): add ScrollbackRequest and ScrollbackResponse"
```

---

### Task 2: pty.rs — ScrollbackData struct and PtyHandle wiring

**Files:**
- Modify: `src/pty.rs`

This task adds the channel, the `PtyHandle::scrollback()` method, and threads the channel through to the reader thread. It does NOT yet implement `do_scrollback()` (that's Task 3). After this task, `scrollback()` will compile and send a request but the reader thread will never drain the channel — that's fine for now.

- [ ] **Step 1: Add ScrollbackData struct**

Find the `RefreshData` struct in `src/pty.rs` (around line 70–78). Add immediately after it:

```rust
pub struct ScrollbackData {
    pub generation:            u64,
    pub data:                  Bytes,
    pub total_scrollback_rows: u32,
}
```

- [ ] **Step 2: Add scrollback_tx field to PtyHandle**

Find the `PtyHandle` struct (around line 82). Add `scrollback_tx` alongside `refresh_tx`:

```rust
    refresh_tx:    std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    scrollback_tx: std::sync::mpsc::SyncSender<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>,
    resize_tx:     std::sync::mpsc::SyncSender<(u32, u32)>,
```

- [ ] **Step 3: Add PtyHandle::scrollback() method**

Find the `PtyHandle::refresh()` method (around line 167). Add immediately after it:

```rust
    pub async fn scrollback(&self, row_offset: u32, row_count: u32) -> Result<ScrollbackData> {
        let (tx, rx) = oneshot::channel();
        self.scrollback_tx.send((row_offset, row_count, tx))
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        let wfd = self.wakeup_write.as_raw_fd();
        let ret = unsafe { libc::write(wfd, [1u8].as_ptr() as *const libc::c_void, 1) };
        if ret < 0 {
            tracing::debug!("wakeup write failed: {}", std::io::Error::last_os_error());
        }
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped scrollback response"))?
    }
```

- [ ] **Step 4: Create the channel and populate the PtyHandle**

Find the channel creation block in `PtyRegistry::create()` (around line 288):

```rust
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let (resize_tx, resize_rx) = std::sync::mpsc::sync_channel::<(u32, u32)>(8);
```

Add the scrollback channel alongside:

```rust
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let (scrollback_tx, scrollback_rx) =
            std::sync::mpsc::sync_channel::<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>(8);
        let (resize_tx, resize_rx) = std::sync::mpsc::sync_channel::<(u32, u32)>(8);
```

In the `PtyHandle { ... }` initializer (around line 315), add `scrollback_tx`:

```rust
        let handle = Arc::new(PtyHandle {
            // ... existing fields ...
            refresh_tx,
            scrollback_tx,
            resize_tx,
            // ... rest unchanged ...
        });
```

- [ ] **Step 5: Pass scrollback_rx to reader_thread**

Find the `std::thread::Builder::new()...spawn(move || reader_thread(...))` call (around line 336). Add `scrollback_rx` to the argument list:

```rust
            .spawn(move || reader_thread(
                master_reader, tx, generation, refresh_rx, scrollback_rx, resize_rx,
                wakeup_read, child, title_for_thread, cols, rows,
                meta_tx_for_thread, id_for_thread, hostname_for_thread,
                pts_name_for_thread, created_at,
            ))
```

Update the `reader_thread` function signature (around line 451) to accept the new parameter:

```rust
fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    generation: Arc<AtomicU64>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
    scrollback_rx: std::sync::mpsc::Receiver<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>,
    resize_rx: std::sync::mpsc::Receiver<(u32, u32)>,
    wakeup_read: OwnedFd,
    // ... rest unchanged ...
```

Leave `scrollback_rx` unused for now (it will be drained in Task 3). Suppress the unused-variable warning with a leading underscore if needed: `_scrollback_rx`.

- [ ] **Step 6: Build**

```bash
cargo build 2>&1 | head -30
```

Expected: no errors (possibly an `unused variable` warning for `_scrollback_rx` — that's fine).

- [ ] **Step 7: Commit**

```bash
git add src/pty.rs
git commit -m "feat(pty): add ScrollbackData, scrollback_tx channel, and PtyHandle::scrollback()"
```

---

### Task 3: pty.rs — implement do_scrollback and drain in reader thread

**Files:**
- Modify: `src/pty.rs`

- [ ] **Step 1: Write a failing test for do_scrollback clamping**

Add to the bottom of `src/pty.rs` (after the existing `// Reader thread` section, or alongside other inline tests if any exist):

```rust
#[cfg(test)]
mod scrollback_tests {
    use super::*;

    fn make_terminal(cols: u16, rows: u16, scrollback: usize) -> Terminal<'static, 'static> {
        Terminal::new(TerminalOptions { cols, rows, max_scrollback: scrollback }).unwrap()
    }

    #[test]
    fn do_scrollback_empty_when_no_history() {
        let terminal = make_terminal(80, 24, 1000);
        // No data written — scrollback_rows() == 0
        let result = do_scrollback(&terminal, 0, 100, 42, 80).unwrap();
        assert_eq!(result.generation, 42);
        assert!(result.data.is_empty());
        assert_eq!(result.total_scrollback_rows, 0);
    }

    #[test]
    fn do_scrollback_offset_beyond_total_returns_empty() {
        let mut terminal = make_terminal(80, 5, 1000);
        // Push 10 rows of scrollback by writing 15 lines (5-row screen scrolls 10 into history)
        for i in 0..15u8 {
            terminal.vt_write(format!("line{}\n", i).as_bytes());
        }
        let total = terminal.scrollback_rows().unwrap() as u32;
        assert!(total > 0, "expected some scrollback");
        let result = do_scrollback(&terminal, total, 10, 7, 80).unwrap();
        assert!(result.data.is_empty());
        assert_eq!(result.total_scrollback_rows, total);
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cargo test scrollback_tests 2>&1 | tail -20
```

Expected: compile error — `do_scrollback` does not exist yet.

- [ ] **Step 3: Implement do_scrollback**

Add this function in `src/pty.rs`, just above `reader_thread` (alongside `do_refresh`):

```rust
fn do_scrollback(
    terminal:   &Terminal<'static, 'static>,
    row_offset: u32,
    row_count:  u32,
    generation: u64,
    cols:       u32,
) -> Result<ScrollbackData> {
    let total = terminal.scrollback_rows()? as u32;

    if total == 0 || row_count == 0 {
        return Ok(ScrollbackData { generation, data: Bytes::new(), total_scrollback_rows: total });
    }
    if row_offset >= total {
        return Ok(ScrollbackData { generation, data: Bytes::new(), total_scrollback_rows: total });
    }

    // Helper: build FormatterTerminalExtra with all flags false.
    // Must set size explicitly — sized-struct ABI pattern (same as do_refresh).
    // ffi::FormatterScreenExtra also has a size field; initialize both explicitly.
    let make_extra = || ffi::FormatterTerminalExtra {
        size: std::mem::size_of::<ffi::FormatterTerminalExtra>(),
        scrolling_region: false,
        modes: false,
        palette: false,
        tabstops: false,
        pwd: false,
        keyboard: false,
        screen: ffi::FormatterScreenExtra {
            size: std::mem::size_of::<ffi::FormatterScreenExtra>(),
            cursor: false,
            style: false,
            hyperlink: false,
            protection: false,
            kitty_keyboard: false,
            charsets: false,
        },
    };

    // Optimization: full-buffer dump avoids the expensive grid_ref page-list traversal.
    // NOTE: grid_ref(Point::History(...)) traverses the internal scrollback page list to
    // locate the target row, which is O(scrollback_depth). For partial ranges this is
    // required; for the full-buffer case we skip it by passing selection: None to the
    // Formatter. If scrollback requests become a latency concern (do_scrollback runs on
    // the reader thread, blocking live PTY I/O), consider offloading to a background thread.
    if row_offset == 0 && row_count >= total {
        let mut fmt = Formatter::new(terminal, FormatterOptions {
            format: Format::Vt,
            trim: false,
            unwrap: false,
            selection: None,
            extra: make_extra(),
        })?;
        let vt = fmt.format_alloc(None)?;
        return Ok(ScrollbackData {
            generation,
            data: Bytes::from(vt.to_vec()),
            total_scrollback_rows: total,
        });
    }

    // Partial range: convert row_offset (distance from bottom) to Point::History y-coords.
    // History: y=0 = oldest row, y=total-1 = most recent row (just above active screen).
    let end_y   = total - 1 - row_offset;
    let rows    = row_count.min(end_y + 1);
    let start_y = end_y + 1 - rows;

    let top_left = terminal.grid_ref(Point::History(PointCoordinate { x: 0, y: start_y }))?;
    let bot_right = terminal.grid_ref(Point::History(PointCoordinate {
        x: cols.saturating_sub(1) as u16,
        y: end_y,
    }))?;
    let selection = Selection { start: top_left, end: bot_right, rectangle: false };

    let mut fmt = Formatter::new(terminal, FormatterOptions {
        format: Format::Vt,
        trim: false,
        unwrap: false,
        selection: Some(selection),
        extra: make_extra(),
    })?;
    let vt = fmt.format_alloc(None)?;
    Ok(ScrollbackData {
        generation,
        data: Bytes::from(vt.to_vec()),
        total_scrollback_rows: total,
    })
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cargo test scrollback_tests 2>&1 | tail -20
```

Expected: both tests pass.

- [ ] **Step 5: Drain scrollback_rx in the reader thread wakeup handler**

Find the wakeup handler in `reader_thread` (around line 516):

```rust
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_refresh(&terminal, current_cols, current_rows, gen);
            let _ = reply_tx.send(result);
        }
```

Add the scrollback drain immediately after (rename `_scrollback_rx` → `scrollback_rx` if you used the underscore prefix in Task 2):

```rust
        while let Ok((row_offset, row_count, reply_tx)) = scrollback_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_scrollback(&terminal, row_offset, row_count, gen, current_cols);
            let _ = reply_tx.send(result);
        }
```

Also drain at the end of the thread (find the post-exit drain for `refresh_rx` around line 659 and add the same for `scrollback_rx`):

```rust
    while let Ok(reply_tx) = refresh_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_refresh(&terminal, current_cols, current_rows, gen);
        let _ = reply_tx.send(result);
    }
    while let Ok((row_offset, row_count, reply_tx)) = scrollback_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_scrollback(&terminal, row_offset, row_count, gen, current_cols);
        let _ = reply_tx.send(result);
    }
```

- [ ] **Step 6: Build and run all tests**

```bash
cargo build 2>&1 | head -20
cargo test 2>&1 | tail -20
```

Expected: clean build, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/pty.rs
git commit -m "feat(pty): implement do_scrollback and drain in reader thread"
```

---

### Task 4: commands.rs — handle_scrollback

**Files:**
- Modify: `src/commands.rs`

- [ ] **Step 1: Confirm no import changes are needed**

`src/commands.rs` already imports everything from `crate::proto` via the wildcard:
```rust
use crate::proto::{terminal_response::Response, *};
```
`ScrollbackRequest`, `ScrollbackResponse`, and `Response::Scrollback` are already in scope.
`ScrollbackData` is not named in the handler body (types are inferred), so no change to the `crate::pty` import is needed.

- [ ] **Step 2: Add handle_scrollback**

Add this function at the bottom of `src/commands.rs`, after `handle_refresh`:

```rust
pub async fn handle_scrollback(
    registry: &PtyRegistry,
    req: ScrollbackRequest,
) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.get(&id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.scrollback(req.row_offset, req.row_count).await {
            Ok(data) => TerminalResponse {
                response: Some(Response::Scrollback(ScrollbackResponse {
                    pty_id:                id,
                    generation:            data.generation,
                    data:                  data.data.to_vec(),
                    total_scrollback_rows: data.total_scrollback_rows,
                })),
            },
            Err(e) => err_response(id, e.to_string()),
        },
    }
}
```

- [ ] **Step 3: Build**

```bash
cargo build 2>&1 | head -20
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add src/commands.rs
git commit -m "feat(commands): add handle_scrollback"
```

---

### Task 5: server.rs — dispatch Command::Scrollback

**Files:**
- Modify: `src/server.rs`

- [ ] **Step 1: Add dispatch arm**

Find the command dispatch match in `src/server.rs` (around line 67):

```rust
        Some(Command::Refresh(r))     => commands::handle_refresh(registry, r).await,
```

Add immediately after:

```rust
        Some(Command::Scrollback(r))  => commands::handle_scrollback(registry, r).await,
```

- [ ] **Step 2: Build and run all tests**

```bash
cargo build 2>&1 | head -20
cargo test 2>&1 | tail -20
```

Expected: clean build, all tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/server.rs
git commit -m "feat(server): dispatch ScrollbackRequest to handle_scrollback"
```
