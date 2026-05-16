# Client Attach Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the raw-passthrough `attach` stub with a correct client-side libghostty render pipeline that handles server/client terminal size mismatches, and add a `termd resize` subcommand for explicit server PTY resizing.

**Architecture:** All attach logic moves to `src/attach.rs` (binary module). `--debug` is a fast path that skips `LocalTerminal` and dumps raw events. Normal mode maintains a `LocalTerminal` (libghostty Terminal + RenderState + iterators at the server's resolution). Every `StreamData` chunk goes through `vt_write` → incremental dirty-row render → stdout using absolute cursor positioning, so wrapping is always at the server's column count regardless of client terminal width. SIGWINCH triggers a local force-full repaint without touching the server PTY.

**Tech Stack:** libghostty-vt (Terminal, TerminalOptions, RenderState, RowIterator, CellIterator, render::Dirty), tokio, tonic gRPC, Rust async

---

## File Structure

- **`src/attach.rs`** (CREATE): `LocalTerminal` struct, `render_dirty()`, `pub async fn run()`, `run_debug()`, `run_normal()`, stdin task, unit tests
- **`src/main.rs`** (MODIFY): add `mod attach;`, `Resize` Cmd variant, `PtyItem` import, `resolve_pty_item()`, wire `Cmd::Attach` → `attach::run()`, remove inline attach logic and resize-on-attach
- **`tests/integration.rs`** (MODIFY): add gRPC-level resize stream test

---

### Task 1: Add `termd resize` subcommand + gRPC stream test

**Files:**
- Modify: `src/main.rs`
- Modify: `tests/integration.rs`

- [ ] **Step 1: Add `PtyItem` to proto imports and `Resize` variant to `Cmd` in `main.rs`**

In the `use termd::{ proto:{ ... } }` block at the top of `main.rs`, add `PtyItem` alongside the existing imports:

```rust
use termd::{
    proto::{
        terminal_command::Command, terminal_response::Response,
        CreateRequest, DestroyRequest, ListRequest, PtyItem, RefreshRequest,
        ResizeRequest, SubscribeRequest, TerminalCommand, WriteRequest,
        StreamMetadataReason,
        terminal_service_client::TerminalServiceClient,
    },
    pty::PtyRegistry,
    server,
};
```

Add `Resize` to the `Cmd` enum after `Send`:

```rust
    /// Resize a PTY's columns and rows on the server
    Resize {
        pty_id: String,
        cols: u32,
        rows: u32,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
    },
```

- [ ] **Step 2: Add the `Resize` match arm in `main()`**

Add this arm to the `match cli.command` block, before `Cmd::Attach`:

```rust
        Cmd::Resize { pty_id, cols, rows, socket } => {
            let mut client = connect_client(socket).await?;
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
            let resp = send_recv(
                &mut client,
                Command::Resize(ResizeRequest { pty_id: pty_id.clone(), cols, rows }),
            ).await?;
            match resp.response {
                Some(Response::Command(c)) => {
                    if c.success {
                        println!("resized {} to {}x{}", pty_id, cols, rows);
                    } else {
                        eprintln!("error: {}", c.error.unwrap_or_default());
                        std::process::exit(1);
                    }
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }
```

- [ ] **Step 3: Build to confirm it compiles**

```bash
cargo build 2>&1
```

Expected: no errors.

- [ ] **Step 4: Write the failing gRPC-level resize stream test in `tests/integration.rs`**

Add after the last test in the file:

```rust
#[tokio::test]
async fn test_resize_via_grpc_delivers_metadata() {
    use termd::proto::{
        terminal_command::Command, terminal_response::Response,
        TerminalCommand, SubscribeRequest, ResizeRequest, StreamMetadataReason,
    };

    let (_dir, mut client) = test_server().await;

    // Create a PTY
    let resp = send_recv(&mut client, Command::Create(CreateRequest {
        cols: 80, rows: 24, command: None,
    })).await;
    let pty_id = match resp.response.unwrap() {
        Response::Create(c) => c.item.unwrap().pty_id,
        other => panic!("expected Create, got {other:?}"),
    };

    // Open a bidi stream and subscribe
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<TerminalCommand>(16);
    let mut resp_stream = client
        .stream(tokio_stream::wrappers::ReceiverStream::new(cmd_rx))
        .await
        .unwrap()
        .into_inner();

    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.clone() })),
    }).await.unwrap();

    // Drain until subscribe ack
    loop {
        match resp_stream.message().await.unwrap().unwrap().response.unwrap() {
            Response::Command(c) if c.success => break,
            _ => {}
        }
    }

    // Send resize
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Resize(ResizeRequest {
            pty_id: pty_id.clone(),
            cols: 120,
            rows: 40,
        })),
    }).await.unwrap();

    // Expect StreamMetadata::Resize with updated dimensions
    let found = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match resp_stream.message().await {
                Ok(Some(resp)) => match resp.response.unwrap() {
                    Response::Metadata(m)
                        if m.reason == StreamMetadataReason::Resize as i32 => {
                            let item = m.item.unwrap();
                            assert_eq!(item.cols, 120);
                            assert_eq!(item.rows, 40);
                            return true;
                        }
                    _ => continue,
                },
                _ => return false,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "resize command should deliver StreamMetadata::Resize to subscriber");
}
```

- [ ] **Step 5: Run the new test to confirm it fails (function not implemented yet, but build should pass)**

```bash
cargo test test_resize_via_grpc_delivers_metadata -- --nocapture 2>&1
```

Expected: PASS (the resize path already exists in the server; this test verifies the full end-to-end gRPC flow).

- [ ] **Step 6: Run the full test suite**

```bash
cargo test -- --test-threads=1 2>&1
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/main.rs tests/integration.rs
git commit -m "feat: add termd resize subcommand and gRPC resize stream test"
```

---

### Task 2: Add `resolve_pty_item` to `main.rs`

The attach code needs full `PtyItem` (including `cols`/`rows`) to seed `LocalTerminal`. Add a helper that returns the full item alongside the existing `resolve_pty_id` which returns just the ID string.

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Add `resolve_pty_item` below `resolve_pty_id` in `main.rs`**

```rust
async fn resolve_pty_item(client: &mut AuthedClient, prefix: &str) -> Result<PtyItem> {
    use termd::proto::terminal_response::Response;

    let resp = send_recv(client, Command::List(ListRequest {})).await?;
    let items = match resp.response {
        Some(Response::List(l)) => l.items,
        other => return Err(anyhow::anyhow!("unexpected list response: {other:?}")),
    };
    let matches: Vec<_> = items.iter().filter(|i| i.pty_id.starts_with(prefix)).collect();
    match matches.len() {
        1 => Ok(matches[0].clone()),
        0 => Err(anyhow::anyhow!("no PTY matches prefix {:?}", prefix)),
        _ => Err(anyhow::anyhow!(
            "ambiguous prefix {:?} matches: {}",
            prefix,
            matches.iter().map(|i| &i.pty_id[..8]).collect::<Vec<_>>().join(", ")
        )),
    }
}
```

- [ ] **Step 2: Build to confirm it compiles**

```bash
cargo build 2>&1
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add resolve_pty_item returning full PtyItem with cols/rows"
```

---

### Task 3: Create `src/attach.rs` with debug fast path; wire `main.rs`

Move all current `Cmd::Attach` logic into `attach.rs` under a debug fast path. Remove resize-on-attach and SIGWINCH resize. The normal mode path is a stub `unimplemented!()` for now.

**Files:**
- Create: `src/attach.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Create `src/attach.rs` with the debug fast path**

```rust
use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    RefreshRequest, SubscribeRequest, TerminalCommand, WriteRequest,
    PtyItem, StreamMetadataReason,
    terminal_service_client::TerminalServiceClient,
};

use crate::{connect_client, setup_raw_mode, get_terminal_size, EscapeState};

type AuthedClient = TerminalServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        fn(Request<()>) -> Result<Request<()>, tonic::Status>,
    >,
>;

pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
) -> Result<()> {
    if debug {
        run_debug(client, item.pty_id).await
    } else {
        unimplemented!("normal attach mode not yet implemented")
    }
}

async fn run_debug(client: &mut AuthedClient, pty_id: String) -> Result<()> {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    // Subscribe
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.clone() })),
    }).await?;

    loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during subscribe"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Command(c)) => {
                    if !c.success {
                        eprintln!("subscribe failed: {}", c.error.unwrap_or_default());
                        return Ok(());
                    }
                    break;
                }
                _ => {}
            }
        }
    }

    // NOTE: no resize sent — server PTY owns its dimensions

    // Request refresh
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
    }).await?;

    // Main debug receive loop — print events to stderr, no rendering
    loop {
        match resp_rx.message().await {
            Ok(Some(r)) => match r.response {
                Some(Response::Stream(s)) => {
                    eprintln!("[Stream gen={} len={}]", s.generation, s.data.len());
                }
                Some(Response::Refresh(rf)) => {
                    eprintln!("[Refresh gen={} len={}]", rf.generation, rf.data.len());
                }
                Some(Response::Metadata(m)) => {
                    eprintln!("[Metadata reason={} gen={} pty_id={}]", m.reason, m.generation, m.pty_id);
                    if m.reason == StreamMetadataReason::Closed as i32 {
                        eprintln!("[Connection closed]");
                        break;
                    }
                }
                _ => {}
            },
            _ => { eprintln!("[Connection closed]"); break; }
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Add `mod attach;` to `main.rs` and wire `Cmd::Attach`**

Add `mod attach;` near the top of `main.rs` (after the `use` blocks):

```rust
mod attach;
```

Replace the entire `Cmd::Attach { ... }` arm in `main()` with:

```rust
        Cmd::Attach { pty_id, socket, debug } => {
            let mut client = connect_client(socket).await?;
            let item = resolve_pty_item(&mut client, &pty_id).await?;
            attach::run(&mut client, item, debug).await?;
        }
```

- [ ] **Step 3: Remove the now-dead attach helpers from `main.rs`**

Delete `run_stdin`, `run_sigwinch`, `TerminalGuard`, `setup_raw_mode`, `get_terminal_size`, and `EscapeState` from `main.rs`. They will move to or be used by `attach.rs`. For now just delete them — the code in `attach.rs` imports `crate::setup_raw_mode` etc., so if they're needed for debug mode, move them first.

Wait — `run_debug` above doesn't use raw mode, `setup_raw_mode`, or `get_terminal_size`. Those are only needed for the normal mode. So delete them from `main.rs` for now; they'll be added directly into `attach.rs` in Task 5.

Also remove the now-unused imports from `main.rs` (nix, libc, tokio::io::AsyncWriteExt, etc.) — let the compiler errors guide you.

- [ ] **Step 4: Remove the `use crate::` imports from `attach.rs` that referenced deleted helpers**

Since `setup_raw_mode`, `get_terminal_size`, and `EscapeState` were deleted from `main.rs`, remove the `use crate::{...}` line in `attach.rs`:

```rust
// Remove this line:
use crate::{connect_client, setup_raw_mode, get_terminal_size, EscapeState};
```

`connect_client` stays in `main.rs` and is not needed by `attach.rs` (the caller passes an already-connected client).

- [ ] **Step 5: Build**

```bash
cargo build 2>&1
```

Fix any compilation errors — these will mostly be unused imports in `main.rs` after moving the attach code out. Remove them as the compiler directs.

- [ ] **Step 6: Test debug mode manually**

```bash
cargo run -- start &
sleep 0.5
PTY=$(cargo run -- create)
cargo run -- attach --debug $PTY
# Should print [Refresh ...] and [Stream ...] events to stderr
# Ctrl-C to stop
kill %1
```

- [ ] **Step 7: Commit**

```bash
git add src/attach.rs src/main.rs
git commit -m "feat: move attach to attach.rs; debug fast path; remove resize-on-attach"
```

---

### Task 4: Add `LocalTerminal` + `render_dirty()` with unit tests

**Files:**
- Modify: `src/attach.rs`

- [ ] **Step 1: Add imports and `LocalTerminal` struct to `attach.rs`**

Add these imports at the top of `attach.rs`:

```rust
use std::io::Write as IoWrite;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;
```

Add the struct definition after the imports:

```rust
struct LocalTerminal {
    terminal: Terminal<'static, 'static>,
    render_state: RenderState<'static>,
    row_iter: RowIterator<'static>,
    cell_iter: CellIterator<'static>,
}

impl LocalTerminal {
    fn new(cols: u32, rows: u32) -> Result<Self> {
        Ok(Self {
            terminal: Terminal::new(TerminalOptions {
                cols: cols as u16,
                rows: rows as u16,
                max_scrollback: 0,
            })?,
            render_state: RenderState::new()?,
            row_iter: RowIterator::new()?,
            cell_iter: CellIterator::new()?,
        })
    }

    fn resize(&mut self, cols: u32, rows: u32) -> Result<()> {
        Ok(self.terminal.resize(cols as u16, rows as u16, 0, 0)?)
    }
}
```

- [ ] **Step 2: Write the failing unit tests for `render_dirty` in `attach.rs`**

Add at the bottom of `attach.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_dirty_produces_no_output_when_clean() {
        let mut lt = LocalTerminal::new(80, 24).unwrap();
        // Initial render to clear the startup-dirty state
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();
        // No changes — second render should produce nothing
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(!changed, "clean terminal should produce no render output");
        assert!(out.is_empty());
    }

    #[test]
    fn render_dirty_emits_only_changed_rows() {
        let mut lt = LocalTerminal::new(80, 24).unwrap();
        // Clear initial dirty state
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        // Write text — lands on row 0 (ANSI row 1)
        lt.terminal.vt_write(b"Hello");

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"), "row 1 cursor position expected");
        assert!(!s.contains("\x1b[2;1H"), "row 2 should not be re-rendered");
    }

    #[test]
    fn force_full_renders_all_rows() {
        let mut lt = LocalTerminal::new(80, 24).unwrap();
        // Clear initial dirty state
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        // Force full repaint with no terminal changes
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed, "force_full should produce output even with no changes");
        let s = String::from_utf8_lossy(&out);
        // All 24 rows should appear
        assert!(s.contains("\x1b[1;1H"));
        assert!(s.contains("\x1b[24;1H"));
    }
}
```

- [ ] **Step 3: Run tests — expect compilation failure (render_dirty not defined yet)**

```bash
cargo test -p termd --lib 2>&1 | head -20
```

Expected: compile error: `render_dirty` not found.

- [ ] **Step 4: Implement `render_dirty()`**

Add this function to `attach.rs` (between `LocalTerminal` impl and `pub async fn run`):

```rust
/// Renders only dirty rows from the terminal state into `out` as VT sequences.
/// Uses absolute cursor positioning, so output is correct for any client terminal size.
/// If `force_full` is true, all rows are rendered regardless of dirty state (used for SIGWINCH).
/// Returns true if any output was produced.
fn render_dirty(
    terminal: &Terminal<'static, 'static>,
    render_state: &mut RenderState<'static>,
    row_iter: &mut RowIterator<'static>,
    cell_iter: &mut CellIterator<'static>,
    force_full: bool,
    out: &mut Vec<u8>,
) -> Result<bool> {
    let snapshot = render_state.update(terminal)?;

    let global_dirty = if force_full {
        snapshot.set_dirty(Dirty::Full)?;
        Dirty::Full
    } else {
        snapshot.dirty()?
    };

    if global_dirty == Dirty::Clean {
        return Ok(false);
    }

    let cursor_visible = snapshot.cursor_visible().unwrap_or(true);
    let (cursor_x, cursor_y) = match snapshot.cursor_viewport().ok().flatten() {
        Some(cv) => (cv.x as u32, cv.y as u32),
        None => (
            terminal.cursor_x().unwrap_or(0) as u32,
            terminal.cursor_y().unwrap_or(0) as u32,
        ),
    };

    let mut row_iter_active = row_iter.update(&snapshot)?;
    let mut row_idx: u32 = 0;
    let mut char_enc = [0u8; 4];
    let mut rendered_any = false;

    while let Some(row) = row_iter_active.next() {
        if global_dirty != Dirty::Full && !row.dirty()? {
            row_idx += 1;
            continue;
        }
        rendered_any = true;
        write!(out, "\x1b[{};1H", row_idx + 1).ok();

        let mut cell_iter_active = cell_iter.update(row)?;
        while let Some(cell) = cell_iter_active.next() {
            let style = cell.style()?;
            let fg = cell.fg_color().ok().flatten();
            let bg = cell.bg_color().ok().flatten();
            let graphemes = cell.graphemes()?;

            out.extend_from_slice(b"\x1b[0");
            if style.bold          { out.extend_from_slice(b";1"); }
            if style.faint         { out.extend_from_slice(b";2"); }
            if style.italic        { out.extend_from_slice(b";3"); }
            match style.underline {
                Underline::None   => {}
                Underline::Double => out.extend_from_slice(b";21"),
                _                 => out.extend_from_slice(b";4"),
            }
            if style.blink         { out.extend_from_slice(b";5"); }
            if style.inverse       { out.extend_from_slice(b";7"); }
            if style.invisible     { out.extend_from_slice(b";8"); }
            if style.strikethrough { out.extend_from_slice(b";9"); }
            if style.overline      { out.extend_from_slice(b";53"); }
            if let Some(c) = fg {
                write!(out, ";38;2;{};{};{}", c.r, c.g, c.b).ok();
            }
            if let Some(c) = bg {
                write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok();
            }
            out.push(b'm');

            if graphemes.is_empty() {
                out.push(b' ');
            } else {
                for ch in &graphemes {
                    out.extend_from_slice(ch.encode_utf8(&mut char_enc).as_bytes());
                }
            }
        }
        row.set_dirty(false)?;
        row_idx += 1;
    }

    if rendered_any {
        out.extend_from_slice(b"\x1b[0m");
        if cursor_visible {
            out.extend_from_slice(b"\x1b[?25h");
        } else {
            out.extend_from_slice(b"\x1b[?25l");
        }
        write!(out, "\x1b[{};{}H", cursor_y + 1, cursor_x + 1).ok();
    }

    snapshot.set_dirty(Dirty::Clean)?;
    Ok(rendered_any)
}
```

- [ ] **Step 5: Run the unit tests**

```bash
cargo test -p termd --lib attach 2>&1
```

Wait — `attach.rs` is part of the binary, not the library. Run the binary's tests:

```bash
cargo test --bin termd 2>&1
```

Expected: `render_dirty_produces_no_output_when_clean`, `render_dirty_emits_only_changed_rows`, `force_full_renders_all_rows` all PASS.

- [ ] **Step 6: Run all tests**

```bash
cargo test -- --test-threads=1 2>&1
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/attach.rs
git commit -m "feat: add LocalTerminal and incremental render_dirty()"
```

---

### Task 5: Implement `run_normal()` — full render pipeline

**Files:**
- Modify: `src/attach.rs`

- [ ] **Step 1: Add raw mode helpers to `attach.rs`**

Copy `TerminalGuard`, `setup_raw_mode`, `get_terminal_size`, and `EscapeState` from their current location into `attach.rs`. Also add the stdin task as a private function. The exact code to copy:

**`TerminalGuard` and `setup_raw_mode`:**
```rust
struct TerminalGuard {
    original: nix::sys::termios::Termios,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        use nix::sys::termios::{tcsetattr, SetArg};
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(libc::STDIN_FILENO) };
        let _ = tcsetattr(fd, SetArg::TCSAFLUSH, &self.original);
    }
}

fn setup_raw_mode() -> Result<TerminalGuard> {
    use nix::sys::termios::{tcgetattr, tcsetattr, SetArg, LocalFlags, InputFlags};
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(libc::STDIN_FILENO) };
    let original = tcgetattr(fd)?;
    let mut raw = original.clone();
    raw.local_flags.remove(
        LocalFlags::ICANON | LocalFlags::ECHO | LocalFlags::ISIG | LocalFlags::IEXTEN,
    );
    raw.input_flags.remove(
        InputFlags::IXON | InputFlags::ICRNL | InputFlags::BRKINT
            | InputFlags::INPCK | InputFlags::ISTRIP,
    );
    raw.control_chars[libc::VMIN as usize] = 1;
    raw.control_chars[libc::VTIME as usize] = 0;
    tcsetattr(fd, SetArg::TCSAFLUSH, &raw)?;
    Ok(TerminalGuard { original })
}
```

**`get_terminal_size`:**
```rust
fn get_terminal_size() -> Result<(u32, u32)> {
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) };
    if ret < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((ws.ws_col as u32, ws.ws_row as u32))
}
```

**`EscapeState` and `run_stdin`:**
```rust
#[derive(Clone, Copy)]
enum EscapeState {
    Normal,
    AfterNewline,
    AfterTilde,
}

async fn run_stdin(
    cmd_tx: tokio::sync::mpsc::Sender<TerminalCommand>,
    pty_id: String,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
) {
    use tokio::io::AsyncReadExt;
    let mut stdin = tokio::io::stdin();
    let mut state = EscapeState::AfterNewline;
    let mut buf = [0u8; 256];
    let mut shutdown_tx = Some(shutdown_tx);

    'outer: loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let mut to_send: Vec<u8> = Vec::new();
        for &byte in &buf[..n] {
            match state {
                EscapeState::Normal => {
                    to_send.push(byte);
                    if byte == b'\r' || byte == b'\n' {
                        state = EscapeState::AfterNewline;
                    }
                }
                EscapeState::AfterNewline => {
                    if byte == b'~' {
                        state = EscapeState::AfterTilde;
                    } else if byte == b'\r' || byte == b'\n' {
                        to_send.push(byte);
                    } else {
                        to_send.push(byte);
                        state = EscapeState::Normal;
                    }
                }
                EscapeState::AfterTilde => {
                    if byte == b'.' {
                        if !to_send.is_empty() {
                            let _ = cmd_tx.send(TerminalCommand {
                                command: Some(Command::Write(WriteRequest {
                                    pty_id: pty_id.clone(),
                                    data: to_send,
                                })),
                            }).await;
                        }
                        if let Some(tx) = shutdown_tx.take() { let _ = tx.send(()); }
                        break 'outer;
                    } else if byte == b'\r' || byte == b'\n' {
                        to_send.push(b'~');
                        to_send.push(byte);
                        state = EscapeState::AfterNewline;
                    } else {
                        to_send.push(b'~');
                        to_send.push(byte);
                        state = EscapeState::Normal;
                    }
                }
            }
        }
        if !to_send.is_empty() {
            if cmd_tx.send(TerminalCommand {
                command: Some(Command::Write(WriteRequest {
                    pty_id: pty_id.clone(),
                    data: to_send,
                })),
            }).await.is_err() {
                break;
            }
        }
    }
}
```

Add these imports to the top of `attach.rs` (they were previously in `main.rs`):

```rust
use libc;
use nix;
```

- [ ] **Step 2: Implement `run_normal()`**

Replace `unimplemented!("normal attach mode not yet implemented")` in `run()` with a call to `run_normal`:

```rust
    } else {
        run_normal(client, item).await
    }
```

Add the `run_normal` function:

```rust
async fn run_normal(client: &mut AuthedClient, item: PtyItem) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    use tokio::signal::unix::{signal, SignalKind};
    use tokio::sync::{mpsc, oneshot};

    let pty_id = item.pty_id.clone();

    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    // Subscribe
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.clone() })),
    }).await?;

    loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during subscribe"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Command(c)) => {
                    if !c.success {
                        eprintln!("subscribe failed: {}", c.error.unwrap_or_default());
                        return Ok(());
                    }
                    break;
                }
                _ => {}
            }
        }
    }

    // NOTE: no resize sent — server PTY owns its dimensions

    // Request refresh
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
    }).await?;

    // Buffer StreamData while waiting for RefreshResponse
    let mut buffered: Vec<(u64, Vec<u8>)> = Vec::new();
    let (refresh_gen, refresh_bytes) = loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during refresh"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Refresh(rf)) => break (rf.generation, rf.data),
                Some(Response::Stream(s)) => buffered.push((s.generation, s.data)),
                _ => {}
            }
        }
    };

    // Create LocalTerminal at server resolution, seed with refresh bytes
    let mut lt = LocalTerminal::new(item.cols, item.rows)?;
    lt.terminal.vt_write(&refresh_bytes);

    // Paint refresh to stdout
    let mut stdout = tokio::io::stdout();
    stdout.write_all(&refresh_bytes).await?;

    // Replay buffered chunks that post-date the refresh
    for (gen, data) in &buffered {
        if *gen > refresh_gen {
            lt.terminal.vt_write(data);
            let mut out = Vec::new();
            render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out)?;
            stdout.write_all(&out).await?;
        }
    }
    stdout.flush().await?;

    // Enter raw mode
    let _guard = setup_raw_mode()?;

    // SIGWINCH — handled inline for local repaint
    let mut sigwinch = signal(SignalKind::window_change())?;

    // Stdin task
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let stdin_task = tokio::spawn(run_stdin(cmd_tx.clone(), pty_id.clone(), shutdown_tx));
    drop(cmd_tx);

    // Main receive loop
    let mut server_closed = false;
    loop {
        let mut out = Vec::new();
        tokio::select! {
            msg = resp_rx.message() => {
                match msg {
                    Ok(Some(r)) => match r.response {
                        Some(Response::Stream(s)) => {
                            if s.generation > refresh_gen {
                                lt.terminal.vt_write(&s.data);
                                render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out)?;
                            }
                        }
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref item) = m.item {
                                    lt.resize(item.cols, item.rows)?;
                                    render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out)?;
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                server_closed = true;
                                break;
                            }
                        }
                        _ => {}
                    },
                    _ => { server_closed = true; break; }
                }
            }
            _ = &mut shutdown_rx => break,
            _ = sigwinch.recv() => {
                // Force full local repaint — no resize sent to server
                render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out)?;
            }
        }
        if !out.is_empty() {
            if stdout.write_all(&out).await.is_err() { break; }
            let _ = stdout.flush().await;
        }
    }

    stdin_task.abort();
    drop(_guard);
    if server_closed {
        eprintln!("[Connection closed]");
    }
    Ok(())
}
```

- [ ] **Step 3: Add TODO comment for future lag recovery**

Inside the `Some(Response::Stream(s))` arm, after the `render_dirty` call, add:

```rust
// TODO: detect generation gaps here and request a Refresh from the server
// to resync LocalTerminal state after broadcast lag. For now, SIGWINCH repaint
// self-heals a lagged client.
```

Lag recovery is out of scope for this task — a lagged client continues from the next received message and corrects on the next SIGWINCH repaint.

- [ ] **Step 4: Build**

```bash
cargo build 2>&1
```

Fix any compile errors (missing imports, lifetime issues). Common fixes:
- If borrow checker complains about `lt.terminal` and `lt.render_state` simultaneously: they are separate struct fields, which Rust allows to borrow independently. If the error persists, extract as local `let terminal = &lt.terminal;` before calling `render_dirty`.
- If `libc` or `nix` imports are missing, they are already in `Cargo.toml` (used by `src/pty.rs`).

- [ ] **Step 5: Run all tests**

```bash
cargo test -- --test-threads=1 2>&1
```

Expected: all tests pass including the three unit tests in `attach.rs` and all integration tests.

- [ ] **Step 6: Manual end-to-end test**

```bash
cargo build --release
./target/release/termd start &
sleep 0.5
PTY=$(./target/release/termd create)
# Attach in a terminal — should show a shell
./target/release/termd attach $PTY
# Type in the shell, verify wrapping at server PTY width
# Type ~. to detach — should print nothing (user-initiated)
# In another terminal, destroy the PTY:
./target/release/termd destroy $PTY
# Attached client should print "[Connection closed]"
kill %1
```

- [ ] **Step 7: Commit**

```bash
git add src/attach.rs src/main.rs
git commit -m "feat: implement run_normal with LocalTerminal incremental render and SIGWINCH repaint"
```
