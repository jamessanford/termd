# Attach Render Modes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--render-mode cell|formatter|raw` to `termd attach`, splitting the current monolithic `attach.rs` into per-mode files so each strategy is self-contained and easy to compare.

**Architecture:** `attach.rs` becomes `attach/mod.rs` owning shared helpers (`LocalTerminal`, `TerminalGuard`, `run_stdin`), the subscribe/refresh preamble, and a `RunContext` struct. Each mode lives in its own file (`cell.rs`, `formatter.rs`, `raw.rs`) with a `pub(super) async fn run(ctx: super::RunContext) -> Result<bool>` entry point; `bool` signals whether the server closed the connection.

**Tech Stack:** Rust, tokio, tonic, clap 4 (ValueEnum), libghostty-vt (cell + formatter modes only)

---

## File map

| File | Action | Responsibility |
|---|---|---|
| `src/attach.rs` | **Delete** | replaced by module |
| `src/attach/mod.rs` | **Create** (from attach.rs) | `RenderMode`, `RunContext`, `LocalTerminal`, shared helpers, preamble, dispatch |
| `src/attach/cell.rs` | **Create** | cell-by-cell render loop + `render_dirty` + tests |
| `src/attach/formatter.rs` | **Create** | formatter-for-Full render loop + `render_dirty` + tests |
| `src/attach/raw.rs` | **Create** | direct passthrough loop, SIGWINCH→Refresh |
| `src/main.rs` | **Modify** | add `--render-mode` arg to `Attach`, pass to `attach::run` |

---

## Task 1 — Convert `attach.rs` → `attach/` module skeleton

**Files:**
- Create: `src/attach/mod.rs`
- Create: `src/attach/cell.rs` (stub)
- Create: `src/attach/formatter.rs` (stub)
- Create: `src/attach/raw.rs` (stub)
- Delete: `src/attach.rs`

- [ ] **Step 1: Create the directory and copy the existing file**

```bash
mkdir src/attach
cp src/attach.rs src/attach/mod.rs
```

- [ ] **Step 2: Add module declarations to the top of `src/attach/mod.rs`** (after the existing `use` lines at the very top of the file, add):

```rust
mod cell;
mod formatter;
mod raw;
```

- [ ] **Step 3: Create stub files**

`src/attach/cell.rs`:
```rust
// Cell-by-cell render mode.
```

`src/attach/formatter.rs`:
```rust
// Formatter render mode — VT formatter for Dirty::Full, cell-by-cell for Dirty::Partial.
```

`src/attach/raw.rs`:
```rust
// Raw passthrough mode — no libghostty on the render path.
```

- [ ] **Step 4: Delete the old file**

```bash
rm src/attach.rs
```

- [ ] **Step 5: Verify it compiles**

```bash
cargo build 2>&1
```

Expected: `Finished` with no errors. Behavior is identical to before.

- [ ] **Step 6: Commit**

```bash
git add src/attach/mod.rs src/attach/cell.rs src/attach/formatter.rs src/attach/raw.rs
git rm src/attach.rs
git commit -m "refactor: convert attach.rs to attach/ module skeleton"
```

---

## Task 2 — Add `RenderMode`, `RunContext`; wire up `main.rs`

**Files:**
- Modify: `src/attach/mod.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Add `RenderMode` enum to `src/attach/mod.rs`** — insert after the `use` block, before `type AuthedClient`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states (default)
    Cell,
    /// VT formatter for full repaints, cell-by-cell for partial repaints
    Formatter,
    /// Raw PTY byte passthrough — no libghostty on the render path
    Raw,
}
```

- [ ] **Step 2: Add `RunContext` struct to `src/attach/mod.rs`** — insert directly after `RenderMode`:

```rust
pub(crate) struct RunContext {
    pub resp_rx:       tonic::Streaming<termd::proto::TerminalResponse>,
    pub cmd_tx:        tokio::sync::mpsc::Sender<TerminalCommand>,
    pub pty_id:        String,
    pub item:          PtyItem,
    pub refresh_gen:   u64,
    pub refresh_bytes: Vec<u8>,
    pub buffered:      Vec<(u64, Vec<u8>)>,
    pub shutdown_rx:   tokio::sync::oneshot::Receiver<()>,
}
```

- [ ] **Step 3: Replace `pub async fn run` in `src/attach/mod.rs`**

Find and replace the existing:
```rust
pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
) -> Result<()> {
    if debug {
        run_debug(client, item.pty_id).await
    } else {
        run_normal(client, item).await
    }
}
```

With:
```rust
pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
    mode: RenderMode,
) -> Result<()> {
    use tokio::sync::oneshot;

    if debug {
        return run_debug(client, item.pty_id).await;
    }

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

    // Request refresh; buffer any Stream chunks that arrive before the response
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
    }).await?;
    let mut buffered: Vec<(u64, Vec<u8>)> = Vec::new();
    let (refresh_gen, refresh_bytes) = loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during refresh"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Refresh(rf)) => break (rf.generation, rf.data),
                Some(Response::Stream(s))   => buffered.push((s.generation, s.data)),
                _ => {}
            }
        }
    };

    // Enter raw terminal mode; guard restores settings on any exit path
    let _guard = setup_raw_mode()?;

    // Spawn stdin forwarder; shutdown_rx fires on ~. escape
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let stdin_task = tokio::spawn(run_stdin(cmd_tx.clone(), pty_id.clone(), shutdown_tx));

    let ctx = RunContext {
        resp_rx,
        cmd_tx,
        pty_id,
        item,
        refresh_gen,
        refresh_bytes,
        buffered,
        shutdown_rx,
    };

    let server_closed = match mode {
        RenderMode::Cell      => cell::run(ctx).await?,
        RenderMode::Formatter => formatter::run(ctx).await?,
        RenderMode::Raw       => raw::run(ctx).await?,
    };

    stdin_task.abort();
    drop(_guard);
    if server_closed {
        eprintln!("[Connection closed]");
    }
    Ok(())
}
```

- [ ] **Step 4: Update `src/main.rs` — add `render_mode` to the `Attach` variant**

Find:
```rust
    /// Attach to a running PTY, streaming output to stdout and forwarding stdin
    Attach {
        /// PTY ID to attach to (from `termd list`)
        pty_id: String,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
        /// Print message metadata to stderr instead of writing data to stdout
        #[arg(long)]
        debug: bool,
    },
```

Replace with:
```rust
    /// Attach to a running PTY, streaming output to stdout and forwarding stdin
    Attach {
        /// PTY ID to attach to (from `termd list`)
        pty_id: String,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
        /// Print message metadata to stderr instead of writing data to stdout
        #[arg(long)]
        debug: bool,
        /// Rendering strategy for terminal output
        #[arg(long, value_enum, default_value_t = attach::RenderMode::Cell)]
        render_mode: attach::RenderMode,
    },
```

- [ ] **Step 5: Update the `Cmd::Attach` match arm in `src/main.rs`**

Find:
```rust
        Cmd::Attach { pty_id, socket, debug } => {
            let mut client = connect_client(socket).await?;
            let item = resolve_pty_item(&mut client, &pty_id).await?;
            attach::run(&mut client, item, debug).await?;
        }
```

Replace with:
```rust
        Cmd::Attach { pty_id, socket, debug, render_mode } => {
            let mut client = connect_client(socket).await?;
            let item = resolve_pty_item(&mut client, &pty_id).await?;
            attach::run(&mut client, item, debug, render_mode).await?;
        }
```

- [ ] **Step 6: Verify it compiles** (`cell::run`, `formatter::run`, `raw::run` are unresolved — expect errors there only)

```bash
cargo build 2>&1 | grep -v "^error\[E0425\].*run"
```

Expected: only errors about `cell::run`, `formatter::run`, `raw::run` not being found in the stub files. No other errors.

Actually, just run:
```bash
cargo build 2>&1
```

Expected output contains errors like `error[E0425]: cannot find function run in module cell`. That's expected — stubs are empty. Fix by temporarily adding placeholder `pub(super) async fn run` to each stub (will be replaced in Tasks 3–5):

Add to `src/attach/cell.rs`:
```rust
pub(super) async fn run(_ctx: super::RunContext) -> anyhow::Result<bool> {
    Ok(false)
}
```

Add identical content to `src/attach/formatter.rs` and `src/attach/raw.rs`.

```bash
cargo build 2>&1
```

Expected: `Finished` with no errors.

- [ ] **Step 7: Commit**

```bash
git add src/attach/mod.rs src/attach/cell.rs src/attach/formatter.rs src/attach/raw.rs src/main.rs
git commit -m "feat: add RenderMode enum, RunContext struct, --render-mode flag"
```

---

## Task 3 — Implement `cell.rs`

**Files:**
- Modify: `src/attach/cell.rs`
- Modify: `src/attach/mod.rs` (remove `run_normal`)

- [ ] **Step 1: Write tests in `src/attach/cell.rs`** (replacing the placeholder `run`)

```rust
use std::io::Write as IoWrite;

use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    RefreshRequest, StreamMetadataReason, TerminalCommand,
};

pub(super) async fn run(_ctx: super::RunContext) -> Result<bool> {
    Ok(false) // replaced in Step 3
}

fn render_dirty(
    terminal: &Terminal<'static, 'static>,
    render_state: &mut RenderState<'static>,
    row_iter: &mut RowIterator<'static>,
    cell_iter: &mut CellIterator<'static>,
    force_full: bool,
    out: &mut Vec<u8>,
) -> Result<bool> {
    todo!() // replaced in Step 3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_dirty_produces_no_output_when_clean() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        // First render clears the startup-dirty state
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();
        // No changes — second render should produce nothing
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(!changed, "clean terminal should produce no output");
        assert!(out.is_empty());
    }

    #[test]
    fn render_dirty_emits_only_changed_rows() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        lt.terminal.vt_write(b"Hello");

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"), "row 1 cursor-goto expected");
        assert!(!s.contains("\x1b[2;1H"), "row 2 should not be rendered");
    }

    #[test]
    fn force_full_renders_all_rows() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed, "force_full should always produce output");
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"));
        assert!(s.contains("\x1b[24;1H"));
    }
}
```

- [ ] **Step 2: Run the tests — expect failure**

```bash
cargo test -p termd 2>&1 | tail -20
```

Expected: compilation error (render_dirty has `todo!()`, run is a stub).

- [ ] **Step 3: Replace `cell.rs` with the full implementation**

```rust
use std::io::Write as IoWrite;

use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    RefreshRequest, StreamMetadataReason, TerminalCommand,
};

pub(super) async fn run(ctx: super::RunContext) -> Result<bool> {
    let super::RunContext { mut resp_rx, item, refresh_gen, refresh_bytes, buffered, mut shutdown_rx, .. } = ctx;

    let mut lt = super::LocalTerminal::new(item.cols, item.rows)?;
    lt.terminal.vt_write(&refresh_bytes);

    let mut stdout = tokio::io::stdout();
    let mut out = Vec::new();

    // Full repaint from seeded state
    render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out)?;
    stdout.write_all(&out).await?;

    // Replay stream chunks buffered while awaiting the initial Refresh response
    for (gen, data) in &buffered {
        if *gen > refresh_gen {
            lt.terminal.vt_write(data);
            out.clear();
            render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out)?;
            stdout.write_all(&out).await?;
        }
    }
    stdout.flush().await?;

    let mut sigwinch = signal(SignalKind::window_change())?;
    let mut server_closed = false;
    out.clear();

    loop {
        out.clear();
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
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        lt.resize(mi.cols, mi.rows)?;
                                        out.extend_from_slice(b"\x1b[2J");
                                    }
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
                render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out)?;
            }
        }
        if !out.is_empty() {
            if stdout.write_all(&out).await.is_err() { break; }
            let _ = stdout.flush().await;
        }
    }

    Ok(server_closed)
}

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
    let mut grapheme_buf: Vec<char> = Vec::new();

    while let Some(row) = row_iter_active.next() {
        if global_dirty != Dirty::Full && !row.dirty()? {
            row_idx += 1;
            continue;
        }
        write!(out, "\x1b[{};1H", row_idx + 1).ok();

        let mut cell_iter_active = cell_iter.update(row)?;
        while let Some(cell) = cell_iter_active.next() {
            let len = cell.graphemes_len()?;
            if len == 0 {
                let bg = cell.bg_color().ok().flatten();
                if let Some(c) = bg {
                    out.extend_from_slice(b"\x1b[0");
                    write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok();
                    out.extend_from_slice(b"m ");
                } else {
                    out.push(b' ');
                }
            } else {
                if grapheme_buf.len() < len {
                    grapheme_buf.resize(len, '\0');
                }
                cell.graphemes_buf(&mut grapheme_buf[..len])?;
                let graphemes = &grapheme_buf[..len];
                let style = cell.style()?;
                let fg = cell.fg_color().ok().flatten();
                let bg = cell.bg_color().ok().flatten();

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
                if let Some(c) = fg { write!(out, ";38;2;{};{};{}", c.r, c.g, c.b).ok(); }
                if let Some(c) = bg { write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok(); }
                out.push(b'm');

                for ch in graphemes {
                    out.extend_from_slice(ch.encode_utf8(&mut char_enc).as_bytes());
                }
            }
        }
        row.set_dirty(false)?;
        row_idx += 1;
    }

    out.extend_from_slice(b"\x1b[0m");
    if cursor_visible { out.extend_from_slice(b"\x1b[?25h"); } else { out.extend_from_slice(b"\x1b[?25l"); }
    write!(out, "\x1b[{};{}H", cursor_y + 1, cursor_x + 1).ok();

    snapshot.set_dirty(Dirty::Clean)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_dirty_produces_no_output_when_clean() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(!changed, "clean terminal should produce no output");
        assert!(out.is_empty());
    }

    #[test]
    fn render_dirty_emits_only_changed_rows() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        lt.terminal.vt_write(b"Hello");

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"), "row 1 cursor-goto expected");
        assert!(!s.contains("\x1b[2;1H"), "row 2 should not be rendered");
    }

    #[test]
    fn force_full_renders_all_rows() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed, "force_full should always produce output");
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"));
        assert!(s.contains("\x1b[24;1H"));
    }
}
```

- [ ] **Step 4: Delete `render_dirty` and tests from `src/attach/mod.rs`**

In `mod.rs`, remove: the `render_dirty` function (lines 52–167), the `#[cfg(test)] mod tests { ... }` block at the bottom, and `run_normal` (it is now replaced by `cell::run`). Also remove the libghostty imports that are no longer used in mod.rs:
- `use libghostty_vt::{Terminal, TerminalOptions, RenderState};`
- `use libghostty_vt::render::{Dirty, RowIterator, CellIterator};`
- `use libghostty_vt::style::Underline;`
- `use libghostty_vt::fmt::{Format, Formatter, FormatterOptions};` (if present)

Keep in mod.rs: `LocalTerminal` struct + impl (cell and formatter both use it via `super::LocalTerminal`).

- [ ] **Step 5: Run the tests**

```bash
cargo test 2>&1
```

Expected:
```
test attach::cell::tests::render_dirty_produces_no_output_when_clean ... ok
test attach::cell::tests::render_dirty_emits_only_changed_rows ... ok
test attach::cell::tests::force_full_renders_all_rows ... ok
```

- [ ] **Step 6: Verify the binary builds**

```bash
cargo build 2>&1
```

Expected: `Finished` with no errors.

- [ ] **Step 7: Commit**

```bash
git add src/attach/mod.rs src/attach/cell.rs
git commit -m "feat: implement cell render mode in attach/cell.rs"
```

---

## Task 4 — Implement `formatter.rs`

**Files:**
- Modify: `src/attach/formatter.rs`

The formatter mode is identical to cell mode except `render_dirty` uses the VT formatter for `Dirty::Full` repaints.

- [ ] **Step 1: Write formatter-specific tests in `src/attach/formatter.rs`**

```rust
use std::io::Write as IoWrite;
use anyhow::Result;

pub(super) async fn run(_ctx: super::RunContext) -> Result<bool> {
    Ok(false) // replaced in Step 2
}

fn render_dirty(
    _terminal: &libghostty_vt::Terminal<'static, 'static>,
    _render_state: &mut libghostty_vt::RenderState<'static>,
    _row_iter: &mut libghostty_vt::render::RowIterator<'static>,
    _cell_iter: &mut libghostty_vt::render::CellIterator<'static>,
    _force_full: bool,
    _out: &mut Vec<u8>,
) -> Result<bool> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use libghostty_vt::{RenderState, render::{RowIterator, CellIterator}};

    #[test]
    fn full_dirty_uses_formatter_clear() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        // Consume startup dirty so next state is clean
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        // Write content so state is meaningful
        lt.terminal.vt_write(b"hello");
        // force_full=true triggers Dirty::Full path → formatter
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        // Formatter path always starts with clear-screen + home
        assert!(s.contains("\x1b[2J"), "formatter Full path must emit clear-screen");
    }

    #[test]
    fn partial_dirty_uses_cell_cursor_goto() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        // Consume startup dirty
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        // One line of content → Partial dirty (only row 0 dirty)
        lt.terminal.vt_write(b"hi");
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"), "Partial path must emit row cursor-goto");
        assert!(!s.contains("\x1b[2J"), "Partial path must not clear screen");
    }
}
```

- [ ] **Step 2: Run tests — expect failure**

```bash
cargo test attach::formatter 2>&1 | tail -10
```

Expected: compile error (todo!() + stub run).

- [ ] **Step 3: Replace `formatter.rs` with the full implementation**

The `run` function is identical to `cell::run` — copy it verbatim. The `render_dirty` differs only in the `Dirty::Full` branch:

```rust
use std::io::Write as IoWrite;

use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::fmt::{Format, Formatter, FormatterOptions};
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    RefreshRequest, StreamMetadataReason, TerminalCommand,
};

pub(super) async fn run(ctx: super::RunContext) -> Result<bool> {
    let super::RunContext { mut resp_rx, item, refresh_gen, refresh_bytes, buffered, mut shutdown_rx, .. } = ctx;

    let mut lt = super::LocalTerminal::new(item.cols, item.rows)?;
    lt.terminal.vt_write(&refresh_bytes);

    let mut stdout = tokio::io::stdout();
    let mut out = Vec::new();

    render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out)?;
    stdout.write_all(&out).await?;

    for (gen, data) in &buffered {
        if *gen > refresh_gen {
            lt.terminal.vt_write(data);
            out.clear();
            render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out)?;
            stdout.write_all(&out).await?;
        }
    }
    stdout.flush().await?;

    let mut sigwinch = signal(SignalKind::window_change())?;
    let mut server_closed = false;

    loop {
        out.clear();
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
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        lt.resize(mi.cols, mi.rows)?;
                                        out.extend_from_slice(b"\x1b[2J");
                                    }
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
                render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out)?;
            }
        }
        if !out.is_empty() {
            if stdout.write_all(&out).await.is_err() { break; }
            let _ = stdout.flush().await;
        }
    }

    Ok(server_closed)
}

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

    if global_dirty == Dirty::Full {
        let mut fmt = Formatter::new(terminal, FormatterOptions {
            format: Format::Vt,
            trim: false,
            unwrap: false,
            selection: None,
        })?;
        out.extend_from_slice(b"\x1b[2J\x1b[H");
        let vt = fmt.format_alloc(None)?;
        out.extend_from_slice(&vt);
    } else {
        let mut row_iter_active = row_iter.update(&snapshot)?;
        let mut row_idx: u32 = 0;
        let mut char_enc = [0u8; 4];
        let mut grapheme_buf: Vec<char> = Vec::new();

        while let Some(row) = row_iter_active.next() {
            if !row.dirty()? {
                row_idx += 1;
                continue;
            }
            write!(out, "\x1b[{};1H", row_idx + 1).ok();

            let mut cell_iter_active = cell_iter.update(row)?;
            while let Some(cell) = cell_iter_active.next() {
                let len = cell.graphemes_len()?;
                if len == 0 {
                    let bg = cell.bg_color().ok().flatten();
                    if let Some(c) = bg {
                        out.extend_from_slice(b"\x1b[0");
                        write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok();
                        out.extend_from_slice(b"m ");
                    } else {
                        out.push(b' ');
                    }
                } else {
                    if grapheme_buf.len() < len {
                        grapheme_buf.resize(len, '\0');
                    }
                    cell.graphemes_buf(&mut grapheme_buf[..len])?;
                    let graphemes = &grapheme_buf[..len];
                    let style = cell.style()?;
                    let fg = cell.fg_color().ok().flatten();
                    let bg = cell.bg_color().ok().flatten();

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
                    if let Some(c) = fg { write!(out, ";38;2;{};{};{}", c.r, c.g, c.b).ok(); }
                    if let Some(c) = bg { write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok(); }
                    out.push(b'm');

                    for ch in graphemes {
                        out.extend_from_slice(ch.encode_utf8(&mut char_enc).as_bytes());
                    }
                }
            }
            row.set_dirty(false)?;
            row_idx += 1;
        }
    }

    out.extend_from_slice(b"\x1b[0m");
    if cursor_visible { out.extend_from_slice(b"\x1b[?25h"); } else { out.extend_from_slice(b"\x1b[?25l"); }
    write!(out, "\x1b[{};{}H", cursor_y + 1, cursor_x + 1).ok();

    snapshot.set_dirty(Dirty::Clean)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_dirty_uses_formatter_clear() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        lt.terminal.vt_write(b"hello");
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[2J"), "formatter Full path must emit clear-screen");
    }

    #[test]
    fn partial_dirty_uses_cell_cursor_goto() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        lt.terminal.vt_write(b"hi");
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"), "Partial path must emit row cursor-goto");
        assert!(!s.contains("\x1b[2J"), "Partial path must not clear screen");
    }
}
```

- [ ] **Step 4: Run all tests**

```bash
cargo test 2>&1
```

Expected: all 5 tests pass (3 from cell, 2 from formatter).

- [ ] **Step 5: Commit**

```bash
git add src/attach/formatter.rs
git commit -m "feat: implement formatter render mode in attach/formatter.rs"
```

---

## Task 5 — Implement `raw.rs`

**Files:**
- Modify: `src/attach/raw.rs`

No libghostty. Bytes forwarded directly. SIGWINCH sends `Command::Refresh` and handles `Response::Refresh` in the main loop.

- [ ] **Step 1: Replace `raw.rs` with the full implementation**

```rust
use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    RefreshRequest, StreamMetadataReason, TerminalCommand,
};

pub(super) async fn run(ctx: super::RunContext) -> Result<bool> {
    let super::RunContext { mut resp_rx, cmd_tx, pty_id, refresh_gen: mut refresh_gen, refresh_bytes, buffered, mut shutdown_rx, .. } = ctx;

    let mut stdout = tokio::io::stdout();

    // Write initial state directly — no local terminal model
    stdout.write_all(&refresh_bytes).await?;
    for (gen, data) in &buffered {
        if *gen > refresh_gen {
            stdout.write_all(data).await?;
        }
    }
    stdout.flush().await?;

    let mut sigwinch = signal(SignalKind::window_change())?;
    let mut server_closed = false;

    loop {
        tokio::select! {
            msg = resp_rx.message() => {
                match msg {
                    Ok(Some(r)) => match r.response {
                        Some(Response::Stream(s)) => {
                            if s.generation > refresh_gen {
                                if stdout.write_all(&s.data).await.is_err() { break; }
                                let _ = stdout.flush().await;
                            }
                        }
                        Some(Response::Refresh(rf)) => {
                            // Response to a SIGWINCH-triggered refresh request
                            refresh_gen = rf.generation;
                            if stdout.write_all(&rf.data).await.is_err() { break; }
                            let _ = stdout.flush().await;
                        }
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                // Clear stale content; server will broadcast a Refresh next
                                let _ = stdout.write_all(b"\x1b[2J").await;
                                let _ = stdout.flush().await;
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
                // Request a fresh screen dump from the server; response arrives as Response::Refresh above
                let _ = cmd_tx.send(TerminalCommand {
                    command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
                }).await;
            }
        }
    }

    Ok(server_closed)
}
```

- [ ] **Step 2: Build and verify**

```bash
cargo build 2>&1
```

Expected: `Finished` with no errors.

- [ ] **Step 3: Run all tests**

```bash
cargo test 2>&1
```

Expected: all 5 tests still pass.

- [ ] **Step 4: Commit**

```bash
git add src/attach/raw.rs
git commit -m "feat: implement raw passthrough render mode in attach/raw.rs"
```

---

## Task 6 — Final cleanup

**Files:**
- Modify: `src/attach/mod.rs`

- [ ] **Step 1: Remove dead code from `src/attach/mod.rs`**

Remove these items that are no longer used in `mod.rs`:
- The `run_normal` function (now replaced by `cell::run`)
- Any remaining `render_dirty` function (moved to mode files)
- Any remaining `#[cfg(test)] mod tests` block (moved to `cell.rs`)
- The `#[allow(dead_code)]` annotation on `get_terminal_size` (it is now used by nothing in mod.rs — if it's only used there, remove the function too; if needed by a mode file, move it)

After cleanup, the imports in `mod.rs` should only include what `mod.rs` itself uses: `LocalTerminal`, `TerminalGuard`, `setup_raw_mode`, `run_stdin`, `run`, `run_debug`.

- [ ] **Step 2: Build and run all tests**

```bash
cargo build 2>&1 && cargo test 2>&1
```

Expected: `Finished`, all 5 tests pass, no warnings about unused imports.

- [ ] **Step 3: Smoke-check the CLI help**

```bash
./target/debug/termd attach --help 2>&1
```

Expected output includes `--render-mode <RENDER_MODE>` with values `cell`, `formatter`, `raw` and default `cell`.

- [ ] **Step 4: Final commit**

```bash
git add src/attach/mod.rs
git commit -m "chore: remove dead code from attach/mod.rs after mode extraction"
```
