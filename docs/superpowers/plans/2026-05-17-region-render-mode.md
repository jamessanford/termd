# Region Render Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--render-mode=region` to `termd attach` — raw PTY byte passthrough within a DECSTBM scroll region sized to the server PTY, with a streaming `VtFilter` that intercepts and rewrites conflicting escape sequences emitted by server-side programs.

**Architecture:** `VtFilter` is a stateful streaming byte filter — bytes in, (possibly rewritten) bytes out, state carried across `filter()` calls to handle escape sequences split across buffer boundaries. It implements a Normal/AfterEsc/InCsi state machine with a 32-byte safety limit on CSI accumulation. `region::run()` wraps it in a select loop identical in structure to `raw::run()`. No libghostty on the render path.

**Tech Stack:** Rust, Tokio async, `libc` (TIOCGWINSZ for client terminal size), `std::io::Write` for `write!` into `Vec<u8>`.

---

## File Map

| File | Change |
|---|---|
| `src/attach/region.rs` | **Create** — `VtFilter` + `region::run()` |
| `src/attach/mod.rs` | **Modify** — add `mod region;`, `RenderMode::Region` variant, dispatch arm |

`src/main.rs` needs no changes — `RenderMode` is imported from `attach` and clap picks up new variants automatically.

---

### Task 1: `VtFilter` skeleton — Normal and AfterEsc states

**Files:**
- Create: `src/attach/region.rs`

- [ ] **Step 1: Create `src/attach/region.rs` with the VtFilter skeleton and failing tests**

Write the entire file (tests at the bottom will drive implementation in this task):

```rust
use std::io::Write as IoWrite;

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_response::Response,
    StreamMetadataReason,
};

// ── VtFilter ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum CsiMode { Normal, Private }

#[derive(Clone, Copy, PartialEq)]
enum FilterState {
    Normal,
    AfterEsc,
    InCsi(CsiMode),
}

struct VtFilter {
    state:          FilterState,
    buf:            Vec<u8>,
    server_rows:    u32,
    server_cols:    u32,
    client_rows:    u32,
    client_cols:    u32,
    declrmm_active: bool,
    in_alt_screen:  bool,
}

impl VtFilter {
    fn new(server_rows: u32, server_cols: u32, client_rows: u32, client_cols: u32) -> Self {
        Self {
            state: FilterState::Normal,
            buf: Vec::new(),
            server_rows,
            server_cols,
            client_rows,
            client_cols,
            declrmm_active: false,
            in_alt_screen: false,
        }
    }

    fn effective_rows(&self) -> u32 { self.server_rows.min(self.client_rows) }
    fn effective_cols(&self) -> u32 { self.server_cols.min(self.client_cols) }

    fn update_region(&mut self, server_rows: u32, server_cols: u32) {
        self.server_rows = server_rows;
        self.server_cols = server_cols;
    }

    fn update_client_size(&mut self, client_rows: u32, client_cols: u32) {
        self.client_rows = client_rows;
        self.client_cols = client_cols;
    }

    fn emit_region_setup(&mut self, out: &mut Vec<u8>) {
        write!(out, "\x1b[1;{}r", self.effective_rows()).ok();
        if self.client_cols > self.server_cols {
            out.extend_from_slice(b"\x1b[?69h");
            write!(out, "\x1b[1;{}s", self.effective_cols()).ok();
            self.declrmm_active = true;
        }
    }

    fn filter(&mut self, input: &[u8], out: &mut Vec<u8>) {
        for &byte in input {
            match self.state {
                FilterState::Normal => {
                    if byte == 0x1b {
                        self.buf.push(byte);
                        self.state = FilterState::AfterEsc;
                    } else {
                        out.push(byte);
                    }
                }
                FilterState::AfterEsc => {
                    match byte {
                        b'[' => {
                            self.buf.push(byte);
                            self.state = FilterState::InCsi(CsiMode::Normal);
                        }
                        b'c' => {
                            // RIS: pass through, then re-emit region setup
                            out.extend_from_slice(b"\x1bc");
                            self.emit_region_setup(out);
                            self.buf.clear();
                            self.state = FilterState::Normal;
                        }
                        // String-sequence openers (OSC/DCS/APC/PM/SOS/ST):
                        // content can be kilobytes — flush ESC + byte immediately
                        b']' | b'P' | b'_' | b'^' | b'X' | b'\\' => {
                            out.extend_from_slice(&self.buf);
                            out.push(byte);
                            self.buf.clear();
                            self.state = FilterState::Normal;
                        }
                        _ => {
                            // Unknown two-char ESC sequence: pass both bytes through
                            out.extend_from_slice(&self.buf);
                            out.push(byte);
                            self.buf.clear();
                            self.state = FilterState::Normal;
                        }
                    }
                }
                FilterState::InCsi(_) => {
                    // Placeholder — implemented in Task 2
                    out.extend_from_slice(&self.buf);
                    out.push(byte);
                    self.buf.clear();
                    self.state = FilterState::Normal;
                }
            }
        }
    }
}

// ── run() placeholder — implemented in Task 5 ─────────────────────────────────

pub(super) async fn run(_ctx: super::RunContext) -> Result<bool> {
    todo!("region::run")
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_all(f: &mut VtFilter, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        f.filter(input, &mut out);
        out
    }

    #[test]
    fn plain_bytes_pass_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"hello world"), b"hello world");
    }

    #[test]
    fn esc_unknown_char_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC A is an unknown two-char sequence — both bytes must pass through
        assert_eq!(filter_all(&mut f, b"\x1bA"), b"\x1bA");
    }

    #[test]
    fn esc_string_opener_passes_immediately() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC ] is OSC opener — ESC + ] flushed immediately; rest flows through Normal
        assert_eq!(
            filter_all(&mut f, b"\x1b]0;window title\x07"),
            b"\x1b]0;window title\x07",
        );
    }

    #[test]
    fn esc_ris_emits_region() {
        let mut f = VtFilter::new(24, 80, 40, 80);
        // ESC c = RIS — pass RIS through, then re-emit DECSTBM
        let out = filter_all(&mut f, b"\x1bc");
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1bc"), "RIS must be passed through first");
        assert!(s.contains("\x1b[1;24r"), "DECSTBM region setup must follow RIS");
    }
}
```

- [ ] **Step 2: Run tests — all 4 should pass (AfterEsc is fully implemented)**

```bash
cargo test -p termd --lib attach::region 2>&1 | tail -15
```

Expected: `test result: ok. 4 passed`.

- [ ] **Step 3: Check compilation**

```bash
cargo check 2>&1 | grep "^error" | head -10
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add src/attach/region.rs
git commit -m "feat(region): VtFilter skeleton with Normal/AfterEsc states"
```

---

### Task 2: `VtFilter` — InCsi state + safety limit

**Files:**
- Modify: `src/attach/region.rs`

- [ ] **Step 1: Add failing InCsi tests to the `mod tests` block**

```rust
    #[test]
    fn csi_unknown_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ 5 A = cursor up 5 — unrecognized CSI, passes through unchanged
        assert_eq!(filter_all(&mut f, b"\x1b[5A"), b"\x1b[5A");
    }

    #[test]
    fn csi_safety_limit_flushes() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // A CSI sequence longer than 32 bytes must be flushed as-is, not buffered forever
        let long: Vec<u8> = b"\x1b[1;2;3;4;5;6;7;8;9;10;11;12;13".to_vec();
        let out = filter_all(&mut f, &long);
        assert!(out.starts_with(b"\x1b["), "safety flush must emit the accumulated bytes");
    }

    #[test]
    fn nested_esc_in_csi_flushes_buf() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ 5 ESC [ 2 J — nested ESC flushes incomplete first CSI
        let out = filter_all(&mut f, b"\x1b[5\x1b[2J");
        assert!(out.starts_with(b"\x1b[5"), "incomplete CSI must be flushed");
        assert!(out.ends_with(b"\x1b[2J"), "subsequent CSI must pass through");
    }

    #[test]
    fn csi_private_marker_detected() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ ? 25 h = show cursor — private CSI not in our rewrite table, passes through
        assert_eq!(filter_all(&mut f, b"\x1b[?25h"), b"\x1b[?25h");
    }
```

- [ ] **Step 2: Run tests to verify the 4 new ones fail**

```bash
cargo test -p termd --lib attach::region 2>&1 | tail -15
```

Expected: 4 new tests FAIL (InCsi placeholder emits bytes in wrong order).

- [ ] **Step 3: Replace the InCsi placeholder with full state handling; add `dispatch_csi` and `parse_csi_params` stubs**

In `filter()`, replace the `FilterState::InCsi(_) => { ... }` arm:

```rust
                FilterState::InCsi(mode) => {
                    if self.buf.len() > 32 {
                        // Safety: too many bytes accumulated — give up on this sequence
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        self.state = FilterState::Normal;
                        if byte == 0x1b {
                            self.buf.push(byte);
                            self.state = FilterState::AfterEsc;
                        } else {
                            out.push(byte);
                        }
                        continue;
                    }
                    if byte == 0x1b {
                        // Nested ESC: flush incomplete sequence, start new escape
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        self.buf.push(byte);
                        self.state = FilterState::AfterEsc;
                    } else if (0x40..=0x7e).contains(&byte) {
                        // Final byte (0x40–0x7E): sequence complete, dispatch
                        self.buf.push(byte);
                        self.dispatch_csi(mode, byte, out);
                        self.buf.clear();
                        self.state = FilterState::Normal;
                    } else if (0x20..=0x3f).contains(&byte) {
                        // Parameter bytes (0x30–0x3F) and intermediate bytes (0x20–0x2F)
                        if byte == b'?' && self.buf.len() == 2 {
                            // '?' immediately after '\x1b[' marks a private CSI
                            self.state = FilterState::InCsi(CsiMode::Private);
                        }
                        self.buf.push(byte);
                    } else {
                        // Unexpected byte: flush and pass through
                        out.extend_from_slice(&self.buf);
                        self.buf.clear();
                        out.push(byte);
                        self.state = FilterState::Normal;
                    }
                }
```

Add these two methods to `impl VtFilter` after `filter()`:

```rust
    fn dispatch_csi(&mut self, mode: CsiMode, final_byte: u8, out: &mut Vec<u8>) {
        // Stub: pass everything through — rewrite rules added in Tasks 3 and 4
        let _ = (mode, final_byte);
        out.extend_from_slice(&self.buf);
    }

    fn parse_csi_params(&self) -> Vec<u32> {
        // buf = "\x1b[" [maybe "?"] <params> <final_byte>
        // skip the leading "\x1b[" (2 bytes), skip "?" if private (1 more byte)
        let start = if self.buf.get(2) == Some(&b'?') { 3 } else { 2 };
        let end = self.buf.len().saturating_sub(1); // exclude final byte
        if start >= end {
            return vec![];
        }
        self.buf[start..end]
            .split(|&b| b == b';')
            .filter_map(|s| {
                let s = std::str::from_utf8(s).ok()?;
                Some(s.parse::<u32>().unwrap_or(0))
            })
            .collect()
    }
```

- [ ] **Step 4: Run all tests**

```bash
cargo test -p termd --lib attach::region 2>&1 | tail -15
```

Expected: `test result: ok. 8 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/attach/region.rs
git commit -m "feat(region): VtFilter InCsi state + safety limit"
```

---

### Task 3: `VtFilter` — DECSTBM rewriting

**Files:**
- Modify: `src/attach/region.rs`

- [ ] **Step 1: Add failing DECSTBM tests**

```rust
    #[test]
    fn decstbm_bare_reset_rewritten() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ r with no params = full-screen reset → rewritten to server bounds
        assert_eq!(filter_all(&mut f, b"\x1b[r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_bottom_clamped_to_server_rows() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // Bottom margin (30) exceeds server_rows (24) → clamped
        assert_eq!(filter_all(&mut f, b"\x1b[1;30r"), b"\x1b[1;24r");
    }

    #[test]
    fn decstbm_within_bounds_unchanged() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // Both margins within server bounds → rewritten to same values
        assert_eq!(filter_all(&mut f, b"\x1b[5;20r"), b"\x1b[5;20r");
    }

    #[test]
    fn decstbm_cross_buffer() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let mut out = Vec::new();
        // Sequence split across two filter() calls — state must be preserved
        f.filter(b"\x1b[1;30", &mut out);
        f.filter(b"r", &mut out);
        assert_eq!(out, b"\x1b[1;24r");
    }
```

- [ ] **Step 2: Run tests to verify the 4 new ones fail**

```bash
cargo test -p termd --lib attach::region 2>&1 | tail -15
```

Expected: 4 new DECSTBM tests FAIL (stub passes sequences through unchanged).

- [ ] **Step 3: Replace the `dispatch_csi` stub with DECSTBM handling**

Replace `dispatch_csi` in `impl VtFilter`:

```rust
    fn dispatch_csi(&mut self, mode: CsiMode, final_byte: u8, out: &mut Vec<u8>) {
        match (mode, final_byte) {
            (CsiMode::Normal, b'r') => {
                // DECSTBM: rewrite scroll region, clamping bottom to server_rows
                let params = self.parse_csi_params();
                let top = params.first().copied().filter(|&p| p != 0).unwrap_or(1);
                let bottom = params.get(1).copied().unwrap_or(0);
                let effective_bottom = if bottom == 0 || bottom > self.effective_rows() {
                    self.effective_rows()
                } else {
                    bottom
                };
                write!(out, "\x1b[{};{}r", top, effective_bottom).ok();
            }
            _ => out.extend_from_slice(&self.buf),
        }
    }
```

- [ ] **Step 4: Run all tests**

```bash
cargo test -p termd --lib attach::region 2>&1 | tail -15
```

Expected: `test result: ok. 12 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/attach/region.rs
git commit -m "feat(region): rewrite DECSTBM scroll-region sequences"
```

---

### Task 4: `VtFilter` — DECSLRM, DECLRMM suppression, alt-screen

**Files:**
- Modify: `src/attach/region.rs`

- [ ] **Step 1: Add failing tests**

```rust
    #[test]
    fn decslrm_right_clamped_to_server_cols() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // client_cols (120) > server_cols (80) → emit_region_setup sets declrmm_active
        let mut out = Vec::new();
        f.emit_region_setup(&mut out);
        out.clear();
        // Right margin (100) exceeds server_cols (80) → clamped
        f.filter(b"\x1b[1;100s", &mut out);
        assert_eq!(out, b"\x1b[1;80s");
    }

    #[test]
    fn declrmm_enable_suppressed() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // Server app tries to enable DECLRMM — we own this mode, suppress it
        assert_eq!(filter_all(&mut f, b"\x1b[?69h"), b"");
    }

    #[test]
    fn declrmm_disable_suppressed() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        assert_eq!(filter_all(&mut f, b"\x1b[?69l"), b"");
    }

    #[test]
    fn alt_screen_enter_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        let out = filter_all(&mut f, b"\x1b[?1049h");
        assert_eq!(out, b"\x1b[?1049h");
        assert!(f.in_alt_screen);
    }

    #[test]
    fn alt_screen_exit_reemits_region() {
        let mut f = VtFilter::new(24, 80, 40, 80);
        filter_all(&mut f, b"\x1b[?1049h"); // enter alt screen first
        let out = filter_all(&mut f, b"\x1b[?1049l");
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1b[?1049l"), "alt-screen exit byte must pass through");
        assert!(s.contains("\x1b[1;24r"), "region setup must be re-emitted after exit");
        assert!(!f.in_alt_screen);
    }

    #[test]
    fn other_private_mode_passes_through() {
        let mut f = VtFilter::new(24, 80, 40, 120);
        // ESC [ ? 25 h = show cursor — not intercepted
        assert_eq!(filter_all(&mut f, b"\x1b[?25h"), b"\x1b[?25h");
    }
```

- [ ] **Step 2: Run tests to verify the 6 new ones fail**

```bash
cargo test -p termd --lib attach::region 2>&1 | tail -15
```

Expected: 6 new tests FAIL.

- [ ] **Step 3: Replace `dispatch_csi` with the complete rewrite table**

```rust
    fn dispatch_csi(&mut self, mode: CsiMode, final_byte: u8, out: &mut Vec<u8>) {
        match (mode, final_byte) {
            (CsiMode::Normal, b'r') => {
                // DECSTBM: clamp bottom margin to server_rows
                let params = self.parse_csi_params();
                let top = params.first().copied().filter(|&p| p != 0).unwrap_or(1);
                let bottom = params.get(1).copied().unwrap_or(0);
                let effective_bottom = if bottom == 0 || bottom > self.effective_rows() {
                    self.effective_rows()
                } else {
                    bottom
                };
                write!(out, "\x1b[{};{}r", top, effective_bottom).ok();
            }
            (CsiMode::Normal, b's') if self.declrmm_active => {
                // DECSLRM: clamp right margin to server_cols
                // Only intercept when we've enabled DECLRMM; otherwise ESC[s = cursor save
                let params = self.parse_csi_params();
                let left = params.first().copied().filter(|&p| p != 0).unwrap_or(1);
                let right = params.get(1).copied().unwrap_or(0);
                let effective_right = if right == 0 || right > self.effective_cols() {
                    self.effective_cols()
                } else {
                    right
                };
                write!(out, "\x1b[{};{}s", left, effective_right).ok();
            }
            (CsiMode::Private, b'h') => {
                let params = self.parse_csi_params();
                match params.first().copied() {
                    Some(69) => {
                        // DECLRMM enable: suppress — we manage this ourselves
                    }
                    Some(1049) => {
                        // Alt-screen enter: pass through
                        out.extend_from_slice(&self.buf);
                        self.in_alt_screen = true;
                    }
                    _ => out.extend_from_slice(&self.buf),
                }
            }
            (CsiMode::Private, b'l') => {
                let params = self.parse_csi_params();
                match params.first().copied() {
                    Some(69) => {
                        // DECLRMM disable: suppress
                    }
                    Some(1049) => {
                        // Alt-screen exit: pass through, then re-emit region setup
                        out.extend_from_slice(&self.buf);
                        self.in_alt_screen = false;
                        self.emit_region_setup(out);
                    }
                    _ => out.extend_from_slice(&self.buf),
                }
            }
            _ => out.extend_from_slice(&self.buf),
        }
    }
```

- [ ] **Step 4: Run all tests**

```bash
cargo test -p termd --lib attach::region 2>&1 | tail -15
```

Expected: `test result: ok. 18 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/attach/region.rs
git commit -m "feat(region): DECSLRM, DECLRMM suppression, alt-screen handling"
```

---

### Task 5: Wire up `region::run()` and `RenderMode::Region`

**Files:**
- Modify: `src/attach/region.rs` — replace `run()` placeholder  
- Modify: `src/attach/mod.rs` — add `mod region;`, `Region` variant, dispatch arm

- [ ] **Step 1: Add `get_terminal_size` and complete `run()` in `region.rs`**

Add `get_terminal_size` before `run()` (uses `libc` which is already a project dependency):

```rust
fn get_terminal_size() -> (u32, u32) {
    let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws); }
    (ws.ws_col as u32, ws.ws_row as u32)
}
```

Replace the `run()` placeholder with the full select loop:

```rust
pub(super) async fn run(ctx: super::RunContext) -> Result<bool> {
    let super::RunContext {
        mut resp_rx, item, refresh_gen,
        refresh_bytes, buffered, mut shutdown_rx, ..
    } = ctx;

    let (client_cols, client_rows) = get_terminal_size();
    let server_rows = item.rows;
    let server_cols = item.cols;

    if client_rows < server_rows || client_cols < server_cols {
        eprintln!(
            "[region: client ({client_cols}x{client_rows}) smaller than \
             server ({server_cols}x{server_rows}), falling back to cell mode]"
        );
        // Reconstruct ctx and hand off
        let ctx = super::RunContext {
            resp_rx, cmd_tx: ctx.cmd_tx, pty_id: ctx.pty_id, item,
            refresh_gen, refresh_bytes, buffered, shutdown_rx,
        };
        return super::cell::run(ctx).await;
    }

    let mut filter = VtFilter::new(server_rows, server_cols, client_rows, client_cols);
    let mut stdout = tokio::io::stdout();
    let mut out = Vec::new();
    let mut current_refresh_gen = refresh_gen;

    // Emit region setup, then seed display from initial refresh data
    filter.emit_region_setup(&mut out);
    filter.filter(&refresh_bytes, &mut out);
    stdout.write_all(&out).await?;

    for (gen, data) in &buffered {
        if *gen > current_refresh_gen {
            out.clear();
            filter.filter(data, &mut out);
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
                            if s.generation > current_refresh_gen {
                                filter.filter(&s.data, &mut out);
                            }
                        }
                        Some(Response::Refresh(rf)) => {
                            current_refresh_gen = rf.generation;
                            filter.filter(&rf.data, &mut out);
                        }
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        filter.update_region(mi.rows, mi.cols);
                                        out.extend_from_slice(b"\x1b[2J");
                                        filter.emit_region_setup(&mut out);
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
                let (new_cols, new_rows) = get_terminal_size();
                if new_rows < filter.server_rows || new_cols < filter.server_cols {
                    eprintln!("[region: client shrank below server PTY size, display may be incomplete]");
                }
                filter.update_client_size(new_rows, new_cols);
                filter.emit_region_setup(&mut out);
            }
        }
        if !out.is_empty() {
            if stdout.write_all(&out).await.is_err() { break; }
            let _ = stdout.flush().await;
        }
    }

    // Cleanup: restore client terminal margins
    let _ = stdout.write_all(b"\x1b[r\x1b[?69l").await;
    let _ = stdout.flush().await;

    Ok(server_closed)
}
```

Note: The fallback path reconstructs `RunContext`. The `..` destructure at the top dropped `cmd_tx` and `pty_id`; for the fallback case, access them via `ctx.cmd_tx` and `ctx.pty_id` — but that requires `ctx` not to be consumed by destructuring. **Change the destructure** to use `ref` or just access `ctx` fields directly. Simplest fix: don't destructure at the top, access via `ctx.` throughout:

```rust
pub(super) async fn run(ctx: super::RunContext) -> Result<bool> {
    let (client_cols, client_rows) = get_terminal_size();
    let server_rows = ctx.item.rows;
    let server_cols = ctx.item.cols;

    if client_rows < server_rows || client_cols < server_cols {
        eprintln!(
            "[region: client ({client_cols}x{client_rows}) smaller than \
             server ({server_cols}x{server_rows}), falling back to cell mode]"
        );
        return super::cell::run(ctx).await;
    }

    let super::RunContext {
        mut resp_rx, refresh_gen,
        refresh_bytes, buffered, mut shutdown_rx, ..
    } = ctx;
    // item, cmd_tx, pty_id dropped via .. — server_rows/cols already captured above

    let mut filter = VtFilter::new(server_rows, server_cols, client_rows, client_cols);
    let mut stdout = tokio::io::stdout();
    let mut out = Vec::new();
    let mut current_refresh_gen = refresh_gen;

    filter.emit_region_setup(&mut out);
    filter.filter(&refresh_bytes, &mut out);
    stdout.write_all(&out).await?;

    for (gen, data) in &buffered {
        if *gen > current_refresh_gen {
            out.clear();
            filter.filter(data, &mut out);
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
                            if s.generation > current_refresh_gen {
                                filter.filter(&s.data, &mut out);
                            }
                        }
                        Some(Response::Refresh(rf)) => {
                            current_refresh_gen = rf.generation;
                            filter.filter(&rf.data, &mut out);
                        }
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        filter.update_region(mi.rows, mi.cols);
                                        out.extend_from_slice(b"\x1b[2J");
                                        filter.emit_region_setup(&mut out);
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
                let (new_cols, new_rows) = get_terminal_size();
                if new_rows < filter.server_rows || new_cols < filter.server_cols {
                    eprintln!("[region: client shrank below server PTY size, display may be incomplete]");
                }
                filter.update_client_size(new_rows, new_cols);
                filter.emit_region_setup(&mut out);
            }
        }
        if !out.is_empty() {
            if stdout.write_all(&out).await.is_err() { break; }
            let _ = stdout.flush().await;
        }
    }

    // Cleanup: restore client terminal margins regardless of how DECLRMM was managed
    let _ = stdout.write_all(b"\x1b[r\x1b[?69l").await;
    let _ = stdout.flush().await;

    Ok(server_closed)
}
```

The `item` field is used for `server_rows`/`server_cols` only before the destructure, so this structure works cleanly.

- [ ] **Step 2: Add `mod region;`, `RenderMode::Region`, and dispatch arm to `src/attach/mod.rs`**

Add `mod region;` after `mod raw;`:

```rust
mod cell;
mod formatter;
mod raw;
mod region;
```

Add `Region` to the `RenderMode` enum:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states (default)
    Cell,
    /// VT formatter for full repaints, cell-by-cell for partial repaints
    Formatter,
    /// Raw PTY byte passthrough — no libghostty on the client render path
    Raw,
    /// Raw passthrough within a DECSTBM scroll region; rewrites conflicting sequences
    Region,
}
```

Add the dispatch arm in `pub async fn run()`:

```rust
    let server_closed = match mode {
        RenderMode::Cell      => cell::run(ctx).await?,
        RenderMode::Formatter => formatter::run(ctx).await?,
        RenderMode::Raw       => raw::run(ctx).await?,
        RenderMode::Region    => region::run(ctx).await?,
    };
```

- [ ] **Step 3: Build and check `--help`**

```bash
cargo build 2>&1 | grep "^error" | head -10
```

Expected: no errors.

```bash
./run-termd attach --help 2>&1 | grep -A3 "render-mode"
```

Expected output includes:
```
--render-mode <RENDER_MODE>
  [default: cell] [possible values: cell, formatter, raw, region]
```

- [ ] **Step 4: Run full test suite**

```bash
cargo test 2>&1 | tail -15
```

Expected: 18 VtFilter tests pass. The pre-existing `test_closed_broadcasts_metadata` integration test failure is unrelated to this work and expected.

- [ ] **Step 5: Commit**

```bash
git add src/attach/region.rs src/attach/mod.rs
git commit -m "feat(region): wire up RenderMode::Region and region::run()"
```
