# render-mode=autowrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `render-mode=autowrap` that forwards raw server PTY bytes for passthrough, drops all horizontal-margin handling, and injects explicit line breaks at soft-wrap points detected by a tracking libghostty terminal, so output is correct on any client of width ≥ server width.

**Architecture:** A self-contained `WrapInjector` in `src/attach/autowrap.rs` owns a tracking `libghostty_vt::Terminal` sized to the server. It classifies the byte stream into printable glyphs vs control/escape sequences. For each printable glyph it records cursor position before and after feeding; if the cursor wrapped (row advanced **or** column moved left), it injects `\r\n` before the glyph. Control/escape bytes pass through a small DECSTBM-clamping filter. The `AutowrapHandler` wires this into the existing `RenderModeHandler` trait and falls back to cell mode when the client is too small.

**Tech Stack:** Rust, `libghostty_vt` (`Terminal`, `vt_write`, `vt_at_boundary`, `cursor_x`, `cursor_y`), `anyhow`, existing `src/attach` dispatch machinery.

## Global Constraints

- Do **not** modify the behavior of `render-mode=cell` or `render-mode=region` (the `upgrade_to` refactor in Task 1 is behavior-preserving for region).
- No `?69` / DECSLRM / DECLRMM handling anywhere in autowrap — horizontal sequences pass straight through.
- DECSTBM (`\x1b[1;{server_rows}r`) is emitted only at reset points: `init`, refresh, resize — never on ordinary `Stream` events. Top margin is always row 1.
- Wrap detection uses cursor-position movement only (no reliance on `is_cursor_pending_wrap()` alone, which misses wide-char-at-edge and bottom-margin-scroll wraps — verified against ghostty's `printWrap`/`spacer_head` logic in `examples/ghostty/src/terminal/Terminal.zig`).
- Inject the literal two bytes `\r\n` (`b"\r\n"`) at a wrap point.
- Tests are inline `#[cfg(test)] mod tests` in `autowrap.rs`, matching `cell.rs`/`region.rs`.
- `cursor_x()` / `cursor_y()` are 0-indexed and return `Result<u16>`.

---

### Task 1: Foundation — `Autowrap` variant, `upgrade_to` refactor, dispatch wiring, stub handler

Adds the mode enum variant, converts cell's upgrade target from a bool to a mode, wires dispatch, and creates a compiling pass-through stub handler. No injection yet.

**Files:**
- Modify: `src/attach/mod.rs` (enum `RenderMode`, `create_handler`, `run` dispatch around line 632, module decl)
- Modify: `src/attach/cell.rs:34-101` (`allow_upgrade: bool` → `upgrade_to: Option<RenderMode>`)
- Create: `src/attach/autowrap.rs`

**Interfaces:**
- Consumes: `RenderModeHandler` trait, `EventResult`, `PtyEvent`, `server_fits_client`, `get_terminal_size` (all in `src/attach/mod.rs`).
- Produces:
  - `RenderMode::Autowrap`
  - `autowrap::AutowrapHandler` with `pub(super) fn new(server_cols: u32, server_rows: u32) -> anyhow::Result<Self>`
  - `cell::CellHandler::new(cols: u32, rows: u32, upgrade_to: Option<RenderMode>) -> Result<Self>`
  - `create_handler(mode, server_cols, server_rows, upgrade_to: Option<RenderMode>) -> Result<Box<dyn RenderModeHandler>>`

- [ ] **Step 1: Add the enum variant and module declaration**

In `src/attach/mod.rs`, add the variant to `RenderMode` (after `Region`):

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states
    Cell,
    /// Raw PTY byte passthrough
    Raw,
    /// Raw passthrough within a DECSTBM scroll region
    Region,
    /// Raw passthrough with libghostty-driven explicit wrap injection
    Autowrap,
}
```

And add the module next to the others (near `mod region;`):

```rust
mod autowrap;
```

- [ ] **Step 2: Create the stub handler**

Create `src/attach/autowrap.rs`:

```rust
use anyhow::Result;

pub(super) struct AutowrapHandler {
    server_cols: u32,
    server_rows: u32,
}

impl AutowrapHandler {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self { server_cols, server_rows })
    }
}

impl super::RenderModeHandler for AutowrapHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        out.extend_from_slice(refresh_data);
        for (_gen, data) in buffered {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        if let super::PtyEvent::Stream { data, .. } = event {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, _cols: u32, _rows: u32, _out: &mut Vec<u8>) -> Result<super::EventResult> {
        Ok(super::EventResult::Continue)
    }
}
```

- [ ] **Step 3: Refactor cell.rs upgrade target from bool to Option<RenderMode>**

In `src/attach/cell.rs`, change the struct field and constructor:

```rust
pub(super) struct CellHandler {
    lt: LocalTerminal,
    upgrade_to: Option<super::RenderMode>,
    server_cols: u32,
    server_rows: u32,
}

impl CellHandler {
    pub(super) fn new(cols: u32, rows: u32, upgrade_to: Option<super::RenderMode>) -> Result<Self> {
        Ok(Self {
            lt: LocalTerminal::new(cols, rows)?,
            upgrade_to,
            server_cols: cols,
            server_rows: rows,
        })
    }
}
```

Replace the three upgrade sites. The Refresh arm (was lines 73-75):

```rust
                let (client_cols, client_rows) = super::get_terminal_size();
                if let Some(target) = self.upgrade_to {
                    if super::server_fits_client(cols, rows, client_cols, client_rows) {
                        return Ok(super::EventResult::ChangeRenderMode(target));
                    }
                }
```

The Resize arm (was lines 83-86):

```rust
                let (client_cols, client_rows) = super::get_terminal_size();
                if let Some(target) = self.upgrade_to {
                    if super::server_fits_client(cols, rows, client_cols, client_rows) {
                        return Ok(super::EventResult::ChangeRenderMode(target));
                    }
                }
```

The `on_sigwinch` site (was lines 95-97):

```rust
        if let Some(target) = self.upgrade_to {
            if super::server_fits_client(self.server_cols, self.server_rows, cols, rows) {
                return Ok(super::EventResult::ChangeRenderMode(target));
            }
        }
```

- [ ] **Step 4: Update create_handler and the dispatch in mod.rs**

In `src/attach/mod.rs`, change `create_handler`:

```rust
fn create_handler(
    mode: RenderMode,
    server_cols: u32,
    server_rows: u32,
    upgrade_to: Option<RenderMode>,
) -> anyhow::Result<Box<dyn RenderModeHandler>> {
    Ok(match mode {
        RenderMode::Cell => Box::new(cell::CellHandler::new(server_cols, server_rows, upgrade_to)?),
        RenderMode::Raw => Box::new(raw::RawHandler::new()),
        RenderMode::Region => {
            let (client_cols, client_rows) = get_terminal_size();
            Box::new(region::RegionHandler::new(server_rows, server_cols, client_rows, client_cols))
        }
        RenderMode::Autowrap => Box::new(autowrap::AutowrapHandler::new(server_cols, server_rows)?),
    })
}
```

In `run` (was line 632), derive the upgrade target from the originally-requested mode and thread it through every `create_handler` call (there are four call sites — replace the `allow_upgrade` argument with `upgrade_to` at each):

```rust
    let upgrade_to = match mode {
        RenderMode::Region => Some(RenderMode::Region),
        RenderMode::Autowrap => Some(RenderMode::Autowrap),
        _ => None,
    };
```

Then replace each `..., allow_upgrade,` / `..., allow_upgrade)?` argument in the four `create_handler(...)` calls with `..., upgrade_to,` / `..., upgrade_to)?`.

- [ ] **Step 5: Build to verify it compiles**

Run: `cargo build`
Expected: builds with no errors. (Warnings about unused `server_cols`/`server_rows` in `AutowrapHandler` are acceptable at this stage.)

- [ ] **Step 6: Commit**

```bash
git add src/attach/mod.rs src/attach/cell.rs src/attach/autowrap.rs
git commit -m "attach: add Autowrap render mode variant and upgrade_to refactor

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `WrapInjector` — stream classification and tracking-terminal sync (no injection yet)

Builds the core struct that owns the tracking terminal and walks the byte stream, classifying it into printable glyphs vs C0 controls vs escape/CSI sequences while keeping the tracking terminal in sync. Output is byte-identical to input at this stage; this isolates the (subtle) classification state machine before adding injection.

> **Why escape handling lives here, not in Task 4:** the bytes *after* `ESC` (`[`, digits, final byte) are all `>= 0x20`, so without an explicit escape state the classifier would misroute them into the glyph path. A cursor-moving sequence (e.g. CUP) would then move the cursor and, once Task 3 adds injection, trip a spurious break. So the three-way classification (Ground / InGlyph / InEscape) must be complete before injection is added.

**Files:**
- Modify: `src/attach/autowrap.rs`

**Interfaces:**
- Consumes: `libghostty_vt::{Terminal, TerminalOptions}`, `Terminal::vt_write`, `Terminal::vt_at_boundary`.
- Produces:
  - `struct WrapInjector` with:
    - `fn new(server_cols: u32, server_rows: u32) -> anyhow::Result<WrapInjector>`
    - `fn process(&mut self, input: &[u8], out: &mut Vec<u8>)`
    - `fn reset(&mut self, server_cols: u32, server_rows: u32) -> anyhow::Result<()>` (rebuilds the tracking terminal; used by Task 5 on refresh/resize)

- [ ] **Step 1: Write the failing tests for passthrough + cross-chunk splitting**

Add to `src/attach/autowrap.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn run_bytes(server_cols: u32, server_rows: u32, chunks: &[&[u8]]) -> Vec<u8> {
        let mut wi = WrapInjector::new(server_cols, server_rows).unwrap();
        let mut out = Vec::new();
        for c in chunks {
            wi.process(c, &mut out);
        }
        out
    }

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(run_bytes(80, 24, &[b"hello world"]), b"hello world");
    }

    #[test]
    fn escape_sequence_passes_through() {
        // A CUP and an SGR sequence, no printing past the edge: identity.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[5;5Hhi\x1b[0m"]), b"\x1b[5;5Hhi\x1b[0m");
    }

    #[test]
    fn utf8_split_across_chunks_passes_through() {
        // "é" = 0xC3 0xA9 split across two process() calls.
        assert_eq!(run_bytes(80, 24, &[b"a\xc3", b"\xa9b"]), b"a\xc3\xa9b");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib attach::autowrap`
Expected: FAIL — `WrapInjector` and `process` not found.

- [ ] **Step 3: Implement WrapInjector with classification + tracking sync (no injection)**

Add to `src/attach/autowrap.rs` (above the test module). The classifier persists across `process` calls via `state`. A printable glyph (lead byte `>= 0x20`, `!= 0x7f`) is accumulated byte-by-byte until the parser returns to a boundary. `ESC` (`0x1b`) opens an escape/CSI sequence accumulated in `seq` until the boundary, then emitted verbatim (Task 4 adds DECSTBM clamping here). Other C0 controls (CR, LF, HT, BEL, ...) are single bytes forwarded immediately. Wrap detection is added in Task 3; here every glyph is just flushed.

```rust
use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions};

/// Where the classifier is between `process` calls.
enum State {
    /// Parser is at a clean boundary; next byte starts a fresh unit.
    Ground,
    /// Accumulating the bytes of one printable glyph (multi-byte UTF-8 or
    /// base+combining run). `x0`/`y0` are the cursor position before the glyph.
    InGlyph { x0: u16, y0: u16 },
    /// Inside an escape/CSI/OSC sequence; bytes buffered in `seq` until boundary.
    InEscape,
}

pub(super) struct WrapInjector {
    term: Terminal<'static, 'static>,
    state: State,
    /// Buffered bytes of the in-progress glyph, not yet emitted.
    glyph: Vec<u8>,
    /// Buffered bytes of the in-progress escape sequence, not yet emitted.
    seq: Vec<u8>,
}

impl WrapInjector {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self {
            term: Terminal::new(TerminalOptions {
                cols: server_cols as u16,
                rows: server_rows as u16,
                max_scrollback: 0,
            })?,
            state: State::Ground,
            glyph: Vec::new(),
            seq: Vec::new(),
        })
    }

    pub(super) fn reset(&mut self, server_cols: u32, server_rows: u32) -> Result<()> {
        self.term = Terminal::new(TerminalOptions {
            cols: server_cols as u16,
            rows: server_rows as u16,
            max_scrollback: 0,
        })?;
        self.state = State::Ground;
        self.glyph.clear();
        self.seq.clear();
        Ok(())
    }

    /// True if `b`, at a parser boundary, begins a printable glyph (as opposed
    /// to a C0 control or escape sequence). UTF-8 lead/continuation bytes and
    /// invalid bytes are all >= 0x20; the parser/`vt_at_boundary` assembles them.
    fn is_printable_start(b: u8) -> bool {
        b >= 0x20 && b != 0x7f
    }

    /// Feed one byte to the tracking terminal.
    fn feed(&mut self, b: u8) {
        self.term.vt_write(&[b]);
    }

    /// Called when a complete glyph has been fed (parser back at boundary).
    /// Emits any injected break (Task 3) followed by the buffered glyph bytes.
    fn flush_glyph(&mut self, _x0: u16, _y0: u16, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.glyph);
        self.glyph.clear();
    }

    /// Called when a complete escape sequence has been fed. Emits it verbatim.
    /// (Task 4 replaces this with DECSTBM-clamping logic.)
    fn emit_sequence(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seq);
        self.seq.clear();
    }

    pub(super) fn process(&mut self, input: &[u8], out: &mut Vec<u8>) {
        for &b in input {
            match self.state {
                State::Ground => {
                    if Self::is_printable_start(b) {
                        let x0 = self.term.cursor_x().unwrap_or(0);
                        let y0 = self.term.cursor_y().unwrap_or(0);
                        self.glyph.push(b);
                        self.feed(b);
                        if self.term.vt_at_boundary() {
                            self.flush_glyph(x0, y0, out);
                            // stay in Ground
                        } else {
                            self.state = State::InGlyph { x0, y0 };
                        }
                    } else if b == 0x1b {
                        self.seq.clear();
                        self.seq.push(b);
                        self.feed(b);
                        // A lone ESC leaves the parser mid-sequence; if the
                        // sequence is somehow already complete (it never is for
                        // a bare ESC) we'd still be safe, but normally we wait.
                        if self.term.vt_at_boundary() {
                            self.emit_sequence(out);
                        } else {
                            self.state = State::InEscape;
                        }
                    } else {
                        // Other C0 control (CR, LF, HT, BEL, BS, ...): forward.
                        self.feed(b);
                        out.push(b);
                    }
                }
                State::InGlyph { x0, y0 } => {
                    self.glyph.push(b);
                    self.feed(b);
                    if self.term.vt_at_boundary() {
                        self.flush_glyph(x0, y0, out);
                        self.state = State::Ground;
                    }
                }
                State::InEscape => {
                    self.seq.push(b);
                    self.feed(b);
                    if self.term.vt_at_boundary() {
                        self.emit_sequence(out);
                        self.state = State::Ground;
                    }
                }
            }
        }
    }
}
```

> Note: every byte is fed to the tracking terminal exactly once, regardless of which path emits it, so the tracking cursor always reflects the true server state. Escape sequences are emitted verbatim and never run through the glyph path, so cursor-moving sequences (CUP, etc.) cannot trip the wrap detector added in Task 3.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib attach::autowrap`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "attach/autowrap: WrapInjector stream classification and tracking sync

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Wrap injection + edge-case corpus

Adds the actual `\r\n` injection, driven by cursor movement across each printable glyph, and the wrap edge-case test corpus.

**Files:**
- Modify: `src/attach/autowrap.rs` (`flush_glyph`)

**Interfaces:**
- Consumes: `WrapInjector` from Task 2, `Terminal::cursor_x`, `Terminal::cursor_y`.
- Produces: no new public symbols; changes `flush_glyph` behavior to inject `b"\r\n"` before a wrapped glyph.

- [ ] **Step 1: Write the failing corpus tests**

Add to the `tests` module in `src/attach/autowrap.rs`. Use a narrow 4×3 server so boundaries are easy to hit. (`█` = U+2588, a 1-wide full block; `世` = U+4E16, a 2-wide CJK glyph.)

```rust
    // 4 columns wide, 3 rows tall server.

    #[test]
    fn full_line_then_printable_injects_break() {
        // "abcd" fills cols 0..3 (deferred wrap), "e" then wraps.
        // Expect a break injected before "e".
        assert_eq!(run_bytes(4, 3, &[b"abcde"]), b"abcd\r\ne");
    }

    #[test]
    fn full_line_then_control_does_not_inject() {
        // "abcd" sets pending wrap; a CUP move clears it without wrapping.
        // No spurious break may be injected.
        assert_eq!(run_bytes(4, 3, &[b"abcd\x1b[2;1Hx"]), b"abcd\x1b[2;1Hx");
    }

    #[test]
    fn exact_fill_no_premature_break() {
        // Exactly cols glyphs and nothing after: no break at all.
        assert_eq!(run_bytes(4, 3, &[b"abcd"]), b"abcd");
    }

    #[test]
    fn wide_char_at_edge_injects_break() {
        // "abc" fills cols 0..2, leaving one column. A 2-wide glyph cannot fit,
        // so the server wraps it to the next line. Detected by cursor movement
        // even though pending-wrap was not set. Expect a break before "世".
        let out = run_bytes(4, 3, &["abc世".as_bytes()]);
        assert_eq!(out, "abc\r\n世".as_bytes());
    }

    #[test]
    fn bottom_margin_scroll_injects_break() {
        // Fill row 0 ("abcd"), wrap to row 1 with "efgh", wrap to row 2 (last)
        // with "ijkl", then "m" wraps again — at the bottom the screen scrolls
        // and cursor_y does not increase, but cursor_x moves left. Expect a
        // break before each wrap, including the scrolling one before "m".
        let out = run_bytes(4, 3, &[b"abcdefghijklm"]);
        assert_eq!(out, b"abcd\r\nefgh\r\nijkl\r\nm");
    }

    #[test]
    fn combining_mark_does_not_inject() {
        // base 'e' + combining acute U+0301 (0xCC 0x81), width 0, attaches to
        // the same cell — no wrap, no break.
        let out = run_bytes(4, 3, &["e\u{0301}f".as_bytes()]);
        assert_eq!(out, "e\u{0301}f".as_bytes());
    }

    #[test]
    fn tab_does_not_inject_a_break() {
        // HT is a C0 control on the control path; it advances the cursor but
        // never itself causes a wrap, so no break may be injected for it. (The
        // exact column HT lands on is terminal-defined; we only assert that the
        // tab passes through untouched. A printable that wraps *after* a tab is
        // covered by the cursor-movement detector, exercised in the other
        // injection tests.)
        assert_eq!(run_bytes(4, 3, &[b"\t"]), b"\t");
    }

    #[test]
    fn wrap_survives_chunk_split() {
        // Same as full_line_then_printable, but "e" arrives in a later chunk and
        // the 4th glyph is split mid-stream. The break must still land before "e".
        assert_eq!(run_bytes(4, 3, &[b"abc", b"d", b"e"]), b"abcd\r\ne");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib attach::autowrap`
Expected: FAIL — glyphs are emitted without injected breaks (e.g. `full_line_then_printable_injects_break` gets `abcde`, expected `abcd\r\ne`).

- [ ] **Step 3: Implement injection in flush_glyph**

Replace `flush_glyph` in `src/attach/autowrap.rs`:

```rust
    /// Called when a complete glyph has been fed (parser back at boundary).
    /// If feeding the glyph moved the cursor to a new row (row advanced) or back
    /// toward the left edge (a wrap that scrolled at the bottom margin), the
    /// server soft-wrapped — inject `\r\n` before the glyph so the wider client
    /// wraps at the same point. Otherwise emit the glyph unchanged.
    fn flush_glyph(&mut self, x0: u16, y0: u16, out: &mut Vec<u8>) {
        let x1 = self.term.cursor_x().unwrap_or(x0);
        let y1 = self.term.cursor_y().unwrap_or(y0);
        let wrapped = y1 > y0 || x1 < x0;
        if wrapped {
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(&self.glyph);
        self.glyph.clear();
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib attach::autowrap`
Expected: PASS (all Task 2 + Task 3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "attach/autowrap: inject line breaks at detected soft-wrap points

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: DECSTBM vertical framing + app-DECSTBM clamping

Adds the vertical scroll-region setup the client needs and clamps the app's own DECSTBM so it cannot exceed the server's rows. This is a small streaming filter on the control path only (no horizontal handling).

**Files:**
- Modify: `src/attach/autowrap.rs`

**Interfaces:**
- Consumes: `WrapInjector` internals.
- Produces:
  - `fn emit_region_setup(&self, out: &mut Vec<u8>)` on `WrapInjector` — emits `\x1b[1;{server_rows}r`.
  - app-DECSTBM clamping inside the control path of `process`.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
    #[test]
    fn region_setup_emits_decstbm() {
        let mut wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.emit_region_setup(&mut out);
        assert_eq!(out, b"\x1b[1;3r");
    }

    #[test]
    fn app_decstbm_bottom_is_clamped_to_server_rows() {
        // App tries to set a scroll region 1..10 on a 3-row server; clamp to 1..3.
        assert_eq!(run_bytes(4, 3, &[b"\x1b[1;10r"]), b"\x1b[1;3r");
    }

    #[test]
    fn app_decstbm_within_bounds_passes_through() {
        assert_eq!(run_bytes(4, 3, &[b"\x1b[2;3r"]), b"\x1b[2;3r");
    }

    #[test]
    fn app_decstbm_reset_passes_through() {
        // A bare \x1b[r (reset scroll region) passes through unchanged.
        assert_eq!(run_bytes(4, 3, &[b"\x1b[r"]), b"\x1b[r");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib attach::autowrap`
Expected: FAIL — `emit_region_setup` not found; app DECSTBM passes through unclamped.

- [ ] **Step 3: Add the `server_rows` field, region setup, and DECSTBM clamping**

The classification state machine (`Ground` / `InGlyph` / `InEscape`, with `seq` buffering and `emit_sequence`) already exists from Task 2. This task only: (a) adds a `server_rows` field so the injector knows the row count, (b) adds `emit_region_setup`, and (c) replaces the verbatim body of `emit_sequence` with DECSTBM-clamping logic.

In `src/attach/autowrap.rs`, add the `server_rows` field to the struct:

```rust
pub(super) struct WrapInjector {
    term: Terminal<'static, 'static>,
    state: State,
    glyph: Vec<u8>,
    seq: Vec<u8>,
    server_rows: u32,
}
```

Set `server_rows` in both `new` and `reset` (both already receive `server_rows: u32`): add `server_rows,` to the `Self { ... }` literal in `new`, and `self.server_rows = server_rows;` in `reset`.

Add `emit_region_setup`:

```rust
    pub(super) fn emit_region_setup(&self, out: &mut Vec<u8>) {
        use std::io::Write as _;
        write!(out, "\x1b[1;{}r", self.server_rows).ok();
    }
```

Replace the Task 2 `emit_sequence` (verbatim) with the clamping version, and add the `clamp_decstbm` helper beside it:

```rust
    /// Emit a completed escape sequence (in `self.seq`), clamping a DECSTBM
    /// bottom margin to the server row count. All other sequences pass verbatim.
    fn emit_sequence(&mut self, out: &mut Vec<u8>) {
        let seq = std::mem::take(&mut self.seq);
        if let Some(clamped) = self.clamp_decstbm(&seq) {
            out.extend_from_slice(&clamped);
        } else {
            out.extend_from_slice(&seq);
        }
    }

    /// If `seq` is a DECSTBM (`\x1b[<top>;<bottom>r`, no private marker) whose
    /// bottom exceeds `server_rows`, return a clamped rewrite. A bare `\x1b[r`
    /// (reset) and in-bounds regions return `None` (emit verbatim).
    fn clamp_decstbm(&self, seq: &[u8]) -> Option<Vec<u8>> {
        // Must be CSI ... 'r' with a non-private parameter body.
        if seq.len() < 3 || seq[0] != 0x1b || seq[1] != b'[' || *seq.last().unwrap() != b'r' {
            return None;
        }
        let params = &seq[2..seq.len() - 1];
        if params.first() == Some(&b'?') {
            return None; // private mode, not DECSTBM
        }
        if params.is_empty() {
            return None; // bare reset \x1b[r — pass through
        }
        // Parse "top;bottom" (either may be empty/absent).
        let mut parts = params.split(|&c| c == b';');
        let top = parts.next().unwrap_or(b"");
        let bottom = parts.next().unwrap_or(b"");
        if parts.next().is_some() {
            return None; // more than two params: not a DECSTBM we model
        }
        // Validate digits only.
        if !top.iter().all(|c| c.is_ascii_digit()) || !bottom.iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let bottom_val: u32 = std::str::from_utf8(bottom).ok()?.parse().ok().unwrap_or(0);
        if bottom.is_empty() || bottom_val <= self.server_rows {
            return None; // already in-bounds (or default bottom = screen)
        }
        let top_str = std::str::from_utf8(top).ok()?;
        let mut rewritten = Vec::new();
        use std::io::Write as _;
        write!(rewritten, "\x1b[{};{}r", top_str, self.server_rows).ok();
        Some(rewritten)
    }
```

> Note: `emit_sequence`/`clamp_decstbm` only rewrites the bytes sent to the client; the tracking terminal was already fed the original bytes (so its own scroll region matches the app's intent within the server grid). This keeps wrap accounting correct while the client stays clamped.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib attach::autowrap`
Expected: PASS (all prior tests + the four new DECSTBM tests).

- [ ] **Step 5: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "attach/autowrap: DECSTBM region setup and app scroll-region clamping

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Handler integration — wire WrapInjector into AutowrapHandler with fallback

Replaces the Task 1 stub handler body with the real one: emit region setup and run all bytes through the `WrapInjector`, fall back to cell mode when the client is too small, and rebuild the tracking terminal on refresh/resize.

**Files:**
- Modify: `src/attach/autowrap.rs` (`AutowrapHandler`)

**Interfaces:**
- Consumes: `WrapInjector::{new, process, reset, emit_region_setup}`, `super::server_fits_client`, `super::get_terminal_size`, `super::{EventResult, PtyEvent, RenderMode}`.
- Produces: final `AutowrapHandler` behavior. No new public symbols.

- [ ] **Step 1: Write the failing fallback test**

Add to the `tests` module:

```rust
    use super::super::{EventResult, PtyEvent, RenderMode, RenderModeHandler};

    #[test]
    fn falls_back_to_cell_when_client_too_small_on_resize() {
        // Server grows wider than the client on resize -> hand off to cell mode.
        // get_terminal_size() reads the real terminal; to keep this deterministic
        // we drive the Resize arm with a server size guaranteed larger than any
        // plausible test terminal.
        let mut h = AutowrapHandler::new(80, 24).unwrap();
        let mut out = Vec::new();
        let r = h.on_pty_event(PtyEvent::Resize { cols: 100_000, rows: 100_000 }, &mut out).unwrap();
        match r {
            EventResult::ChangeRenderMode(RenderMode::Cell) => {}
            _ => panic!("expected fallback to Cell"),
        }
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib attach::autowrap`
Expected: FAIL — stub `on_pty_event` ignores `Resize` and returns `Continue`.

- [ ] **Step 3: Implement the real handler**

Replace `AutowrapHandler` and its impl in `src/attach/autowrap.rs`:

```rust
pub(super) struct AutowrapHandler {
    inj: WrapInjector,
    server_cols: u32,
    server_rows: u32,
}

impl AutowrapHandler {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self {
            inj: WrapInjector::new(server_cols, server_rows)?,
            server_cols,
            server_rows,
        })
    }

    fn fits_client(&self) -> bool {
        let (client_cols, client_rows) = super::get_terminal_size();
        super::server_fits_client(self.server_cols, self.server_rows, client_cols, client_rows)
    }
}

impl super::RenderModeHandler for AutowrapHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !self.fits_client() {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        self.inj.reset(self.server_cols, self.server_rows)?;
        self.inj.emit_region_setup(out);
        self.inj.process(refresh_data, out);
        for (_gen, data) in buffered {
            self.inj.process(data, out);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                self.inj.process(data, out);
            }
            super::PtyEvent::Refresh { cols, rows, data, .. } => {
                self.server_cols = cols;
                self.server_rows = rows;
                if !self.fits_client() {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                // Refresh is a full reset/redraw: rebuild the tracking terminal
                // and re-emit the region setup before replaying the repaint.
                self.inj.reset(cols, rows)?;
                self.inj.emit_region_setup(out);
                self.inj.process(data, out);
            }
            super::PtyEvent::Resize { cols, rows } => {
                self.server_cols = cols;
                self.server_rows = rows;
                if !self.fits_client() {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                // No repaint data accompanies a Resize; just rebuild the tracking
                // terminal to the new server size. The server sends a Refresh
                // shortly after, which re-emits setup and repaints.
                self.inj.reset(cols, rows)?;
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, cols: u32, rows: u32, _out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !super::server_fits_client(self.server_cols, self.server_rows, cols, rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        // The client width/height changed but the transformed stream is
        // width-agnostic for any client >= server, so there is nothing to
        // re-emit; the existing region setup and injected breaks remain valid.
        Ok(super::EventResult::Continue)
    }

    fn cleanup(&mut self, out: &mut Vec<u8>) {
        // Release the vertical scroll region on detach.
        out.extend_from_slice(b"\x1b[r");
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --lib attach::autowrap`
Expected: PASS (all autowrap tests).

- [ ] **Step 5: Full build and test run**

Run: `cargo build && cargo test`
Expected: workspace builds; all tests pass (including region/cell tests unaffected by the `upgrade_to` refactor).

- [ ] **Step 6: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "attach/autowrap: wire WrapInjector into handler with cell fallback

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Notes for the implementer

- **Known limitations (by design, not bugs):** an app that itself enables `?69h`+DECSLRM can double-wrap against our injection (rare; TODO); multi-client query-response routing is deferred; one FFI cursor query per glyph is chatty (the upstream wrap-hook is the future optimization). See `docs/superpowers/specs/2026-06-23-autowrap-render-mode-design.md`.
- **Manual smoke test (optional, after Task 5):** run the server and attach a client wider than the server PTY with `--render-mode autowrap`, then run something that wraps long lines (e.g. `yes "aaaaaaaaaa....(>server width)"` or `seq`/`ls -la` in a wide window) and confirm wrapping happens at the server width with OSC/true-color passthrough intact. Keep the client window at least as large as the server during testing (per the brainstorm: narrower-client handling is cell mode's domain).
- If `cargo test --lib attach::autowrap` reports a wide-char or bottom-scroll test asserting a different cursor delta than expected, re-read `examples/ghostty/src/terminal/Terminal.zig` print logic before adjusting — the detector (`y1 > y0 || x1 < x0`) is deliberately position-based to cover both the pending-wrap and the wide-char/spacer-head cases.
```
