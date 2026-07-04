# Autowrap Byte-Feed Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the `vt_write_until_wrap` C entry point from the ghostty/libghostty-rs forks by re-implementing wrap detection in `src/attach/autowrap.rs` with byte-at-a-time FFI over the existing API, and close the known gaps: unbounded buffering of long OSC/APC payloads (gap 1), wrong injection column under app left margins (gap 2), app DECSTBM/RIS/DECSTR clobbering the region setup (gap 3), missing screen clear on init/refresh (gap 4), and dropped carry bytes on close/mode-switch (gap 6).

**Architecture:** `WrapInjector` feeds the tracking terminal one byte at a time via `vt_write`, using `vt_at_boundary()` to segment the stream into units (one codepoint or one complete escape sequence). Printable units are held (≤4 bytes) until complete so a break can be injected before them; control units are held only up to 64 bytes (for CSI rewriting) and streamed eagerly past that, so OSC/APC payloads flow through unbuffered. Wrap detection compares cursor position (via `cursor_x()`/`cursor_y()`) across printable units; the injected break is `\r\n` plus a CHA when the wrapped glyph landed on a left margin > column 1. A small control-rewrite pass (mirroring `region.rs`) clamps app DECSTBM and re-establishes the region after RIS/DECSTR.

**Tech Stack:** Rust (term repo), libghostty-vt path deps at `examples/libghostty-rs`, ghostty Zig fork at `examples/ghostty`, `unicode-width` crate (new dep).

## Global Constraints

- `Cargo.toml` keeps the local `path = "examples/libghostty-rs/..."` deps (commit 2886a34's intent) — do not repoint at git revs.
- No changes to `examples/ghostty` other than dropping commit `ce1b400` (its branch tip on `autowrap-wrap-hook`).
- `examples/libghostty-rs` ends with commit `58cfd3d` dropped and a new bindings-drift-only regen commit.
- `cursor_x()`/`cursor_y()` are 0-based ("inner-indexed") and return `anyhow`-compatible `Result<u16>`.
- All existing `cargo test` suites in the term repo must pass at every commit **except** the term repo will not build against the trimmed libghostty-rs until Task 1 lands — hence upstream cleanup is the *last* task.
- Commits in each repo end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Rewrite `WrapInjector` as a byte-feed unit segmenter (gap 1 included)

**Files:**
- Modify: `src/attach/autowrap.rs` (replace `WrapInjector` internals and its tests; keep `AutowrapHandler` shape)
- Modify: `Cargo.toml` (add `unicode-width = "0.2"`)

**Interfaces:**
- Consumes: `libghostty_vt::Terminal::{vt_write, vt_at_boundary, cursor_x, cursor_y, resize}` (already in the wrapper — no upstream changes).
- Produces: `WrapInjector::process(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()>` (now fallible), `WrapInjector::flush(&mut self, out: &mut Vec<u8>)` (used by Task 4), `fn rewrite_control(&self, cx: u16, cy: u16, out: &mut Vec<u8>)` stub that Task 3 fills in (in this task it just passes the unit through), `unit_width(unit: &[u8]) -> u16`.

- [ ] **Step 1: Add the dependency**

In `Cargo.toml` under `[dependencies]` (near `hostname = "0.4"`):

```toml
unicode-width = "0.2"
```

- [ ] **Step 2: Write the failing tests**

Replace the `tests` module's `run_bytes` helper and add gap-1 tests. Keep all existing test functions (they remain valid black-box specs) but adapt to the fallible `process`:

```rust
    fn run_bytes(server_cols: u32, server_rows: u32, chunks: &[&[u8]]) -> Vec<u8> {
        let mut wi = WrapInjector::new(server_cols, server_rows).unwrap();
        let mut out = Vec::new();
        for c in chunks {
            wi.process(c, &mut out).unwrap();
        }
        out
    }

    #[test]
    fn long_osc_streams_eagerly() {
        // An OSC payload longer than the control hold cap must be emitted
        // before its terminator arrives — no unbounded buffering (gap 1).
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        let mut osc = b"\x1b]52;c;".to_vec();
        osc.extend(std::iter::repeat(b'A').take(4096)); // no ST yet
        wi.process(&osc, &mut out).unwrap();
        assert!(out.len() >= 4000, "payload must stream out before the terminator, got {} bytes", out.len());
        // Terminator arrives later; everything matches the original stream.
        wi.process(b"\x1b\\", &mut out).unwrap();
        osc.extend_from_slice(b"\x1b\\");
        assert_eq!(out, osc);
    }

    #[test]
    fn carry_is_bounded() {
        // Only an incomplete printable unit (max 4 bytes of UTF-8) or a short
        // control prefix may be held across calls.
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        wi.process(b"ab\xf0\x9f\x98", &mut out).unwrap(); // incomplete 4-byte emoji
        assert_eq!(out, b"ab");
        wi.process(b"\x80", &mut out).unwrap(); // 😀 completes
        assert_eq!(out, "ab😀".as_bytes());
    }
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib attach::autowrap 2>&1 | tail -20`
Expected: compile errors (`process` signature, missing helpers) — that counts as failing.

- [ ] **Step 4: Replace the `WrapInjector` implementation**

Replace the struct and its impl (keep `AutowrapHandler` and the trait impl, adjusting `process` call sites with `?`):

```rust
use std::io::Write as _;

use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions};

/// Control units are held only this long (enough for any CSI we rewrite);
/// longer control strings (OSC/DCS/APC payloads) stream out eagerly.
const CONTROL_HOLD_MAX: usize = 64;

#[derive(Clone, Copy, PartialEq)]
enum UnitKind {
    Printable,
    Control { streamed: bool },
}

pub(super) struct WrapInjector {
    term: Terminal<'static, 'static>,
    /// Bytes of the in-progress unit (a codepoint or an escape sequence).
    /// Printable units are held whole so a break can be injected before them;
    /// control units are held only up to CONTROL_HOLD_MAX for CSI rewriting.
    unit: Vec<u8>,
    kind: Option<UnitKind>,
    prev_x: u16,
    prev_y: u16,
    server_rows: u32,
}

impl WrapInjector {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self {
            term: Terminal::new(TerminalOptions {
                cols: server_cols as u16,
                rows: server_rows as u16,
                max_scrollback: 0,
            })?,
            unit: Vec::new(),
            kind: None,
            prev_x: 0,
            prev_y: 0,
            server_rows,
        })
    }

    /// Tell the tracking terminal its dimensions changed, preserving cursor and
    /// screen state (unlike `reset`). Used on a Resize event: Stream bytes that
    /// arrive before the server's follow-up Refresh must still be tracked
    /// against the real cursor position.
    pub(super) fn resize(&mut self, server_cols: u32, server_rows: u32) -> Result<()> {
        self.term.resize(server_cols as u16, server_rows as u16, 0, 0)?;
        self.server_rows = server_rows;
        // The resize may clamp the cursor; rebaseline so the next printable
        // isn't misread as a wrap.
        self.prev_x = self.term.cursor_x()?;
        self.prev_y = self.term.cursor_y()?;
        Ok(())
    }

    pub(super) fn reset(&mut self, server_cols: u32, server_rows: u32) -> Result<()> {
        self.term = Terminal::new(TerminalOptions {
            cols: server_cols as u16,
            rows: server_rows as u16,
            max_scrollback: 0,
        })?;
        self.unit.clear();
        self.kind = None;
        self.prev_x = 0;
        self.prev_y = 0;
        self.server_rows = server_rows;
        Ok(())
    }

    /// Emit the DECSTBM that establishes the vertical scroll region the
    /// client must use: top margin always 1, bottom margin the server rows.
    pub(super) fn emit_region_setup(&self, out: &mut Vec<u8>) {
        write!(out, "\x1b[1;{}r", self.server_rows).ok();
    }

    /// Emit any held unit bytes verbatim. Only valid at end-of-life (Closed or
    /// cleanup): after a mid-unit flush the classifier and the parser disagree,
    /// so `process` must not be called again.
    pub(super) fn flush(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.unit);
        self.unit.clear();
        self.kind = None;
    }

    pub(super) fn process(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        for &b in input {
            let kind = *self.kind.get_or_insert(if b >= 0x20 && b != 0x7f {
                UnitKind::Printable
            } else {
                UnitKind::Control { streamed: false }
            });
            self.term.vt_write(&[b]);
            match kind {
                UnitKind::Printable => self.unit.push(b),
                UnitKind::Control { streamed: false } => {
                    self.unit.push(b);
                    if self.unit.len() > CONTROL_HOLD_MAX {
                        // Too long to be a CSI we rewrite: stream from here on.
                        out.extend_from_slice(&self.unit);
                        self.unit.clear();
                        self.kind = Some(UnitKind::Control { streamed: true });
                    }
                }
                UnitKind::Control { streamed: true } => out.push(b),
            }
            if !self.term.vt_at_boundary() {
                continue;
            }
            // A complete unit just landed.
            let cx = self.term.cursor_x()?;
            let cy = self.term.cursor_y()?;
            match self.kind.take().expect("kind set above") {
                UnitKind::Printable => {
                    if cy > self.prev_y || cx < self.prev_x {
                        // The glyph soft-wrapped on the server: inject a break,
                        // then move to the column it actually landed on (the
                        // left margin, which is column 1 unless the app set
                        // DECSLRM — see the CHA below).
                        out.extend_from_slice(b"\r\n");
                        let left = cx.saturating_sub(unit_width(&self.unit));
                        if left > 0 {
                            write!(out, "\x1b[{}G", left + 1).ok();
                        }
                    }
                    out.extend_from_slice(&self.unit);
                    self.unit.clear();
                }
                UnitKind::Control { streamed: false } => {
                    self.rewrite_control(cx, cy, out);
                    self.unit.clear();
                }
                UnitKind::Control { streamed: true } => {}
            }
            self.prev_x = cx;
            self.prev_y = cy;
        }
        Ok(())
    }

    /// Pass a completed, held control unit to the client, rewriting the few
    /// sequences that contend with our region setup. Filled in by the
    /// DECSTBM/RIS/DECSTR task; passthrough until then.
    fn rewrite_control(&self, _cx: u16, _cy: u16, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.unit);
    }
}

/// Terminal-column width of the first codepoint of a printable unit.
fn unit_width(unit: &[u8]) -> u16 {
    use unicode_width::UnicodeWidthChar;
    std::str::from_utf8(unit)
        .ok()
        .and_then(|s| s.chars().next())
        .and_then(|c| c.width())
        .unwrap_or(1) as u16
}
```

Then update the trait impl call sites: in `init` and the `Stream`/`Refresh` arms, `self.inj.process(...)` becomes `self.inj.process(...)?;`.

- [ ] **Step 5: Run the autowrap tests**

Run: `cargo test --lib attach::autowrap 2>&1 | tail -20`
Expected: all PASS, including the pre-existing tests (`full_line_then_printable_injects_break`, `wide_char_at_edge_injects_break`, `bottom_margin_scroll_injects_break`, chunk-split tests, etc.) and the two new gap-1 tests. If `escape_sequence_passes_through`-style tests fail on ordering, the bug is real — do not weaken the assertions.

- [ ] **Step 6: Run the full test suite**

Run: `cargo test 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/attach/autowrap.rs
git commit -m "autowrap: byte-feed wrap detection over existing FFI, stream long control payloads"
```

---

### Task 2: Inject the correct column under app left margins (gap 2)

**Files:**
- Modify: `src/attach/autowrap.rs` (tests only — the CHA emission already landed in Task 1's `process`; this task proves it against real DECSLRM)

**Interfaces:**
- Consumes: `WrapInjector::process` from Task 1 (the `\x1b[{}G` branch).
- Produces: nothing new — behavioral verification.

- [ ] **Step 1: Write the failing/verifying tests**

```rust
    #[test]
    fn wrap_with_left_margin_emits_cha() {
        // App enables DECLRMM and sets left margin 2, right margin 4 on an
        // 8-col server. Cursor to row 1 col 2; "abc" fills cols 2..4 (pending
        // wrap at the right margin); "x" wraps to row 2 *column 2*, not column
        // 1. The injected break must be \r\n followed by CHA to column 2.
        let mut wi = WrapInjector::new(8, 4).unwrap();
        let mut out = Vec::new();
        wi.process(b"\x1b[?69h\x1b[2;4s\x1b[1;2Habcx", &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(
            s.ends_with("abc\r\n\x1b[2Gx"),
            "expected break + CHA to the left margin before 'x', got {:?}", s
        );
    }

    #[test]
    fn wrap_without_margin_has_no_cha() {
        // Plain wrap at column 1 must stay a bare \r\n (no redundant CHA).
        assert_eq!(run_bytes(4, 3, &[b"abcde"]), b"abcd\r\ne");
    }

    #[test]
    fn wide_char_wrap_with_left_margin_emits_cha() {
        // Same margins; 世 (2 wide) can't fit in the last column and wraps to
        // the left margin. left = cx_after - 2.
        let mut wi = WrapInjector::new(8, 4).unwrap();
        let mut out = Vec::new();
        wi.process("\x1b[?69h\x1b[2;5s\x1b[1;4H".as_bytes(), &mut out).unwrap();
        wi.process("a世".as_bytes(), &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(
            s.ends_with("a\r\n\x1b[2G世"),
            "expected CHA to left margin before the wide glyph, got {:?}", s
        );
    }
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --lib attach::autowrap 2>&1 | tail -20`
Expected: PASS if ghostty's tracked terminal honors DECSLRM as assumed. If a test fails, diagnose against the tracked terminal's actual cursor values (print `cursor_x` in the test) before touching `process` — the fix belongs in the `left` computation, not the assertions. Known risk: `unicode_width` disagreeing with ghostty on ambiguous-width chars; acceptable for now, documented in the code comment.

- [ ] **Step 3: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "autowrap: verify injected break lands on the app's left margin"
```

---

### Task 3: Rewrite app DECSTBM / RIS / DECSTR (gap 3)

**Files:**
- Modify: `src/attach/autowrap.rs` (fill in `rewrite_control`, add tests)

**Interfaces:**
- Consumes: `rewrite_control(&self, cx: u16, cy: u16, out: &mut Vec<u8>)` stub and `emit_region_setup` from Task 1.
- Produces: final `rewrite_control` behavior relied on by the handler (no signature change).

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn app_decstbm_bottom_clamped_to_server_rows() {
        // App asks for rows 1..50 on a 24-row server: clamp bottom to 24.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[1;50r"]), b"\x1b[1;24r");
    }

    #[test]
    fn app_decstbm_full_reset_becomes_server_region() {
        // Bare ESC[r means "full screen" to the app — which is the *server*
        // screen, not the taller client. Rewrite to 1..server_rows.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[r"]), b"\x1b[1;24r");
    }

    #[test]
    fn app_decstbm_within_bounds_passes() {
        assert_eq!(run_bytes(80, 24, &[b"\x1b[5;20r"]), b"\x1b[5;20r");
    }

    #[test]
    fn private_csi_r_not_rewritten() {
        // ESC[?...r is DEC private mode *restore*, not DECSTBM.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[?1049r"]), b"\x1b[?1049r");
    }

    #[test]
    fn ris_reestablishes_region() {
        let out = run_bytes(80, 24, &[b"\x1bc"]);
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1bc"), "RIS passes through first");
        assert!(s.contains("\x1b[1;24r"), "region setup must follow RIS");
    }

    #[test]
    fn decstr_reestablishes_region_and_restores_cursor() {
        // DECSTR resets margins but does NOT move the cursor; our re-emitted
        // DECSTBM homes it, so a CUP back to the tracked position must follow.
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        wi.process(b"\x1b[5;10H\x1b[!p", &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\x1b[!p"), "DECSTR passes through");
        assert!(s.contains("\x1b[1;24r"), "region setup follows DECSTR");
        assert!(s.ends_with("\x1b[5;10H"), "cursor restored after region setup, got {:?}", s);
    }
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `cargo test --lib attach::autowrap 2>&1 | tail -25`
Expected: the six new tests FAIL (passthrough stub); everything else PASSES.

- [ ] **Step 3: Implement `rewrite_control`**

```rust
    /// Pass a completed, held control unit to the client, rewriting the few
    /// sequences that contend with our region setup. Mirrors region.rs's
    /// dispatch_csi, minus the DECSLRM/DECLRMM handling autowrap doesn't need.
    fn rewrite_control(&self, cx: u16, cy: u16, out: &mut Vec<u8>) {
        let u = &self.unit;
        if u.as_slice() == b"\x1bc" {
            // RIS: pass through (clears + homes the client), then re-establish
            // the region. DECSTBM homes again, matching RIS's own homing.
            out.extend_from_slice(u);
            self.emit_region_setup(out);
            return;
        }
        if u.starts_with(b"\x1b[") && u.len() >= 3 {
            let body = &u[2..u.len() - 1];
            let fin = *u.last().expect("unit non-empty");
            if fin == b'p' && body.first() == Some(&b'!') {
                // DECSTR: resets margins without moving the cursor. Re-emit the
                // region (which homes the cursor) and put the cursor back where
                // the tracked terminal says it is.
                out.extend_from_slice(u);
                self.emit_region_setup(out);
                write!(out, "\x1b[{};{}H", cy + 1, cx + 1).ok();
                return;
            }
            if fin == b'r' && body.iter().all(|&b| b.is_ascii_digit() || b == b';') {
                // DECSTBM: the app's "full screen" is the server screen. Clamp
                // the bottom margin so it never spills into client rows the
                // server doesn't have.
                let mut params = body
                    .split(|&b| b == b';')
                    .map(|s| std::str::from_utf8(s).ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0));
                let top = params.next().filter(|&p| p != 0).unwrap_or(1);
                let bottom = params.next().unwrap_or(0);
                let bottom = if bottom == 0 || bottom > self.server_rows {
                    self.server_rows
                } else {
                    bottom
                };
                let top = if top >= bottom { 1 } else { top };
                write!(out, "\x1b[{};{}r", top, bottom).ok();
                return;
            }
        }
        out.extend_from_slice(u);
    }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib attach::autowrap 2>&1 | tail -25`
Expected: all PASS. Note `\x1b[r` alone is 3 bytes (`u.len() >= 3`, empty body) — covered by `app_decstbm_full_reset_becomes_server_region`.

- [ ] **Step 5: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "autowrap: clamp app DECSTBM, re-establish region after RIS/DECSTR"
```

---

### Task 4: Clear on init/refresh; flush held bytes on Closed and cleanup (gaps 4 + 6)

**Files:**
- Modify: `src/attach/autowrap.rs` (`AutowrapHandler` trait impl, tests)

**Interfaces:**
- Consumes: `WrapInjector::flush` from Task 1.
- Produces: final handler behavior; no signature changes.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn closed_flushes_held_bytes() {
        // A partial CSI held for rewriting must not be swallowed when the PTY
        // closes mid-sequence.
        let mut h = AutowrapHandler::new(80, 24).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();
        h.on_pty_event(PtyEvent::Stream { data: b"\x1b[5;10", gen: 0 }, &mut out).unwrap();
        assert_eq!(out, b"", "partial CSI must be held");
        h.on_pty_event(PtyEvent::Closed, &mut out).unwrap();
        assert_eq!(out, b"\x1b[5;10", "Closed must flush the held bytes");
    }

    #[test]
    fn cleanup_flushes_then_releases_region() {
        let mut h = AutowrapHandler::new(80, 24).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();
        h.on_pty_event(PtyEvent::Stream { data: b"ab\xc3", gen: 0 }, &mut out).unwrap(); // partial é
        out.clear();
        h.cleanup(&mut out);
        assert_eq!(out, b"\xc3\x1b[r", "cleanup must flush the carry before releasing the region");
    }

    #[test]
    fn init_clears_screen_after_region_setup() {
        let mut h = AutowrapHandler::new(80, 24).unwrap();
        let mut out = Vec::new();
        h.init(b"hello", &[], &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(
            s.starts_with("\x1b[1;24r\x1b[2J"),
            "init must set the region then clear stale client content, got {:?}", s
        );
        assert!(s.ends_with("hello"));
    }
```

Note: check `PtyEvent::Stream`'s actual field names in `src/attach/mod.rs` (the variant carries data plus a generation field) and match them in the tests.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib attach::autowrap 2>&1 | tail -25`
Expected: the three new tests FAIL.

- [ ] **Step 3: Implement in the trait impl**

In `init`, after `self.inj.emit_region_setup(out);` add:

```rust
        // DECSTBM homed the cursor; clear any stale client content (from a
        // previous render mode, or client columns/rows beyond the server's)
        // before replaying the repaint.
        out.extend_from_slice(b"\x1b[2J");
```

Do the same in the `Refresh` arm after its `emit_region_setup`.

Replace the `Closed` arm:

```rust
            super::PtyEvent::Closed => {
                // The stream is over; a partial unit held for wrap/rewrite
                // inspection will never complete. Emit it verbatim.
                self.inj.flush(out);
            }
```

Replace `cleanup`:

```rust
    fn cleanup(&mut self, out: &mut Vec<u8>) {
        // Flush any held partial unit, then release the vertical scroll region.
        self.inj.flush(out);
        out.extend_from_slice(b"\x1b[r");
    }
```

- [ ] **Step 4: Run the full suite**

Run: `cargo test 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "autowrap: clear on init/refresh, flush held bytes on close and cleanup"
```

---

### Task 5: Drop `vt_write_until_wrap` from both upstream forks

**Files:**
- Modify: `examples/ghostty` (git only: drop commit `ce1b400`)
- Modify: `examples/libghostty-rs` (git: drop commit `58cfd3d`; then regenerate `crates/libghostty-vt-sys/src/bindings.rs` for the pre-existing API drift only)
- Verify: term repo builds and tests green against the trimmed forks

**Interfaces:**
- Consumes: Task 1–4's autowrap.rs, which no longer references `vt_write_until_wrap` or `WrapWrite`.
- Produces: clean upstream branches; a drift-only bindings regen commit in libghostty-rs.

- [ ] **Step 1: Confirm term no longer references the removed API**

Run: `grep -rn "vt_write_until_wrap\|WrapWrite" /home/jsanford/term/src`
Expected: no output. If there are hits, fix them before touching the upstreams.

- [ ] **Step 2: Check whether the commits are pushed**

Run:
```bash
git -C /home/jsanford/term/examples/ghostty log --oneline @{upstream}..HEAD 2>/dev/null || git -C /home/jsanford/term/examples/ghostty branch -vv | head -3
git -C /home/jsanford/term/examples/libghostty-rs log --oneline @{upstream}..HEAD 2>/dev/null || git -C /home/jsanford/term/examples/libghostty-rs branch -vv | head -3
```
If either tip commit is already on its remote branch, STOP and ask the user before force-pushing; otherwise proceed (a plain reset suffices).

- [ ] **Step 3: Drop the commits**

```bash
git -C /home/jsanford/term/examples/ghostty reset --hard HEAD~1   # drops ce1b400
git -C /home/jsanford/term/examples/libghostty-rs reset --hard HEAD~1   # drops 58cfd3d
```

Verify: `git -C ... log --oneline -1` shows `42a80a9` (ghostty) and `0bc6029` (libghostty-rs).

- [ ] **Step 4: Regenerate bindings for the API drift only**

The dropped libghostty-rs commit bundled a legitimate fix: `bindings.rs` predated C API drift (selection-gesture etc.). Recreate that fix without the wrap API:

```bash
cd /home/jsanford/term/examples/libghostty-rs
cargo build -p libghostty-vt-sys
cargo run --bin gen-bindings
git diff --stat crates/libghostty-vt-sys/src/bindings.rs
```

Expected: a diff similar to the drift portion of the old commit, with **no** `vt_write_until_wrap` / `WriteUntilWrapResult` symbols:

```bash
grep -c "write_until_wrap" crates/libghostty-vt-sys/src/bindings.rs   # expected: 0 (grep exits 1)
```

If `cargo build -p libghostty-vt-sys` rebuilds ghostty from a vendored source rather than `examples/ghostty`, set `GHOSTTY_SOURCE_DIR=/home/jsanford/term/examples/ghostty` (requires `zig build` having populated `zig-out/include` there — run `zig build` in examples/ghostty if needed) and re-run gen-bindings.

- [ ] **Step 5: Test libghostty-rs and commit**

```bash
cd /home/jsanford/term/examples/libghostty-rs
cargo test 2>&1 | tail -5
git add crates/libghostty-vt-sys/src/bindings.rs
git commit -m "bindings: regenerate from current ghostty headers (API drift)"
```

Expected: tests PASS (the `vt_write_until_wrap_reports_wrap` test went away with the reset).

- [ ] **Step 6: Full verification in term**

```bash
cd /home/jsanford/term
cargo build 2>&1 | tail -3
cargo test 2>&1 | tail -5
```

Expected: clean build (path deps now point at the trimmed forks) and all tests PASS.

- [ ] **Step 7: Commit any term-side fallout**

If `Cargo.lock` changed: `git add Cargo.lock && git commit -m "build: lockfile refresh after upstream trim"`. If nothing changed, no commit.

---

## Verification checklist (end of plan)

- `grep -rn "vt_write_until_wrap" /home/jsanford/term/src /home/jsanford/term/examples/ghostty/src /home/jsanford/term/examples/libghostty-rs/crates` → no hits.
- `cargo test` green in term and in examples/libghostty-rs.
- `git -C examples/ghostty status` clean; branch tip is `42a80a9`.
- Autowrap unit tests cover: passthrough, wrap injection (plain / wide / bottom-scroll / chunk-split), gap 1 (eager OSC streaming, bounded carry), gap 2 (CHA under left margin), gap 3 (DECSTBM clamp, RIS, DECSTR), gaps 4+6 (init clear, Closed/cleanup flush).
