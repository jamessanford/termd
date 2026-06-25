# autowrap wrap-hook (`vt_write_until_wrap`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an upstream `vt_write_until_wrap` entry point to the libghostty fork that bulk-consumes bytes but stops at the first soft-wrap and reports where to splice a line break, then rewire `render-mode=autowrap` to use it — removing the per-glyph cursor polling and the byte-classifying state machine.

**Architecture:** A new Zig C-API function drives the existing per-byte VT stream, tracking the cursor across each printable glyph and halting at the first wrap (consume-through: it prints the wrapping glyph, then reports the glyph's start offset). A `skip` argument names an already-fed carry prefix so split units are not re-fed. The function is exported through the existing header → bindgen → safe-wrapper chain. The autowrap proxy then feeds in bulk, carries the uncommitted partial-unit tail, and splices `\r\n` at reported offsets.

**Tech Stack:** Zig 0.15.2 (libghostty fork at `examples/ghostty`), rust-bindgen 0.72.1, Rust (`libghostty-vt`, `termd` `src/attach`), `anyhow`.

## Global Constraints

- Do **not** modify the existing `vt_write` / `ghostty_terminal_vt_write` API; add the new function alongside it (keeps the fork diff easy to track upstream).
- Do **not** modify `render-mode=cell` or `render-mode=region`.
- The build uses the local ghostty checkout at `examples/ghostty` (its HEAD already matches the pinned fork commit `42a80a917c0e9615190fb0a64f7192e116bd7319`); Zig changes there are picked up by `cargo build`.
- Wrap detection is position-based: a wrap occurred when, after a **printable** glyph completes, the cursor row increased **or** the cursor column decreased. C0 controls (including `LF`) and escape sequences never signal a wrap.
- Consume-through semantics: on a wrap the function prints the wrapping glyph and reports `committed` = boundary just past it, `wrap_offset` = the glyph's start.
- Offsets (`committed`, `wrap_offset`) are in the full-buffer (`ptr[0..len]`) frame; `wrap_offset` may be `< skip`.
- The carry buffer always begins at a true unit boundary, so its first byte (`input[0]`) is a genuine lead byte usable for printable-vs-control classification.
- Inject the literal two bytes `b"\r\n"` at a wrap point.
- Drop in-proxy DECSTBM clamping (the four `app_decstbm_*` tests and `clamp_decstbm` are removed).

---

### Task 1: Zig `vt_write_until_wrap` — function, export wiring, and Zig unit tests

Adds the new C-API function and its result struct to the Zig shim, wires it through the two export indirections, and proves behavior with Zig `test` blocks. No Rust changes yet; the symbol is only reachable from Zig tests at this point.

**Files:**
- Modify: `examples/ghostty/src/terminal/c/terminal.zig` (add struct + fn near `vt_write`/`vt_at_boundary` around line 296–316; add tests at the end with the other `test "vt_write ..."` blocks)
- Modify: `examples/ghostty/src/terminal/c/main.zig:168` (add the `terminal_vt_write_until_wrap` alias next to `terminal_vt_write`)
- Modify: `examples/ghostty/src/lib_vt.zig:238` (add the `@export` next to `terminal_vt_write`)

**Interfaces:**
- Consumes: `wrapper.stream.next(u8)`, `wrapper.stream.parser.state`, `wrapper.stream.utf8decoder.state`, `wrapper.terminal.screens.active.cursor.x` / `.y` (same access path as the `cursor_x`/`cursor_y` getters at `c/terminal.zig:701-702`), `lib.calling_conv`, `Terminal = ?*TerminalWrapper`.
- Produces (exported C symbol `ghostty_terminal_vt_write_until_wrap`):
  - `WriteUntilWrapResult` extern struct `{ committed: usize, wrapped: bool, wrap_offset: usize }`
  - `vt_write_until_wrap(t: Terminal, ptr: [*]const u8, len: usize, skip: usize, result: *WriteUntilWrapResult) void`

- [ ] **Step 1: Write the failing Zig tests**

Add these at the end of `examples/ghostty/src/terminal/c/terminal.zig`, following the existing `test "vt_write"` scaffolding (a `var t: Terminal = null;` created via `new(...)`, `defer free(t);`). Use a 4×3 grid so wraps are easy to hit. Helper inline:

```zig
test "vt_write_until_wrap: no wrap consumes all" {
    var t: Terminal = null;
    try testing.expectEqual(Result.success, new(
        &lib.alloc.test_allocator,
        &t,
        .{ .cols = 4, .rows = 3, .max_scrollback = 0 },
    ));
    defer free(t);

    var r: WriteUntilWrapResult = undefined;
    vt_write_until_wrap(t, "abc", 3, 0, &r);
    try testing.expect(!r.wrapped);
    try testing.expectEqual(@as(usize, 3), r.committed);
}

test "vt_write_until_wrap: deferred wrap reports glyph start" {
    var t: Terminal = null;
    try testing.expectEqual(Result.success, new(
        &lib.alloc.test_allocator,
        &t,
        .{ .cols = 4, .rows = 3, .max_scrollback = 0 },
    ));
    defer free(t);

    var r: WriteUntilWrapResult = undefined;
    // "abcd" fills the row (pending wrap); "e" wraps.
    vt_write_until_wrap(t, "abcde", 5, 0, &r);
    try testing.expect(r.wrapped);
    try testing.expectEqual(@as(usize, 4), r.wrap_offset); // start of "e"
    try testing.expectEqual(@as(usize, 5), r.committed);   // through "e"
}

test "vt_write_until_wrap: LF does not signal a wrap" {
    var t: Terminal = null;
    try testing.expectEqual(Result.success, new(
        &lib.alloc.test_allocator,
        &t,
        .{ .cols = 4, .rows = 3, .max_scrollback = 0 },
    ));
    defer free(t);

    var r: WriteUntilWrapResult = undefined;
    vt_write_until_wrap(t, "ab\ncd", 5, 0, &r);
    try testing.expect(!r.wrapped);
    try testing.expectEqual(@as(usize, 5), r.committed);
}

test "vt_write_until_wrap: wide char at edge wraps" {
    var t: Terminal = null;
    try testing.expectEqual(Result.success, new(
        &lib.alloc.test_allocator,
        &t,
        .{ .cols = 4, .rows = 3, .max_scrollback = 0 },
    ));
    defer free(t);

    var r: WriteUntilWrapResult = undefined;
    // "abc" leaves one column; the 2-wide glyph cannot fit and wraps.
    const in = "abc\u{4e16}"; // 世 = E4 B8 96
    vt_write_until_wrap(t, in.ptr, in.len, 0, &r);
    try testing.expect(r.wrapped);
    try testing.expectEqual(@as(usize, 3), r.wrap_offset); // start of 世
    try testing.expectEqual(in.len, r.committed);
}

test "vt_write_until_wrap: skip prefix not re-fed, wrap_offset in carry" {
    var t: Terminal = null;
    try testing.expectEqual(Result.success, new(
        &lib.alloc.test_allocator,
        &t,
        .{ .cols = 4, .rows = 3, .max_scrollback = 0 },
    ));
    defer free(t);

    // First call: feed "abc" then the first byte of 世 (E4); the glyph is
    // incomplete, so no wrap and committed stops before E4.
    var r1: WriteUntilWrapResult = undefined;
    const buf1 = "abc\xe4";
    vt_write_until_wrap(t, buf1.ptr, buf1.len, 0, &r1);
    try testing.expect(!r1.wrapped);
    try testing.expectEqual(@as(usize, 3), r1.committed); // E4 uncommitted

    // Second call: carry "\xe4" (already fed) + remaining bytes. skip=1 means
    // E4 is not re-fed; the completed wide glyph wraps and the break belongs
    // before the carry (wrap_offset = 0 < skip).
    var r2: WriteUntilWrapResult = undefined;
    const buf2 = "\xe4\xb8\x96"; // E4 B8 96 = 世
    vt_write_until_wrap(t, buf2.ptr, buf2.len, 1, &r2);
    try testing.expect(r2.wrapped);
    try testing.expectEqual(@as(usize, 0), r2.wrap_offset);
    try testing.expectEqual(@as(usize, 3), r2.committed);
    // Cursor advanced by exactly the 2-wide glyph (col 2 on the wrapped row),
    // proving E4 was not double-fed.
    try testing.expectEqual(@as(size.CellCountInt, 2), t.?.terminal.screens.active.cursor.x);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd examples/ghostty && zig build test-lib-vt -Dtest-filter="vt_write_until_wrap"`
Expected: FAIL — compile error, `vt_write_until_wrap` / `WriteUntilWrapResult` are undefined.

- [ ] **Step 3: Implement the function and struct**

In `examples/ghostty/src/terminal/c/terminal.zig`, immediately after `vt_at_boundary` (around line 316), add:

```zig
/// C: GhosttyTerminalWriteUntilWrapResult
pub const WriteUntilWrapResult = extern struct {
    /// Offset (within ptr[0..len]) of the last parser ground boundary reached;
    /// bytes [0..committed] are complete and safe to emit, and this is the
    /// caller's resume point. When `wrapped`, the boundary just past the
    /// wrapping glyph. When not `wrapped`, bytes [committed..len] are a partial
    /// trailing unit the caller must carry.
    committed: usize,
    /// Whether processing stopped at a soft-wrap.
    wrapped: bool,
    /// If `wrapped`, the byte offset (within ptr[0..len]) of the first byte of
    /// the glyph that wrapped — where the caller inserts the line break. May be
    /// < skip when that glyph began in the already-fed prefix. Unspecified
    /// when `wrapped` is false.
    wrap_offset: usize,
};

/// Feed `ptr[skip..len]`, stopping at the first soft-wrap. `ptr[0..skip]` was
/// already fed on a previous call (the caller's carried partial-unit tail) and
/// is NOT re-fed; the full buffer exists only to anchor `committed`/`wrap_offset`
/// in one offset frame spanning the carry. Callers with no carry pass skip=0.
pub fn vt_write_until_wrap(
    terminal_: Terminal,
    ptr: [*]const u8,
    len: usize,
    skip: usize,
    result: *WriteUntilWrapResult,
) callconv(lib.calling_conv) void {
    const wrapper = terminal_ orelse {
        result.* = .{ .committed = 0, .wrapped = false, .wrap_offset = 0 };
        return;
    };
    const input = ptr[0..len];

    // The carry (ptr[0..skip]) always begins at a true unit boundary. If the
    // parser is mid-unit at entry, that unit began at offset 0 (its real lead
    // byte is input[0]); otherwise the next unit begins at `skip`.
    const at_boundary = wrapper.stream.parser.state == .ground and
        wrapper.stream.utf8decoder.state == 0;
    const base: usize = if (at_boundary) skip else 0;

    var unit_start: usize = base;
    var committed: usize = base;
    var prev_x = wrapper.terminal.screens.active.cursor.x;
    var prev_y = wrapper.terminal.screens.active.cursor.y;

    var offset: usize = skip;
    while (offset < len) {
        wrapper.stream.next(input[offset]);
        offset += 1;

        const now_ground = wrapper.stream.parser.state == .ground and
            wrapper.stream.utf8decoder.state == 0;
        if (!now_ground) continue;

        // A complete unit just landed: [unit_start, offset).
        committed = offset;
        const start_byte = input[unit_start];
        const is_printable = start_byte >= 0x20 and start_byte != 0x7f;
        const cx = wrapper.terminal.screens.active.cursor.x;
        const cy = wrapper.terminal.screens.active.cursor.y;
        if (is_printable and (cy > prev_y or cx < prev_x)) {
            result.* = .{ .committed = offset, .wrapped = true, .wrap_offset = unit_start };
            return;
        }
        // Advance the baseline (printable that did not wrap, or a control/escape
        // that may have moved the cursor) and start the next unit.
        prev_x = cx;
        prev_y = cy;
        unit_start = offset;
    }

    result.* = .{ .committed = committed, .wrapped = false, .wrap_offset = 0 };
}
```

In `examples/ghostty/src/terminal/c/main.zig`, after line 168 (`pub const terminal_vt_write = terminal.vt_write;`) add:

```zig
pub const terminal_vt_write_until_wrap = terminal.vt_write_until_wrap;
```

In `examples/ghostty/src/lib_vt.zig`, after line 238 (`@export(&c.terminal_vt_write, ...)`) add:

```zig
        @export(&c.terminal_vt_write_until_wrap, .{ .name = "ghostty_terminal_vt_write_until_wrap" });
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd examples/ghostty && zig build test-lib-vt -Dtest-filter="vt_write_until_wrap"`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add examples/ghostty/src/terminal/c/terminal.zig examples/ghostty/src/terminal/c/main.zig examples/ghostty/src/lib_vt.zig
git commit -m "libghostty/vt: add vt_write_until_wrap C entry point

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Header declaration, regenerated bindings, and the Rust safe wrapper

Declares the new function/struct in the public C header, regenerates the bindgen bindings, and adds the idiomatic Rust wrapper on `Terminal`. After this task the function is callable from Rust.

**Files:**
- Modify: `examples/ghostty/include/ghostty/vt/terminal.h` (add the struct typedef near the other `typedef struct` blocks ~line 164–242, and the function declaration after `ghostty_terminal_vt_at_boundary` ~line 1049)
- Modify: `examples/libghostty-rs/crates/libghostty-vt-sys/src/bindings.rs` (regenerated, not hand-edited)
- Modify: `examples/libghostty-rs/crates/libghostty-vt/src/terminal.rs` (add `WrapWrite` + `vt_write_until_wrap` near `vt_write`/`vt_at_boundary` ~line 300–316; add a unit test)

**Interfaces:**
- Consumes: `ffi::ghostty_terminal_vt_write_until_wrap`, `ffi::WriteUntilWrapResult` (produced by bindgen from the header); `self.inner.as_raw()`.
- Produces:
  - `pub struct WrapWrite { pub committed: usize, pub wrap: Option<usize> }`
  - `Terminal::vt_write_until_wrap(&mut self, buf: &[u8], skip: usize) -> WrapWrite`

- [ ] **Step 1: Add the C header declaration**

In `examples/ghostty/include/ghostty/vt/terminal.h`, near the other `typedef struct` blocks (around lines 164–242) add:

```c
/**
 * Result of ghostty_terminal_vt_write_until_wrap().
 */
typedef struct {
  /** Offset up to which bytes were consumed (complete units, safe to emit);
   *  also the resume point. When wrapped, the boundary just past the wrapping
   *  glyph. When not wrapped, [committed, len) is a partial trailing unit the
   *  caller must carry. */
  size_t committed;
  /** Whether processing stopped at a soft-wrap. */
  bool wrapped;
  /** If wrapped, the byte offset of the first byte of the glyph that wrapped
   *  (where the caller inserts a line break). May be < skip. Unspecified when
   *  wrapped is false. */
  size_t wrap_offset;
} GhosttyTerminalWriteUntilWrapResult;
```

After the `ghostty_terminal_vt_at_boundary` declaration (~line 1049) add:

```c
/**
 * Feed bytes to the terminal, stopping at the first soft-wrap.
 *
 * Consumes data[skip..len], stopping immediately after the glyph that causes
 * the first soft-wrap (the wrapping glyph IS printed). data[0..skip] was fed on
 * a previous call (the caller's carried partial-unit tail) and is NOT re-fed;
 * the full buffer only anchors the returned offsets in one frame spanning the
 * carry. Callers with no carry pass skip=0. Results are written to *result.
 *
 * @param terminal The terminal handle (NULL writes a zeroed result)
 * @param data Pointer to the data
 * @param len Length of the data in bytes
 * @param skip Length of the already-fed prefix not to re-feed
 * @param result Out-pointer for the result
 *
 * @ingroup terminal
 */
GHOSTTY_API void ghostty_terminal_vt_write_until_wrap(GhosttyTerminal terminal,
                                const uint8_t* data,
                                size_t len,
                                size_t skip,
                                GhosttyTerminalWriteUntilWrapResult* result);
```

- [ ] **Step 2: Regenerate the bindings**

The bindgen tool reads the header from the sys crate's build output, so build first, then regenerate:

Run:
```
cd examples/libghostty-rs && cargo build -p libghostty-vt-sys && cargo run -p libghostty-vt-sys --bin gen-bindings --features bindgen-tool
```
Expected: `crates/libghostty-vt-sys/src/bindings.rs` is rewritten; `git diff` shows a new `pub fn ghostty_terminal_vt_write_until_wrap(...)` and a `WriteUntilWrapResult` (or `GhosttyTerminalWriteUntilWrapResult`) struct. Note the exact generated Rust type name for use in Step 4.

- [ ] **Step 3: Write the failing Rust wrapper test**

In `examples/libghostty-rs/crates/libghostty-vt/src/terminal.rs`, add to the existing `#[cfg(test)] mod tests` (or create one mirroring the file's test style) a test driving a 4×3 terminal:

```rust
#[test]
fn vt_write_until_wrap_reports_wrap() {
    let mut t = Terminal::new(TerminalOptions { cols: 4, rows: 3, max_scrollback: 0 }).unwrap();
    // No wrap: whole input committed.
    let r = t.vt_write_until_wrap(b"abc", 0);
    assert_eq!(r.committed, 3);
    assert_eq!(r.wrap, None);
    // "d" fills the row (pending), "e" wraps; break before "e" at offset 4.
    let r = t.vt_write_until_wrap(b"de", 0);
    assert_eq!(r.wrap, Some(1)); // "e" is at offset 1 within "de"
    assert_eq!(r.committed, 2);
}
```

(`TerminalOptions` and `Terminal` are already in scope in this module; match the construction used by other tests in the file.)

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd examples/libghostty-rs && cargo test -p libghostty-vt vt_write_until_wrap`
Expected: FAIL — `vt_write_until_wrap` / `WrapWrite` not found.

- [ ] **Step 5: Implement the safe wrapper**

In `examples/libghostty-rs/crates/libghostty-vt/src/terminal.rs`, immediately after `vt_at_boundary` (~line 316), add (use the exact generated struct name from Step 2 in place of `ffi::WriteUntilWrapResult` if bindgen named it differently):

```rust
/// Outcome of one [`Terminal::vt_write_until_wrap`] call. Offsets are in the
/// `buf` frame passed to that call.
pub struct WrapWrite {
    /// Offset up to which `buf` formed complete units (safe to emit) and the
    /// resume point. `buf[committed..]` is a partial trailing unit to carry.
    pub committed: usize,
    /// `Some(offset)` if a soft-wrap was hit: the offset within `buf` at which
    /// to insert a line break (start of the wrapping glyph). May be `< skip`.
    /// `None` if no wrap occurred in this call.
    pub wrap: Option<usize>,
}

impl Terminal<'_, '_> {
    /// Feed `buf[skip..]`, stopping at the first soft-wrap. The prefix
    /// `buf[0..skip]` was fed on a prior call and is not re-fed; it only anchors
    /// the returned offset frame so a glyph spanning the carry reports a
    /// `wrap` offset `< skip`. Pass `skip = 0` when there is no carry.
    pub fn vt_write_until_wrap(&mut self, buf: &[u8], skip: usize) -> WrapWrite {
        let mut result = ffi::WriteUntilWrapResult { committed: 0, wrapped: false, wrap_offset: 0 };
        unsafe {
            ffi::ghostty_terminal_vt_write_until_wrap(
                self.inner.as_raw(),
                buf.as_ptr(),
                buf.len(),
                skip,
                &mut result,
            );
        }
        WrapWrite {
            committed: result.committed,
            wrap: if result.wrapped { Some(result.wrap_offset) } else { None },
        }
    }
}
```

If `WrapWrite` should be public API, also re-export it where the crate exposes other terminal types (mirror how `SizeReportSize` is surfaced; a `pub struct` in `terminal.rs` within a `pub` module is sufficient if `Terminal` is already reachable there).

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd examples/libghostty-rs && cargo test -p libghostty-vt vt_write_until_wrap`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add examples/ghostty/include/ghostty/vt/terminal.h examples/libghostty-rs/crates/libghostty-vt-sys/src/bindings.rs examples/libghostty-rs/crates/libghostty-vt/src/terminal.rs
git commit -m "libghostty-rs: bind vt_write_until_wrap with safe wrapper

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Rewire the autowrap proxy to bulk-feed with a carry buffer

Replaces `WrapInjector`'s byte-classifying state machine with a bulk loop over `vt_write_until_wrap`, adds the `carry` buffer for split units, removes DECSTBM clamping, and adds the split-glyph regression test. The existing wrap-injection corpus stays byte-for-byte identical as the regression guarantee.

**Files:**
- Modify: `src/attach/autowrap.rs` (`WrapInjector` struct + `new`/`reset`/`process`; delete `State`, `is_printable_start`, `feed`, `flush_glyph`, `emit_sequence`, `clamp_decstbm`, and the four `app_decstbm_*` tests)

**Interfaces:**
- Consumes: `libghostty_vt::Terminal::vt_write_until_wrap(&mut self, buf, skip) -> WrapWrite`, `WrapWrite { committed, wrap }` (from Task 2).
- Produces: unchanged public surface — `WrapInjector::{new, reset, resize, process, emit_region_setup}` and `AutowrapHandler` keep their existing signatures.

- [ ] **Step 1: Add the failing split-glyph regression test**

In the `#[cfg(test)] mod tests` of `src/attach/autowrap.rs`, add:

```rust
#[test]
fn wide_glyph_split_across_calls_not_sliced_at_wrap() {
    // 世 (E4 B8 96) is 2 wide. "abc" leaves one column, so 世 wraps. The glyph
    // arrives split across two process() calls. The break must land before the
    // whole glyph — its bytes must never be sliced by the \r\n.
    let mut wi = WrapInjector::new(4, 3).unwrap();
    let mut out = Vec::new();
    wi.process(b"abc\xe4", &mut out); // up to the first byte of 世
    wi.process(b"\xb8\x96", &mut out); // the rest of 世
    assert_eq!(out, "abc\r\n世".as_bytes());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --bin termd attach::autowrap::tests::wide_glyph_split`
Expected: FAIL — the old classifier handles this, but once `process` is rewritten in Step 4 without care it would slice; at this point it fails to compile only if you have already started removing code. If the current code still compiles, this test PASSES against the old classifier — that is fine; it documents the property the rewrite must preserve. Proceed to Step 3.

> Note: this test guards a property the *current* code already satisfies. Its real value is catching a regression in the Step 4 rewrite. Keep it.

- [ ] **Step 3: Remove the DECSTBM tests and the classifier helpers**

Delete from `src/attach/autowrap.rs`:
- the four tests `app_decstbm_bottom_is_clamped_to_server_rows`, `app_decstbm_within_bounds_passes_through`, `app_decstbm_reset_passes_through`, `app_decstbm_overflow_bottom_is_clamped`;
- the `enum State { ... }` definition;
- the methods `is_printable_start`, `feed`, `flush_glyph`, `emit_sequence`, `clamp_decstbm`.

Leave `emit_region_setup`, `new`, `reset`, `resize`, and `AutowrapHandler` in place (they are edited next).

- [ ] **Step 4: Rewrite the `WrapInjector` struct and `process`**

Replace the struct fields and `new`/`reset`/`process` in `src/attach/autowrap.rs`. The struct keeps only `term`, `server_rows`, and the new `carry`:

```rust
pub(super) struct WrapInjector {
    term: Terminal<'static, 'static>,
    /// Partial trailing unit fed to `term` but not yet emitted, carried to the
    /// next `process` call so a glyph split across calls is never sliced.
    carry: Vec<u8>,
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
            carry: Vec::new(),
            server_rows,
        })
    }

    pub(super) fn reset(&mut self, server_cols: u32, server_rows: u32) -> Result<()> {
        self.term = Terminal::new(TerminalOptions {
            cols: server_cols as u16,
            rows: server_rows as u16,
            max_scrollback: 0,
        })?;
        self.carry.clear();
        self.server_rows = server_rows;
        Ok(())
    }

    // resize(...) is unchanged. It preserves the tracking terminal's parser
    // state in place, so a carried partial unit stays consistent with `term`;
    // do NOT clear carry here (only reset, which rebuilds `term`, clears it).

    pub(super) fn process(&mut self, input: &[u8], out: &mut Vec<u8>) {
        // Prepend the carried partial-unit tail (already fed last call) and mark
        // it as the skip prefix so it is not re-fed; it lives in `buf` only to
        // keep one offset frame spanning the carry.
        let mut buf = std::mem::take(&mut self.carry);
        let mut skip = buf.len();
        buf.extend_from_slice(input);

        let mut emit = 0usize; // next unemitted offset within buf
        loop {
            let r = self.term.vt_write_until_wrap(&buf, skip);
            match r.wrap {
                Some(off) => {
                    out.extend_from_slice(&buf[emit..off]);        // up to wrapping glyph
                    out.extend_from_slice(b"\r\n");                 // injected break
                    out.extend_from_slice(&buf[off..r.committed]);  // the wrapping glyph
                    emit = r.committed;
                    skip = r.committed; // everything up to here is now fed
                    // loop: more of buf may remain unfed past the wrap
                }
                None => {
                    out.extend_from_slice(&buf[emit..r.committed]); // complete units
                    self.carry = buf[r.committed..].to_vec();       // partial tail (already fed)
                    break;
                }
            }
        }
    }
}
```

Keep the existing `use libghostty_vt::{Terminal, TerminalOptions};` import (the `WrapWrite` return type is used by value via field access and does not need importing unless referenced by name). Leave `resize` as-is — it preserves `term`'s parser state, so the carry stays valid across a resize.

- [ ] **Step 5: Run the autowrap tests**

Run: `cargo test --bin termd attach::autowrap`
Expected: PASS — the full retained corpus (`plain_text_passes_through`, `escape_sequence_passes_through`, `utf8_split_across_chunks_passes_through`, `full_line_then_printable_injects_break`, `full_line_then_control_does_not_inject`, `exact_fill_no_premature_break`, `wide_char_at_edge_injects_break`, `bottom_margin_scroll_injects_break`, `combining_mark_does_not_inject`, `tab_does_not_inject_a_break`, `wrap_survives_chunk_split`, `region_setup_emits_decstbm`, `resize_preserves_cursor_state_for_wrap_detection`, `falls_back_to_cell_when_client_too_small_on_resize`, and the new `wide_glyph_split_across_calls_not_sliced_at_wrap`) passes; the four `app_decstbm_*` tests are gone.

- [ ] **Step 6: Full workspace build and test**

Run: `cargo build && cargo test`
Expected: builds (Zig rebuilt from the local checkout); all tests pass; cell/region modes unaffected.

- [ ] **Step 7: Commit**

```bash
git add src/attach/autowrap.rs
git commit -m "attach/autowrap: bulk-feed via vt_write_until_wrap, drop classifier

Replace the per-glyph cursor-polling state machine with a bulk loop over
the new vt_write_until_wrap entry point, carrying the uncommitted
partial-unit tail so a glyph split across reads is never sliced at a wrap.
Drop in-proxy DECSTBM clamping (app PTY is server-sized, so the row map is
identity by construction).

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Notes for the implementer

- **Cursor access path:** mirror the existing `cursor_x`/`cursor_y` getters in `c/terminal.zig` (~line 701–702) for the exact `wrapper.terminal.screens.active.cursor.x` / `.y` expression and the `size.CellCountInt` type.
- **Why the byte-at-a-time path:** the function deliberately drives `wrapper.stream.next(byte)` rather than `nextSlice` so it can halt at an exact glyph boundary; the SIMD batch decoder in `nextSlice` cannot be interrupted mid-run. The proxy still issues bulk calls, so the cost is one FFI crossing per wrap-delimited chunk, far below the prior two-cursor-queries-per-glyph.
- **Carry invariant:** the carry is always set to `buf[committed..]` where `committed` is a parser ground boundary, so it always begins at a true unit lead byte — which is why `input[0]` is safe to classify as printable-vs-control even when the unit started in the carry.
- **bindgen struct name:** depending on bindgen's settings the generated Rust type may be `WriteUntilWrapResult` or `GhosttyTerminalWriteUntilWrapResult`; use whatever Step 2's `git diff` shows in Task 2 Step 5.
- **Manual smoke test (optional, after Task 3):** attach a client wider than the server with `--render-mode autowrap`, run something that wraps long lines and prints wide CJK at the edge, and confirm wrapping at the server width with OSC/true-color passthrough intact and no sliced glyphs.
- **Out of scope (unchanged from the first cut):** an app enabling `?69h`+DECSLRM can double-wrap against our injection; multi-client query-response routing remains deferred; a misbehaving app setting a vertical region beyond `server_rows` is no longer clamped (worst case: a slightly-wrong scroll).
