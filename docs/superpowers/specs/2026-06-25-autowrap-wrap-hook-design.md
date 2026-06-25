# autowrap wrap-hook: `vt_write_until_wrap` design

Status: **approved design / not yet implemented.**

## Background

`render-mode=autowrap` (see `AUTOWRAP.md` and the prior spec
`2026-06-23-autowrap-render-mode-design.md`) forwards raw server PTY bytes for
passthrough and injects an explicit `\r\n` at each point where the narrower
server *would* have soft-wrapped, so the output is correct on any client of
width ≥ server width. The first cut — already implemented and shipped on the
`autowrap` branch — detects wraps by running a tracking `libghostty_vt::Terminal`
sized to the server and, **for every printable glyph**, querying `cursor_x()` /
`cursor_y()` before and after feeding it. A glyph "wrapped" when the cursor's row
increased or its column decreased (a position-based test that covers ordinary
deferred wrap, a wide glyph that cannot fit at the edge, and a wrap that scrolls
at the bottom margin).

That cut works but is "FFI-busy": two FFI cursor queries per printable glyph,
plus one `vt_write` per glyph, driven by a byte-classifying state machine in
`WrapInjector`. AUTOWRAP.md's step 3 calls for replacing the polling with an
upstream hook so the proxy can feed bytes in bulk and be told where the wraps
are.

## Goal

Add an upstream entry point to the libghostty fork that consumes a byte buffer in
bulk but **stops at the first soft-wrap**, reporting where the wrap occurred, so
the autowrap proxy can splice in a line break and resume. This removes the
per-glyph cursor polling and the byte-classifying state machine entirely.

## Key decisions (resolved during brainstorming)

1. **A new entry point, not a callback.** No userdata / trampoline / VTable
   plumbing. A plain function alongside `vt_write`.
2. **A new name, `vt_write_until_wrap`, leaving `vt_write` untouched.** Keeps the
   fork's diff against upstream ghostty easy to track.
3. **Consume-through, report offset.** When a wrap is hit, the function prints the
   wrapping glyph (applies the wrap internally) and returns, reporting both how
   far it consumed *and* the byte offset of the wrapping glyph's first byte. This
   always makes forward progress (no re-entry ambiguity, no infinite-loop risk
   from a deferred-wrap cursor sitting in pending-wrap state) and leaves the
   tracking terminal in a clean state. Contrast with a "stop before the wrapping
   glyph" model, which would require extra state to avoid re-signalling the same
   wrap at offset 0 on the next call.
5. **`skip` prefix + proxy `carry` for split units.** A multi-byte glyph can be
   split across two PTY read chunks *and* land exactly at the wrap column. If the
   proxy emitted each call's consumed bytes immediately, the partial bytes would
   go out before the wrap is known and the injected break would slice the glyph
   (mojibake / invalid UTF-8) — a passthrough-fidelity regression the current
   classifier avoids by buffering a glyph's bytes across calls. The fix: the proxy
   carries the uncommitted partial-unit tail (`carry: Vec<u8>`) and prepends it to
   the next chunk; `vt_write_until_wrap` takes a `skip` arg naming that
   already-fed prefix so it is not re-fed (which would double-advance the cursor)
   while still anchoring `committed` / `wrap_offset` in a single offset frame that
   spans the carry. This keeps the "no per-glyph cursor queries, no byte
   classifier" win — `carry` is a pure output-ordering buffer, not a parser.

6. **Drop in-proxy DECSTBM clamping.** The current proxy pass does two jobs:
   inject wraps and clamp an app's DECSTBM bottom margin to `server_rows`
   (fail-closed, commit `8a83447`). With bulk feeding the proxy no longer sees
   individual escape sequences. The clamp is removed rather than reintroduced,
   because the app's PTY is sized to exactly `server_cols × server_rows`
   (`pty.rs::resize` → `TIOCSWINSZ`), so the app's worldview is `server_rows`
   tall and — unlike region mode's contended horizontal margins — the app↔server
   row mapping is the identity. A correct app therefore never names a bottom
   margin beyond `server_rows`; the clamp only ever guarded a malformed sequence.
   If the fail-closed guard is wanted later, the cheap re-add is to have the
   variant report a second stop reason (a tagged result: `Wrap | DecstbmClamp |
   End`); we do not build that speculatively.

## Architecture

Three layers, plus the proxy. The existing `vt_write` is not modified.

### Layer 1 — Zig C-API (`examples/ghostty/src/terminal/c/terminal.zig`)

New exported function and result struct:

```zig
pub const WriteUntilWrapResult = extern struct {
    /// Offset (within `ptr[0..len]`) of the last parser ground boundary reached —
    /// i.e. bytes `[0..committed]` form complete units and are safe to emit. When
    /// `wrapped`, this is the boundary just past the wrapping glyph (feeding also
    /// stopped here). When not `wrapped`, bytes `[committed..len]` are a partial
    /// trailing unit the caller must carry. Also the caller's resume point.
    committed: usize,
    /// Whether processing stopped at a soft-wrap.
    wrapped: bool,
    /// If `wrapped`, the byte offset (within `ptr[0..len]`) of the first byte of
    /// the glyph that wrapped — i.e. where the caller inserts the break. May be
    /// `< skip` when the wrapping glyph began in the already-fed prefix.
    /// Unspecified when `wrapped` is false.
    wrap_offset: usize,
};

pub fn vt_write_until_wrap(
    t: Terminal,
    ptr: [*]const u8,
    len: usize,
    skip: usize,
    result: *WriteUntilWrapResult,
) callconv(lib.calling_conv) void;
```

The `skip` parameter names a prefix `ptr[0..skip]` that was **already fed** to the
parser on a previous call (the caller's carried partial-unit tail). The function
does **not** re-feed those bytes (re-feeding would double-process and corrupt the
cursor); it feeds only `ptr[skip..len]`. The full `ptr[0..len]` buffer exists
solely to give `consumed` / `wrap_offset` a single coordinate frame that spans the
carry, so a glyph that began in the carry reports a `wrap_offset < skip`. Callers
with no carry pass `skip = 0`.

Behavior:

- Feeds `ptr[skip..len]` advancing through the existing byte-at-a-time `next` path
  (not the SIMD `nextSlice` batch path), so it can halt at an exact glyph
  boundary.
- Tracks, as it advances: the current input offset; the offset of the last parser
  **ground boundary** (the start of the glyph currently being assembled); and the
  cursor `(x, y)` captured at that boundary. At entry the boundary offset is `skip`
  if the parser is already at a clean boundary, otherwise `0` (the in-progress
  unit began in the carry prefix).
- After each printable glyph completes (parser returns to ground), compares the
  cursor to the boundary cursor. A wrap is detected when `y` increased **or** `x`
  decreased — the same position-based test the current Rust cut uses, covering
  deferred wrap, wide-char-at-edge, and bottom-margin-scroll. Only units whose
  ground-boundary start byte is a printable (`>= 0x20`, `!= 0x7f`) are wrap-tested;
  C0 controls (notably `LF`, which legitimately advances the row) and escape
  sequences advance the boundary without ever signalling a wrap.
- On the first wrap: stop feeding. Set `committed` = offset just past the wrapping
  glyph (a ground boundary), `wrap_offset` = that glyph's ground-boundary start,
  `wrapped = true`. Bytes `[committed..len]` are left unfed for the next call.
- If `len` is reached with no wrap: `committed` = the last ground boundary reached
  (= `len` when the buffer ended cleanly, otherwise the start of the partial
  trailing unit), `wrapped = false`. All of `[skip..len]` has been fed.
- Partial trailing escape or UTF-8 sequences are fed normally (the stream's parser
  state persists across calls exactly as `vt_write` already relies on); they never
  produce a wrap report. The caller carries `[committed..len]` (see Layer 4).
- Control sequences and escapes pass through with no special handling (no DECSTBM
  clamping); they only matter insofar as they move the cursor, which the
  position-based detector already accounts for.

At most one wrap is reported per call; the caller loops to process the rest.

### Layer 2 — sys bindings (`crates/libghostty-vt-sys/src/bindings.rs`)

- Add the `extern "C"` declaration for `ghostty_terminal_vt_write_until_wrap`.
- Add the matching `#[repr(C)]` `WriteUntilWrapResult` struct (with the existing
  layout assertion style used elsewhere in the file).
- No new `TerminalOption` enum variant — this is a direct function, not a
  callback.

### Layer 3 — Rust `Terminal` (`crates/libghostty-vt/src/terminal.rs`)

Safe wrapper returning an idiomatic result:

```rust
/// Outcome of one `vt_write_until_wrap` call. Offsets are in the `buf` frame.
pub struct WrapWrite {
    /// Offset up to which `buf` formed complete units (safe to emit) and the
    /// resume point. `buf[committed..]` is a partial trailing unit to carry.
    pub committed: usize,
    /// `Some(offset)` if a soft-wrap was hit: the offset within `buf` at which
    /// the caller inserts a line break (start of the wrapping glyph). May be
    /// `< skip`. `None` if no wrap occurred in this call.
    pub wrap: Option<usize>,
}

impl Terminal<'_, '_> {
    /// Feed `buf[skip..]` (the prefix `buf[0..skip]` was fed on a prior call and
    /// is not re-fed; it only anchors the offset frame), stopping at the first
    /// soft-wrap. See the design spec for the `skip`/carry protocol.
    pub fn vt_write_until_wrap(&mut self, buf: &[u8], skip: usize) -> WrapWrite { /* ffi call */ }
}
```

`wrap` folds `wrapped`/`wrap_offset` into an `Option` so callers cannot read a
meaningless offset.

### Layer 4 — proxy (`src/attach/autowrap.rs`)

`WrapInjector` keeps the tracking `Terminal`, `server_rows`, and gains one new
field `carry: Vec<u8>` (the partial-unit tail fed but not yet emitted). Removed:
the `State` enum, the `state` / `glyph` / `seq` fields, `is_printable_start`,
`feed`, `flush_glyph`, `emit_sequence`, `clamp_decstbm`, and the four
`app_decstbm_*` tests. `new`/`reset` initialize/clear `carry`; `resize` and
`emit_region_setup` are otherwise unchanged. `process` becomes:

```rust
pub(super) fn process(&mut self, input: &[u8], out: &mut Vec<u8>) {
    // Prepend the carried partial-unit tail. Those bytes were already fed to the
    // tracking terminal last call, so mark them as the `skip` prefix (not re-fed);
    // they live in `buf` only to keep one offset frame spanning the carry.
    let mut buf = std::mem::take(&mut self.carry);
    let mut skip = buf.len();
    buf.extend_from_slice(input);

    let mut emit = 0; // next unemitted offset in buf
    loop {
        let r = self.term.vt_write_until_wrap(&buf, skip);
        match r.wrap {
            Some(off) => {
                out.extend_from_slice(&buf[emit..off]);          // up to wrapping glyph
                out.extend_from_slice(b"\r\n");                   // injected break
                out.extend_from_slice(&buf[off..r.committed]);    // the wrapping glyph
                emit = r.committed;
                skip = r.committed; // everything up to here is now fed
                // continue: more of buf may remain unfed past the wrap
            }
            None => {
                out.extend_from_slice(&buf[emit..r.committed]);   // complete units
                self.carry = buf[r.committed..].to_vec();         // partial tail, already fed
                break;
            }
        }
    }
}
```

Each iteration that wraps advances `committed` past at least the wrapping glyph,
and the no-wrap branch always terminates, so the loop is bounded. `reset` must
clear `carry` (a refresh rebuilds the tracking terminal, orphaning any carried
bytes). `AutowrapHandler` and its `RenderModeHandler` impl are unchanged.

## Testing

- **Zig** (`c/terminal.zig` tests): `vt_write_until_wrap` over a narrow grid —
  full consume with no wrap (`committed == len`); a single wrap with correct
  `committed` / `wrap_offset`; wide-char-at-edge wrap; bottom-margin scroll wrap;
  an `LF` does not report a wrap; an escape sequence preceding a wrap (offset
  unaffected by the escape's bytes); exact-fill with no premature wrap; a
  `skip`-prefix call where the wrapping glyph began in the skipped prefix
  (`wrap_offset < skip`) and re-feeding is suppressed (cursor not double-advanced);
  a buffer ending mid-unit reports `committed < len` with `wrapped == false`.
- **Rust vt** (`terminal.rs` tests): a thin test that the safe wrapper maps
  `wrapped`/`wrap_offset` to `Option` and threads `skip`/`committed` correctly for
  a wrap and a no-wrap input.
- **Proxy** (`autowrap.rs` tests): the existing wrap-injection corpus is kept
  **byte-for-byte identical in expectations** — it is the regression guarantee
  that the bulk path reproduces the old classifier. One new test is added: a wide
  (multi-byte) glyph split across two `process` calls at the wrap column must emit
  the break *before* the whole glyph, never slicing its bytes. Only the DECSTBM
  tests are removed.
- **Build:** `cargo build` rebuilds the Zig static lib from the local
  `examples/ghostty` checkout (already pinned to the fork commit), then the Rust
  crates.

## Known limitations

- A misbehaving app could set a vertical scroll region whose bottom exceeds
  `server_rows`; with the clamp removed, the worst case is a slightly-wrong
  scroll. AUTOWRAP.md already treats the vertical axis as low-contention and this
  outcome as acceptable.
- The wrap-aware path forgoes the SIMD `nextSlice` batch decode in favor of the
  byte-at-a-time `next` path so it can halt at an exact boundary. This trades a
  decode micro-optimization for interruptibility; the proxy still issues bulk
  calls, so the per-call overhead (one FFI crossing per wrap-delimited chunk)
  is far below the prior two-cursor-queries-per-glyph cost.
- Carried over from the first cut, unchanged by this work: an app that itself
  enables `?69h` + DECSLRM can double-wrap against our injection; multi-client
  query-response routing is still deferred.
