# Design: `--render-mode=region` for `termd attach`

**Date:** 2026-05-17
**Status:** Approved
**Branch:** main

---

## Context

`termd attach` now supports `--render-mode` with three values: `cell`, `formatter`, and `raw`
(see `2026-05-17-attach-render-modes-design.md`). This spec adds a fourth: `region`.

`raw` mode is fast and zero-allocation, but breaks when the client terminal is larger than the
server PTY because cursor positions and scroll regions are absolute. `region` mode solves this
by confining the client's scroll area to the server PTY's dimensions using DECSTBM, then
forwarding the raw byte stream — with a streaming filter that intercepts and rewrites any
conflicting escape sequences emitted by programs running on the server (vim, less, htop, tmux).

---

## Scope

- New `RenderMode::Region` variant in `src/attach/mod.rs`
- New file `src/attach/region.rs` containing both `run()` and `VtFilter`
- No libghostty on the render path (mirrors `raw.rs`)
- No server-side changes

---

## File Structure

```
src/attach/
  mod.rs       -- add Region variant, mod region
  region.rs    -- run() + VtFilter
```

---

## Architecture

`region::run` follows the same select-loop structure as `raw::run`. The key addition is a
`VtFilter` that sits between the gRPC stream and stdout: bytes in, (possibly rewritten) bytes
out, state carried across calls to handle escape sequences that span buffer boundaries.

```
gRPC stream bytes → VtFilter::filter() → stdout
```

**Startup sequence:**
1. Query client terminal size via `get_terminal_size()`
2. If client is smaller than server PTY in either dimension: print a warning to stderr and
   dispatch to `cell::run(ctx)` instead (region mode cannot work; can't display content that
   doesn't fit)
3. Emit `\x1b[1;{server_rows}r` (DECSTBM) to confine client scrolling to top N rows
4. If client is wider than server: also emit `\x1b[?69h\x1b[1;{server_cols}s`
   (enable DECLRMM, then set left-right margin to server cols)
5. Pass `refresh_bytes` and buffered chunks through `VtFilter` to stdout

**Select loop:**

| Event | Action |
|---|---|
| `Stream(s)` where `s.generation > refresh_gen` | filter bytes → stdout |
| `Refresh(rf)` | update `refresh_gen`, filter bytes → stdout |
| `Metadata(Resize)` | `filter.update_region(new_rows, new_cols)`, emit `\x1b[2J`, re-emit region setup |
| `Metadata(Closed)` | break |
| SIGWINCH | re-query client size; re-emit region setup (clamped if client shrank) |
| shutdown (`~.`) | break |

**Cleanup on exit** (after loop, before returning):
- Emit `\x1b[r` (DECSTBM full reset — restore client scroll region to full screen)
- If DECLRMM was enabled: emit `\x1b[?69l` (disable DECLRMM)

---

## VtFilter

### State machine

```rust
enum CsiMode { Normal, Private }

enum FilterState {
    Normal,
    AfterEsc,
    InCsi(CsiMode),
}

struct VtFilter {
    state:           FilterState,
    buf:             Vec<u8>,   // current escape sequence accumulator
    server_rows:     u32,
    server_cols:     u32,
    declrmm_active:  bool,      // true if we emitted DECLRMM to client
    in_alt_screen:   bool,
}
```

### API

```rust
impl VtFilter {
    fn new(server_rows: u32, server_cols: u32) -> Self
    fn update_region(&mut self, rows: u32, cols: u32)
    fn filter(&mut self, input: &[u8], out: &mut Vec<u8>)
    fn emit_region_setup(&self, out: &mut Vec<u8>)   // DECSTBM [+ DECLRMM/DECSLRM]
}
```

`filter` is the pipe interface: called once per chunk, produces output bytes in `out`.

### State transitions

**Normal:**
- `\x1b` → push to `buf`, → `AfterEsc`
- anything else → push directly to `out`

**AfterEsc** (`buf` holds `\x1b`):
- `[` → push to `buf`, → `InCsi(Normal)`
- `c` → RIS: flush `emit_region_setup` to `out`, clear `buf`, → `Normal`
- `]` `P` `_` `^` `X` → flush `buf` + this byte to `out`, → `Normal`
  *(OSC/DCS/APC/PM/SOS openers: content can be kilobytes, pass immediately without buffering)*
- `\` → flush `buf` + this byte to `out`, → `Normal` *(ST terminator)*
- anything else → flush `buf` + this byte to `out`, → `Normal` *(unknown two-char ESC)*

**InCsi(mode)** (`buf` holds `\x1b[` + accumulated parameter/intermediate bytes):
- byte `0x30–0x3F` (digits, `;`, `?`) → accumulate in `buf`; if first byte after `[` and equals `?`, → `InCsi(Private)`
- byte `0x20–0x2F` (intermediate byte) → accumulate in `buf`
- byte `0x40–0x7E` (final byte) → complete sequence: **dispatch**, clear `buf`, → `Normal`
- `\x1b` → safety: flush `buf` to `out`, → `AfterEsc` with fresh `buf` = `[0x1b]`
- `buf.len() > 32` → safety: flush `buf` to `out`, → `Normal`

The 32-byte safety limit is well above the longest sequence we target (≤ 16 bytes for
DECSTBM with three-digit row numbers) and far below any DCS/OSC content.

### CSI dispatch table

| Final byte | Mode | Param(s) | Action |
|---|---|---|---|
| `r` | Normal | any (including empty) | DECSTBM: parse `top;bottom` (empty → `1;0`), clamp `bottom` to `server_rows`, emit rewritten `\x1b[{top};{bottom}r` |
| `s` | Normal | any | DECSLRM: parse `left;right`, clamp `right` to `server_cols`, emit rewritten `\x1b[{left};{right}s` |
| `h` | Private | `69` | DECLRMM enable: suppress (we manage this state via `declrmm_active`) |
| `l` | Private | `69` | DECLRMM disable: suppress |
| `h` | Private | `1049` | Alt-screen enter: pass through, set `in_alt_screen = true` |
| `l` | Private | `1049` | Alt-screen exit: pass through, then append `emit_region_setup` to `out`, set `in_alt_screen = false` |
| anything else | — | — | pass `buf` + final byte to `out` unchanged |

Bare `\x1b[r` (no params) is a full-screen margin reset — rewritten to `\x1b[1;{server_rows}r`.

---

## Region management

### Server resize (`Metadata(Resize)`)

1. `filter.update_region(new_rows, new_cols)` — stores new bounds; clamps to client terminal
   size if server grew beyond what the client can display, with a warning to stderr
2. Emit `\x1b[2J` to clear stale content
3. `filter.emit_region_setup(&mut out)` to push updated DECSTBM (and DECLRMM/DECSLRM if
   applicable) to the client

### SIGWINCH (client terminal resized)

1. Re-query client terminal size
2. If client is now smaller than server in either dimension: log a warning to stderr; continue
   with clamped region (best-effort — a full runtime fallback adds complexity not worth it in
   the research phase)
3. Otherwise: `filter.emit_region_setup(&mut out)` with updated client bounds

### `update_region` semantics

`update_region` stores the new server dimensions. `emit_region_setup` uses
`min(server_rows, client_rows)` and `min(server_cols, client_cols)` when emitting DECSTBM/DECSLRM,
so both startup and any subsequent resize stay consistent.

---

## What is not in scope

- Mode 2026 synchronized output wrapping (noted in `RENDERING_MODES.md` as a quick win for `formatter`)
- Handling `\x1b[?1049h` save/restore of DECSTBM (some terminals save scroll margins on alt-screen
  enter and restore on exit — we re-emit unconditionally on `?1049l` which is sufficient)
- Full VT parser (ghostty C API extension) — documented in `RENDERING_MODES.md` as a future upstream contribution
- Performance profiling of region mode vs. raw/cell
