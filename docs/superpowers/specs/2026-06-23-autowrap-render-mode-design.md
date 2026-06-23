# render-mode=autowrap — design spec

Status: **approved design, not yet implemented.** Feasibility cut.
Date: 2026-06-23.

Background and motivation live in `AUTOWRAP.md` at the repo root. This spec is the
concrete implementation design for a first, feasibility-oriented version of the new
mode, built in `src/attach/autowrap.rs`.

## Goal

Implement `render-mode=autowrap` as a new `RenderModeHandler` alongside `cell` and
`region`, without modifying region or cell mode behavior. It forwards raw server PTY
bytes (preserving passthrough) but **drops all horizontal-margin handling** (no
`?69`/DECSLRM/DECLRMM). Instead it runs a libghostty terminal over the forwarded
stream to track the cursor and **injects an explicit line break** at the exact byte
boundary where the server would have soft-wrapped, so the output is correct on any
client of width ≥ `server_cols`.

This first cut uses `is_cursor_pending_wrap()` polling (no upstream fork changes) to
validate feasibility, plus a basic test corpus of wrap edge cases.

## Non-goals (this cut)

- No change to `render-mode=cell` or `render-mode=region`.
- No multi-client query-response routing (`on_pty_write`/`on_device_attributes`).
- No upstream wrap-hook (the byte-offset soft-wrap callback) — polling only.
- No support for clients **narrower or shorter** than the server (falls back to cell).

## Why DECSTBM is safe to keep but DECSLRM is not

DECSTBM (vertical scroll region) has real side effects, but none reopen a contention
surface:

1. **Setting it homes the cursor.** So we emit DECSTBM only at reset points (init,
   resize, refresh) where we redraw anyway — never on ordinary stream output.
2. **Origin mode (DECOM, `\x1b[?6h`) makes positioning relative to the top margin.**
   Because term frames the box at the top, our top margin is always row 1, so
   origin-relative == absolute. DECOM is therefore an identity transform we don't
   track, as long as top stays 1.
3. **Scroll confinement is the feature**: LF at the bottom margin scrolls only rows
   `1..=server_rows`, leaving any client chrome below untouched.
4. **The app's own DECSTBM** is clamped (bottom margin ≤ server_rows); its home-on-set
   then lands inside our region.

DECSLRM is categorically worse because it is gated behind the `?69` (DECLRMM) mode
bit, which apps enable, query via DECRQM, and reset — an open-ended contended set.
DECSTBM has no enabling mode bit: one sequence, always available, nothing to query or
race. That is why autowrap keeps vertical framing and deletes the horizontal mechanism
rather than patching it.

## Components

### 1. `WrapInjector` — the wrap-injection core (the testable unit)

A self-contained struct owning a tracking `libghostty_vt::Terminal` sized
`server_cols × server_rows`. Pure bytes-in / bytes-out: feed input bytes, append
transformed bytes to an `out: &mut Vec<u8>`. No async, no I/O — the corpus tests drive
it directly.

Polling algorithm, per input byte `b`:

- If `tracking.vt_at_boundary()` **and** `b` starts a printable glyph **and**
  `tracking.is_cursor_pending_wrap()` → inject `\r\n` to `out` first (replicating the
  soft-wrap the wider client would not perform on its own).
- Feed `b` to the tracking terminal (`vt_write`) and forward `b` to `out`.

The injector relies entirely on the tracking terminal's `is_cursor_pending_wrap()`
flag rather than its own column bookkeeping. Consequences (all handled by the engine's
accounting, for free):

- **Deferred wrap**: a glyph at the last column followed by a *control* sequence (e.g.
  a cursor reposition) does not wrap — no spurious break, because we only inject before
  printables and the reposition clears the pending-wrap flag.
- **Wide chars / combining marks / grapheme clusters**: the engine wraps internally
  when a wide glyph won't fit and sets the flag earlier; our check fires before its
  lead byte.
- **Tabs** advancing past the edge set the pending-wrap flag; the next printable wraps.
- **UTF-8 multibyte and mid-escape**: `vt_at_boundary()` gates injection to parser
  ground state, so we never splice inside a sequence or a multibyte glyph.

"Starts a printable glyph" means: at a parser boundary, a byte that begins a printable
(>= 0x20, != 0x7f, including UTF-8 lead bytes) rather than a control/escape.

### 2. Vertical framing + app-sequence filter

Interleaved into the same single byte-scan pass as the injector, so framing and
injection are one pass:

- At reset points (init, resize, refresh): emit DECSTBM `\x1b[1;{server_rows}r` (top
  always 1). Rebuild the tracking terminal at these points.
- Clamp the app's own DECSTBM bottom margin to ≤ server_rows.
- **No** `?69` / DECSLRM / DECLRMM handling — horizontal sequences pass straight
  through untouched.
- Never emit DECSTBM on ordinary `Stream` events (avoids spuriously homing the cursor).

### 3. Handler + integration

- Add `RenderMode::Autowrap` variant; `autowrap.rs` implements `RenderModeHandler`
  (`init` / `on_pty_event` / `on_sigwinch` / `cleanup`).
- `init` / `on_sigwinch`: if `!server_fits_client(...)` → return
  `ChangeRenderMode(Cell)`.
- On refresh and resize: rebuild the tracking terminal and re-emit DECSTBM, matching
  how region/cell treat a refresh as a full reset/redraw.
- `cell.rs`: replace `allow_upgrade: bool` with `upgrade_to: Option<RenderMode>`, set
  from the requested mode at `create_handler`/dispatch time. Cell then upgrades back to
  whichever mode it was launched as (Region **or** Autowrap) when
  `server_fits_client` becomes true. The three hardcoded
  `ChangeRenderMode(RenderMode::Region)` sites in `cell.rs` use `upgrade_to` instead.

## Tests — basic corpus

Inline `#[cfg(test)]` in `autowrap.rs` (matching cell/region), feeding byte sequences
through `WrapInjector` and asserting transformed output:

- exactly `server_cols` glyphs then a control sequence → **no** injected break
  (deferred-wrap correctness)
- exactly `server_cols` glyphs then a printable → break injected before it
- a wide (2-column) glyph straddling the boundary
- a combining / zero-width sequence at the boundary
- a tab advancing past the edge
- resequencing across feed-call boundaries (input split mid-glyph and mid-escape
  between two calls)

## Known limitations (documented, not handled this cut)

- **App that itself uses `?69h` + DECSLRM**: horizontal sequences pass through and land
  correctly (columns are identity-mapped to the left-anchored box), but a client that
  honors the app's right margin could double-wrap against our injection. Rare — most
  apps never touch horizontal margins. TODO.
- Multi-client query-response routing deferred (single shared stream for now).
- One FFI query per printable is chatty; the upstream wrap-hook (soft-wrap byte-offset
  callback) is the eventual optimization.
- Clients narrower/shorter than the server are out of scope (cell mode's domain via
  the fallback).
