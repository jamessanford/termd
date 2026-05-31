# Pin-anchored scrollback — design

**Date:** 2026-05-31
**Branch:** `feature/scrollback`
**Status:** approved design, pending spec review

## Problem

Scrollback paging (`src/attach/scrollback.rs` + `do_scrollback` in `src/pty.rs`)
addresses history by `row_offset` = distance from the bottom (live edge). Each
keystroke issues an independent `ScrollbackRequest`; the server recomputes
`total = total_rows()` fresh and maps `row_offset` against the *current* bottom.

Because the anchor is the moving live edge, the view does not stay on the
content you are reading while output streams into the server's libghostty:

- New rows append at the bottom, so a fixed `row_offset` slides toward newer
  content. Scrolling up one line while N rows arrived nets a move of N−1 lines
  the wrong way.
- Under a full, constantly-evicting buffer (the realistic steady state), a
  top-relative absolute screen-y drifts too, because the oldest row is
  continuously pruned. Neither distance-from-bottom nor distance-from-top keeps
  you on the same content.

The only thing that stays on the content under both append and eviction is a
**tracked pin**: libghostty rides the pin along its row as the page list
mutates.

## Goal

While paging scrollback, the viewport stays locked to the content the user is
reading, regardless of live output or eviction. Scrolling moves relative to
what is displayed, not relative to a drifting coordinate. "Jump to live tail"
remains available.

Non-goal: live auto-refresh of the scrollback page. The pager stays modal and
static between keystrokes (it only re-fetches on input), as today.

## Mechanism: server-side tracked pin

The reader thread (sole owner of the non-Send libghostty `Terminal`) keeps one
tracked pin per scrolling client:

```
scrollback_pins: HashMap<String /* subscriber_id */, TrackedGridRef>
```

The pin marks the **top-left row of the viewport**. The client never knows the
pin's position. It sends intents (place / nudge / remove); the server owns all
position state and reports the resulting position back for the status bar.

### Pin cost (verified against upstream ghostty)

- Memory is trivial (`Pin = { node*, x, y, garbage }`, pool-allocated) and a
  held pin does **not** retain pages or block trimming: `PageList.erasePage`
  frees the evicted page and *relocates* the pin to the neighboring page's
  top-left (`PageList.zig:3919`). So holding a pin never grows memory.
- CPU: every page-list mutation iterates all tracked pins to fix them up.
  Upstream warns to "limit the number of active pins as much as possible"
  (`PageList.zig:5122`). There is always ≥1 (the viewport pin); one extra per
  actively-scrolling client is negligible. The cost only bites if pins
  **accumulate**, so release must be deterministic (below).

### Eviction note

Ordinary scrollback trimming relocates the pin to top-left and leaves
`garbage = false` (`PageList.zig:3942`) — it does **not** invalidate the pin.
So an abandoned pin rides to the oldest row and lives forever; we cannot rely
on `!has_value()` to garbage-collect it. `has_value()`/`garbage` is therefore
used only as a *rendering* safeguard (treat a garbage pin as "re-anchor to
oldest"), never as the GC.

## Protocol (`proto/terminal.proto`)

Free to change — no released consumers.

```proto
enum ScrollbackOp {
  SCROLLBACK_OP_OPEN  = 0;  // place pin at the live tail (row_offset 0); amount ignored
  SCROLLBACK_OP_MOVE  = 1;  // move pin by `amount` rows; + = older/up, - = newer/down
  SCROLLBACK_OP_CLOSE = 2;  // remove the pin; no data returned
}

message ScrollbackRequest {
  uint64       pty_id    = 1;
  reserved                 2;   // was row_offset
  uint32       row_count = 3;   // viewport height in rows
  ScrollbackOp op        = 4;
  sint32       amount    = 5;   // MOVE only; signed row delta (+ older / up)
}

message ScrollbackResponse {
  uint64 pty_id                = 1;
  uint64 generation            = 2;
  bytes  data                  = 3;
  uint32 total_scrollback_rows = 4;
  uint32 row_offset            = 5;  // viewport bottom-edge distance from live tail (0 = tail)
}
```

Request = verbs on the pin (the client's whole vocabulary is place / nudge-by-N
/ remove). Response = where the pin landed, in rows-from-tail. The dropped
`at_top`/`at_bottom` flags are derivable: `at_bottom ⟺ row_offset == 0`;
`at_top` is approximated client-side (`row_offset + row_count >= total`) and a
no-op `MOVE` past the top is harmlessly clamped.

`OPEN` with default-0 enum value means a request that omits `op` re-anchors to
the tail — a safe default. Per-keystroke `MOVE` always sets `op` explicitly.

## Server changes

### `src/pty.rs`

- The `scrollback_rx` channel message changes from `(u32, u32, reply)` to a
  struct: `{ subscriber_id: String, op: ScrollbackOp, amount: i32, row_count:
  u32, reply }`. (Use an internal enum mirroring `ScrollbackOp`, not the proto
  type, to keep `pty.rs` proto-free as it is today.)
- The reader thread owns `scrollback_pins: HashMap<String, TrackedGridRef>`.
- `do_scrollback` is rewritten around the pin (Screen space, y=0 oldest,
  y=total-1 live tail; pin = viewport top row `start_y`):
  - **OPEN:** `start_y = max(0, total - row_count)`; `track_grid_ref(Screen{0,
    start_y})`; insert/replace the subscriber's pin.
  - **MOVE:** read `pin.point(Screen).y` (if missing → OPEN fallback; if garbage
    → `start_y = 0`); `start_y = clamp(pin_y - amount, 0, max(0, total -
    row_count))`; re-`set` the pin to the new row.
  - **CLOSE:** drop the subscriber's pin; return empty data + `row_offset = 0`.
  - Render `render_rows = min(row_count, total - start_y)` rows `[start_y,
    start_y+render_rows-1]` via the existing `Formatter`/`Selection` path
    (unchanged from current partial-range code).
  - Report `row_offset = total - 1 - (start_y + render_rows - 1)`.
- A `drop_scrollback_pin(subscriber_id)` path so teardown can remove a pin
  (sends a CLOSE-equivalent control message to the reader thread).
- `PtyHandle::scrollback(...)` signature updates to take `subscriber_id, op,
  amount, row_count`; add `PtyHandle::close_scrollback(subscriber_id)`.

### `src/commands.rs`

- `handle_scrollback` gains `subscriber_id: &str` (threaded from
  `dispatch_command`, mirroring `handle_subscribe`), maps proto `op`/`amount`
  to the internal request, and calls the handle.

### `src/server.rs`

- `dispatch_command` passes `subscriber_id` into `handle_scrollback`.
- Pin cleanup on teardown (the actual GC, since eviction won't collect pins):
  - Disconnect loop (`server.rs:176`): for each subscribed pty, call
    `handle.close_scrollback(&subscriber_id)`.
  - `handle_unsubscribe` / `handle_destroy`: same, for the targeted pty.

## Client changes (`src/attach/scrollback.rs`)

- Intent-driven loop, no position math:
  - Entry → `OPEN` (place pin), render.
  - ↑ / ↓ → `MOVE amount = +1 / -1`.
  - `^B` / `^F` (and PageUp/Down) → `MOVE amount = +rows / -rows`.
  - Home → `MOVE amount = i32::MAX` (clamped to oldest); End → `OPEN`.
  - Exit (`q`/ESC/Ctrl-Q, or alt-screen teardown) → `CLOSE`.
- Delete `max_row_offset` and client-side clamping; gating is approximate off
  the server-reported `row_offset`/`total` (always safe to send and let the
  server clamp).
- `format_page` renders the server-reported `row_offset`/`total` in the status
  bar (unchanged formatting; values now come from the response).
- `fetch_scrollback` builds the new request shape and returns the response;
  `CLOSE` is fire-and-forget (no data expected, but still single round-trip for
  simplicity).

## Edge cases

- **Pin missing on MOVE** (never opened / reaped): treat as OPEN.
- **Pin garbage** (rare prune where relocation wasn't sensible): re-anchor
  `start_y = 0` (oldest), report accordingly.
- **`total <= row_count`** (history smaller than viewport): `start_y = 0`,
  render all rows, `row_offset = 0`.
- **`row_count == 0`** or **`total == 0`**: empty data, `row_offset = 0` (as
  today).
- **Two clients scrolling the same PTY:** independent pins keyed by
  `subscriber_id`; no interference.
- **Resize while paged:** the pin survives reflow (libghostty updates it);
  next keystroke re-renders at the pin's new location.

## Testing

Server unit tests (`src/pty.rs`, against a real `Terminal` with known history):

- OPEN then append rows → next MOVE renders content above the *same* rows the
  user was viewing (drift is gone): the pinned content does not slide.
- Eviction past the pinned row → pin relocates to oldest; render starts at
  oldest; `row_offset` reflects it; no panic.
- MOVE clamping at top (`start_y` floored at 0) and at bottom (`row_offset`
  reaches 0).
- OPEN positions at the live tail (`row_offset == 0`).
- CLOSE removes the pin; a subsequent MOVE behaves as OPEN.
- `total <= row_count` and empty-history cases.

Client unit tests (`src/attach/scrollback.rs`):

- `format_page` renders the server-reported `row_offset`/`total` (1-based "row
  X / total").
- Keystroke → correct `ScrollbackOp`/`amount` mapping.

## Out of scope

- Rendering scrollback through the cell/region formatters (the existing
  "dump"-style output is retained; that cleanup is tracked separately).
- Live auto-refresh of the scrollback page.
