# Attach Scrollback — Notes and Future Considerations

This document captures design questions and deferred ideas from the v1
scrollback viewer design. The v1 spec is in
`docs/superpowers/specs/2026-05-19-attach-scrollback-design.md`.

## What v1 does

- `^A s` enters scrollback mode.
- Issues `ScrollbackRequest` to the server; displays the VT-encoded response
  bytes directly to stdout (no local libghostty in the viewer).
- One row per arrow press, one screen per request (request-on-demand).
- ESC exits; a fresh `RefreshRequest` restores the active screen.
- Works identically across all render modes (Cell, Formatter, Raw, Region)
  because the viewer is at the session-loop level, not inside any renderer.

## Known limitations / future ideas

### Local libghostty has `max_scrollback: 0`

Every client-side `LocalTerminal` is created with `max_scrollback: 0`, so the
client holds zero scrollback history. The "check local libghostty first, fall
back to ScrollbackRequest" idea was considered but dropped for v1: the check
would always miss. If a local cache ever becomes desirable (e.g., to avoid a
round-trip for recently-seen lines), `LocalTerminal::new` would need a
non-zero `max_scrollback` and logic to reconcile local vs server history.

### Server backpressure on busy PTYs

While the user is in scrollback mode, the renderer task is down and no one is
consuming `StreamData` responses. The gRPC send buffer absorbs the backlog for
a while, but on a very high-throughput PTY a long scrollback session could
stall the server-side reader thread. Options deferred:

- **Stream-draining during scrollback** (Option C in the design discussion):
  pass `resp_rx` into `show_scrollback` and discard `Stream` messages as they
  arrive. Prevents backpressure but adds complexity to the function signature.
- **Renderer-internal scrollback** (Option B): keep the renderer loop alive,
  suppressing display during scrollback. Cleanest for busy PTYs but requires
  implementation in all four renderers.

### Smooth / per-line scrolling

v1 makes one `ScrollbackRequest` per arrow press. Holding the arrow key will
fire N requests in rapid succession; each has an RTT (sub-millisecond on a
local socket but non-zero). Options for faster navigation:

- **Pre-buffer on entry**: issue one large `ScrollbackRequest` (e.g.,
  `row_count = 10 × screen_rows`) and navigate locally. Requires a
  line-indexed buffer since the VT bytes are not pre-split by row.
- **Dedicated libghostty viewer terminal**: create a `LocalTerminal` sized
  `cols × N` (where N is the fetched row count), feed all scrollback VT in,
  then use the Formatter's selection API to render a sliding row-window with
  zero additional I/O. Allows instant per-line navigation. Downside: a very
  tall terminal is unusual; the Formatter selection API requires `GridRef`
  lookups that traverse the internal page list (O(N) per call for the partial
  range path).
- **Chunked prefetch with overlap**: fetch the next chunk in the background
  as the user scrolls, similar to infinite-scroll pagination.

### Richer navigation keys

- `Page Up` / `Page Down` for whole-screen jumps.
- `g` / `G` to jump to oldest / most-recent row (home/end).
- `/` to open an inline search (requires line-indexed content).

### Display quality: status bar overlap

The status bar is written at row `rows` (the last visible row). If the
scrollback content itself has content on that row, the bar overwrites it. A
cleaner approach: reserve the last row as a status row and request
`row_count = rows - 1`. Deferred for v1.

### Alt-screen notification

When `total_scrollback_rows == 0` (PTY is in alt-screen mode), the viewer
currently shows a notice and exits. A future improvement: show a brief overlay
without clearing the active screen, so the user returns to exactly what they
were looking at.
