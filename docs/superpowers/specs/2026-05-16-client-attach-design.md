# Client Attach Design

**Date:** 2026-05-16
**Status:** Approved

## Problem

The current `attach` subcommand is a minimal stub in `main.rs` that streams raw PTY bytes
directly to stdout. This breaks when the client terminal size differs from the server PTY size:
the server PTY wraps text at its column count (e.g. 80), but the client terminal applies its own
wrapping rules to the raw bytes, producing incorrect output.

The server PTY owns its resolution. Clients must adapt to it.

## Solution

Each attached client maintains a local libghostty-vt `Terminal` at the server's resolution.
All streaming output passes through this terminal emulator, and only dirty rows are re-rendered
to stdout using absolute cursor positioning. This produces correct output regardless of client
terminal size: content within the server's bounds renders correctly, extra client space is left
empty (client larger), and content beyond the client's visible area is naturally clipped (client
smaller).

## Architecture

### New file: `src/attach.rs`

The attach implementation moves out of `main.rs` into `src/attach.rs`. As the logic grows
significantly in complexity, it deserves its own module. `main.rs` becomes a thin CLI dispatcher
that calls `attach::run(pty_id, socket, debug)`.

Both `--debug` and normal mode live in `attach.rs`. `--debug` is a fast path that skips
`LocalTerminal` creation entirely — it subscribes, streams events, and prints metadata to stderr
exactly as the current stub does. Normal mode creates `LocalTerminal` and runs the full render
pipeline. Keeping both in `attach.rs` avoids duplicating the subscribe/refresh setup and makes
the two paths easy to compare.

`attach.rs` owns:
- `LocalTerminal` struct — libghostty `Terminal` + `RenderState` + reusable `RowIterator` /
  `CellIterator`, all at the server's resolution. These are `!Send` and live on a single thread.
  Only created in normal (non-debug) mode.
- The main receive loop (gRPC stream dispatch), branching on `debug`
- The stdin task (escape sequence handling, write forwarding)
- The SIGWINCH task (local repaint trigger, normal mode only)
- `render_dirty()` — incremental render function (see Data Flow)

### New CLI subcommand: `termd resize <pty_id> <cols> <rows>`

Sends a `ResizeRequest` directly to the server. Used for manual testing and future programmatic
control. Accepts a pty_id prefix like other subcommands.

## Removed Behavior

- `attach` startup no longer sends a `ResizeRequest` to match the local terminal size.
- SIGWINCH in `attach` no longer sends a `ResizeRequest` to the server.

Both of these were appropriate for the old single-client passthrough model. With the server PTY
owning its resolution, clients must not resize it implicitly.

## Data Flow

### Initial attach

```
subscribe → subscribe ack
resize (REMOVED — no longer sent on attach)
refresh request → Refresh response:
    write refresh_bytes to stdout          (initial paint, reuses existing behavior)
    local_terminal.vt_write(refresh_bytes) (seeds local state)
```

### Streaming

```
StreamData(bytes, gen)
    if gen <= refresh_gen: discard         (already reflected in seeded state)
    local_terminal.vt_write(bytes)
    render_dirty()

StreamMetadata::Resize(cols, rows)
    local_terminal.resize(cols, rows, 0, 0)
    render_dirty()                         (libghostty marks Full dirty on resize)

StreamMetadata::Closed
    break receive loop, print "[Connection closed]"
```

### SIGWINCH

```
SIGWINCH
    render_state.set_dirty(Full)           (force full repaint from local state)
    render_dirty()
    (no ResizeRequest sent to server)
```

### render_dirty()

```
snapshot = render_state.update(&terminal)
match snapshot.dirty():
    Clean  → return (no output)
    Partial/Full →
        for each row in row_iter:
            if !row.dirty(): continue
            emit \x1b[{row+1};1H           (move cursor to row start)
            for each cell in row:
                emit \x1b[0m + SGR params  (reset + style)
                emit grapheme (or space)
            row.set_dirty(false)
        emit \x1b[0m                       (reset SGR)
        emit cursor visibility sequence
        emit \x1b[{cursor_y+1};{cursor_x+1}H  (restore cursor position)
flush stdout
```

The render output uses absolute cursor positioning within the server's row/col coordinate space.
A wider client terminal shows server content top-left with empty space around it. A narrower
client clips content at its own edges.

### Lag recovery

If a broadcast lag warning arrives on the data stream, the client requests a Refresh from the
server and re-seeds the local terminal with the response bytes, same as the initial attach flow.
This resynchronizes local state after missed chunks.

## LocalTerminal Struct

```rust
struct LocalTerminal {
    terminal: Terminal<'static, 'static>,
    render_state: RenderState<'static>,
    row_iter: RowIterator<'static>,
    cell_iter: CellIterator<'static>,
    server_cols: u32,
    server_rows: u32,
}
```

Created after the subscribe ack, using the cols/rows from the `PtyItem` already retrieved during
`resolve_pty_id()` (the `ListResponse`). Updated when `StreamMetadata::Resize` arrives.

## Error Handling

- `vt_write` never fails — bad input is logged internally by libghostty and ignored.
- `render_state.update()` → `OutOfMemory`: fatal, print error and exit attach.
- stdout write failure: break receive loop and exit cleanly (existing behavior).
- Broadcast lag: request Refresh from server, re-seed local terminal.

## Testing

- Integration test: create PTY, subscribe, send `termd resize <id> <cols> <rows>`, confirm
  `StreamMetadata::Resize` arrives with correct dimensions.
- Manual: attach with mismatched terminal sizes, verify text wrapping is correct.
- Manual: use `--debug` flag to inspect metadata events (resize, closed, etc.).
- Existing integration tests for the gRPC stream and metadata pipeline are unaffected.

## Future Work

- Per-subscriber dimension tracking: server could track each client's native resolution for
  features like per-client cursor rendering or smarter resize negotiation.
- Scrollback: local terminal accumulates scrollback; expose scroll commands in attach.
- Mouse forwarding: translate client mouse coordinates to server PTY coordinate space.
