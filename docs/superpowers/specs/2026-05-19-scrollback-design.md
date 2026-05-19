# Scrollback: Proto Design

**Date:** 2026-05-19

## Overview

Add a `ScrollbackRequest`/`ScrollbackResponse` pair to the gRPC protocol so
clients can retrieve VT-encoded scrollback history from a server-side PTY.
The active-screen attach path (`src/attach/`) is explicitly out of scope for
this spec; only `proto/terminal.proto`, `src/pty.rs`, `src/commands.rs`, and
`src/server.rs` are addressed.

## Proto changes (`proto/terminal.proto`)

### New command field

```proto
message TerminalCommand {
  oneof command {
    // … existing fields 1–9 …
    ScrollbackRequest scrollback = 10;
  }
}
```

### New response field

```proto
message TerminalResponse {
  oneof response {
    // … existing fields 1–6 …
    ScrollbackResponse scrollback = 7;
  }
}
```

### New message types

```proto
// Request a slice of scrollback history for a PTY.
message ScrollbackRequest {
  string pty_id     = 1;
  // Distance from the active-screen edge, in rows. 0 = the row immediately
  // above the active screen (most recent history). Increasing values go
  // further back in time.
  uint32 row_offset = 2;
  // Maximum number of scrollback rows to return.
  uint32 row_count  = 3;
}

// VT-encoded scrollback content for a PTY.
message ScrollbackResponse {
  string pty_id                = 1;
  // Generation counter at snapshot time. Clients use this to know which
  // StreamData events to buffer and re-apply when exiting scroll mode.
  uint64 generation            = 2;
  // VT-encoded content. Empty when row_offset >= total_scrollback_rows or
  // the PTY is in alt-screen mode.
  bytes  data                  = 3;
  // Total available scrollback rows at snapshot time. 0 if the PTY is in
  // alt-screen mode (alternate screen has no scrollback).
  uint32 total_scrollback_rows = 4;
}
```

## Semantics

- `row_offset=0, row_count=1000` → the 1000 rows immediately above the
  active screen (most recent history).
- `row_offset=1000, row_count=1000` → rows 1001–2000 above the active screen.
- If `row_offset >= total_scrollback_rows`: `data` is empty,
  `total_scrollback_rows` is still populated.
- If `row_offset + row_count > total_scrollback_rows`: response is clamped to
  available rows (no error).
- Subscription is not required (same policy as `RefreshRequest`).
- Returns `ScrollbackResponse` directly — no `CommandResponse` ack first
  (same pattern as `RefreshRequest`).
- The VT content has no DECSTR/clear/home preamble. It is raw styled content,
  not a terminal-state restore. The Formatter ends with `\x1b[0m`.

## Implementation notes for `pty.rs`

### New types

```rust
pub struct ScrollbackData {
    pub generation:            u64,
    pub data:                  Bytes,
    pub total_scrollback_rows: u32,
}
```

### `PtyHandle` additions

Add a `scrollback_tx` channel alongside `refresh_tx`:

```rust
scrollback_tx: std::sync::mpsc::SyncSender<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>
```

Add `PtyHandle::scrollback(row_offset: u32, row_count: u32) -> Result<ScrollbackData>` — sends the
`(row_offset, row_count, tx)` tuple, writes the wakeup pipe, and awaits the
oneshot. Identical pattern to `PtyHandle::refresh()`.

### Reader thread

In the wakeup handler, drain `scrollback_rx` alongside `refresh_rx`:

```rust
while let Ok((row_offset, row_count, reply_tx)) = scrollback_rx.try_recv() {
    let gen = generation.load(Ordering::Relaxed);
    let result = do_scrollback(&terminal, row_offset, row_count, gen,
                               current_cols);
    let _ = reply_tx.send(result);
}
```

### `do_scrollback`

```rust
fn do_scrollback(
    terminal:   &Terminal<'static, 'static>,
    row_offset: u32,
    row_count:  u32,
    generation: u64,
    cols:       u32,
) -> Result<ScrollbackData>
```

Implementation outline:

1. `terminal.scrollback_rows()` → `total`. If `total == 0` (alt screen or no
   history), return `ScrollbackData { generation, data: Bytes::new(), total_scrollback_rows: 0 }`.
2. If `row_offset >= total`, return empty data with `total_scrollback_rows = total as u32`.
3. Convert to `Point::History` y-coordinates (y=0 = oldest, y=total-1 = most
   recent):
   ```
   end_y   = total - 1 - row_offset
   rows    = row_count.min(end_y + 1)
   start_y = end_y + 1 - rows
   ```
4. Resolve two `GridRef`s via `terminal.grid_ref(Point::History(...))` for
   the start and end of the range.
5. Build `Formatter` with `Format::Vt`, `selection: Some(range)`, and all
   `FormatterTerminalExtra` flags `false` (use `ffi::sized!(ffi::FormatterTerminalExtra)`
   as the base — `Default::default()` does not set the `size` field correctly
   for the sized-struct ABI). No DECSTR, no mode restore, no cursor emit.
6. Call `fmt.format_alloc(None)` and return the bytes.

**Optimization:** when `row_offset == 0` and `row_count >= total`, pass
`selection: None` to the Formatter to dump all scrollback without the
`grid_ref` calls. This is the common case (initial load of recent history)
and avoids the page-list traversal entirely.

> **NOTE (performance):** `terminal.grid_ref(Point::History(...))` traverses
> the internal scrollback page list to locate the target row, which is
> O(scrollback_depth). For the partial-range case (row_offset > 0), two such
> traversals are required — one for the start row and one for the end row. For
> `max_scrollback = 10_000` this is likely fast enough in practice, but
> `do_scrollback` runs on the reader thread and blocks live PTY I/O during the
> call. If scrollback requests become a latency concern, the fix is to move the
> Formatter call to a background thread (similar to how `do_refresh` could be
> offloaded). The Formatter itself runs in native code and is not the
> bottleneck; the page-list traversal is the cost to watch.

## Implementation notes for `commands.rs`

Add `handle_scrollback(registry, req) -> TerminalResponse` (async), analogous
to `handle_refresh`:

```rust
pub async fn handle_scrollback(
    registry: &PtyRegistry,
    req: ScrollbackRequest,
) -> TerminalResponse {
    let id = req.pty_id.clone();
    match registry.get(&id) {
        None => err_response(id, "PTY not found".into()),
        Some(h) => match h.scrollback(req.row_offset, req.row_count).await {
            Ok(data) => TerminalResponse {
                response: Some(Response::Scrollback(ScrollbackResponse {
                    pty_id:                id,
                    generation:            data.generation,
                    data:                  data.data.to_vec(),
                    total_scrollback_rows: data.total_scrollback_rows,
                })),
            },
            Err(e) => err_response(id, e.to_string()),
        },
    }
}
```

## Implementation notes for `server.rs`

Wire `Command::Scrollback(req)` → `handle_scrollback(registry, req).await` in
the command dispatch match, alongside the existing `Command::Refresh` arm.

## Client-side scroll mode (attach — future work)

This section is informational only; attach changes are out of scope.

When a client enters scroll mode:
1. Stop applying incoming `StreamData` to the active-screen render (buffer
   the events).
2. Send `ScrollbackRequest`.
3. Render the `ScrollbackResponse.data` as a frozen scrollback view.
4. For additional pages, send further `ScrollbackRequest`s with increasing
   `row_offset`. Note: if the PTY is running, `total_scrollback_rows` may
   grow between pages (new rows pushed into history). Clients should re-anchor
   if they need consistent pagination.
5. On exit from scroll mode: apply all buffered `StreamData` with
   `generation > scrollback_response.generation` and resume live rendering.
   Send a `RefreshRequest` first to avoid missing any intermediate state.

## Alt-screen behavior

When the PTY is running a full-screen application (vim, htop, etc.) the
alternate screen is active. `terminal.scrollback_rows()` returns 0 in this
case. The response will have `data = []` and `total_scrollback_rows = 0`.
Clients should hide or disable the scrollback UI accordingly.

## Files changed

| File | Change |
|---|---|
| `proto/terminal.proto` | Add `ScrollbackRequest` (field 10 of `TerminalCommand`) and `ScrollbackResponse` (field 7 of `TerminalResponse`) |
| `src/pty.rs` | `ScrollbackData` struct; `scrollback_tx/rx` channel; `PtyHandle::scrollback()`; `do_scrollback()` function; reader thread drain |
| `src/commands.rs` | `handle_scrollback()` async function |
| `src/server.rs` | Dispatch `Command::Scrollback` → `handle_scrollback` |
