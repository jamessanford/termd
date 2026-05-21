# Follow-on: Route Repaint chunks as Response::Refresh

## Background

`PtyChunk` now carries a `kind: PtyChunkKind` field (`Stream` or `Repaint`).
`Repaint` chunks come from `do_refresh` — full-screen formatter output broadcast
during resize and screen-switch events. They currently still arrive at clients as
`Response::Stream`, indistinguishable from incremental PTY data.

## What needs to happen

### 1. `src/server.rs` — route by kind at the one conversion point (~line 129)

```rust
PtyEvent::Data(chunk) => match chunk.kind {
    PtyChunkKind::Stream => proto::TerminalResponse {
        response: Some(proto::terminal_response::Response::Stream(
            proto::StreamData {
                pty_id,
                generation: chunk.generation,
                data: chunk.data.to_vec(),
            }
        )),
    },
    PtyChunkKind::Repaint => proto::TerminalResponse {
        response: Some(proto::terminal_response::Response::Refresh(
            proto::RefreshResponse {
                pty_id,
                generation: chunk.generation,
                data: chunk.data.to_vec(),
                cursor_x: 0,  // unused by all client modes; cursor is in the VT data
                cursor_y: 0,
            }
        )),
    },
},
```

### 2. `src/attach/cell.rs` — add `Response::Refresh` arm to main loop

`raw.rs` and `region.rs` already handle `Response::Refresh` mid-loop. `cell.rs`
silently drops it. Add an arm that re-seeds the local terminal and forces a full
render — same as what it does at startup with the initial refresh:

```rust
Some(Response::Refresh(rf)) => {
    current_refresh_gen = rf.generation;
    lt.terminal.vt_write(&rf.data);
    out.extend_from_slice(b"\x1b[2J");
    render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter,
                 &mut lt.cell_iter, true, &mut out)?;
}
```

### 3. `src/attach/formatter.rs` — same pattern as cell.rs

```rust
Some(Response::Refresh(rf)) => {
    current_refresh_gen = rf.generation;
    lt.terminal.vt_write(&rf.data);
    out.extend_from_slice(b"\x1b[2J");
    render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter,
                 &mut lt.cell_iter, true, &mut out)?;
}
```

## Why cursor_x/cursor_y are 0

`PtyChunk` doesn't store cursor position — it was discarded when packing
`RefreshData` into a chunk. The cursor position is embedded in the VT data
itself via the formatter's `screen.cursor: true` option (emits a CUP sequence).
No client render mode reads `cursor_x/cursor_y` from `RefreshResponse` anyway;
cell and formatter modes compute cursor position from their local terminal state.

## Prerequisite

Validate that the formatter refresh output (`feat/formatter-refresh` branch)
looks correct on real sessions before wiring this up — especially with programs
that use DECSTBM (vim, less, htop) to confirm scrolling region state is restored
correctly on connect and after resize.
