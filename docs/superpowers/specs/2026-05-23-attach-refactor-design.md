# Attach Module Refactor: Decouple gRPC from Render Modes

## Problem

Every render mode (`cell.rs`, `raw.rs`, `region.rs`) and every control-plane helper (`subscribe`, `request_refresh`, `fetch_list`, `fetch_scrollback`) directly reads from `tonic::Streaming<TerminalResponse>`, matches on protobuf response variants, filters by `pty_id`, and silently discards non-matching messages. The `RunContext` struct shuttles the raw `resp_rx` between render modes and back to the session loop so ownership can transfer.

This makes it hard to:
- Run multiple simultaneous PTY views (each in their own render mode)
- Connect to multiple termd servers
- Reason about where event filtering/discarding happens

## Design

### Core Idea

The main loop in `mod.rs::run()` becomes a long-lived demux loop that permanently owns `resp_rx`. Render modes become synchronous event handlers called by the loop, rather than async functions that own their own select loops. The hot path (`Response::Stream`) is a direct callback into the active handler — no channel hop, no extra copy.

### PtyEvent and RenderModeHandler Trait

```rust
enum PtyEvent<'a> {
    Stream { gen: u64, data: &'a [u8] },
    Refresh { gen: u64, cols: u32, rows: u32, data: &'a [u8] },
    Resize { cols: u32, rows: u32 },
    Closed,
}

enum EventResult {
    Continue,
    ChangeRenderMode(RenderMode),
    RequestRefresh,
}

trait RenderModeHandler {
    fn on_pty_event(&mut self, event: PtyEvent, out: &mut Vec<u8>) -> Result<EventResult>;
    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> Result<EventResult>;
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<()>;
}
```

The hot path: the demux loop reads `Response::Stream`, constructs `PtyEvent::Stream` with a borrowed `&[u8]` to the data, calls `handler.on_pty_event()`, writes the resulting `out` bytes to stdout. No channel hop, no copy of the payload bytes.

Protobuf types are still used freely throughout the codebase — the abstraction is about who reads the gRPC stream and dispatches events, not about hiding protobufs.

### Long-Lived Demux (Main Loop)

`run()` in `mod.rs` is restructured so that `resp_rx` never leaves the function. The main loop permanently owns it and dispatches events to the active handler:

```
run() {
    // owns resp_rx for the entire session lifetime
    // owns active_handler: Box<dyn RenderModeHandler>
    // owns active_pty_id: String

    'session: loop {
        select! {
            msg = resp_rx.message() => {
                // pty_id filtering happens HERE, once
                // Stream/Refresh/Metadata -> active_handler.on_pty_event()
                // control-plane responses -> resolve pending request
            }
            action = action_rx.recv() => {
                // handle inline: subscribe, create, destroy, switch...
            }
            _ = sigwinch.recv() => {
                active_handler.on_sigwinch(...)
            }
            _ = refresh_debounce_timer => {
                // send RefreshRequest after SIGWINCH settles
            }
        }
        // write handler's output to stdout
    }
}
```

`RunContext` goes away entirely. `RunOutcome` simplifies to just `ServerClosed | Detach` — no more `ChangeRenderMode` or `Action` variants carrying the stream back up.

### Control-Plane Operations

Control-plane operations (subscribe, request_refresh, fetch_list, create, destroy, scrollback) become phases of the main loop. When the session loop needs to do a control-plane operation (e.g., during a PTY switch), it sends the command on `cmd_tx` and the main loop enters a temporary phase where it reads `resp_rx` looking for the matching response, discarding or buffering other events as appropriate.

This is similar to how the current standalone helpers (`subscribe()`, `request_refresh()`, etc.) work, but they no longer borrow `resp_rx` — the main loop handles it inline.

### Render Mode Switches

When `on_pty_event` or `on_sigwinch` returns `ChangeRenderMode(new_mode)`, the main loop swaps `active_handler` to the new mode, requests a refresh if needed, and continues. No need to break out of the loop and re-enter.

### SIGWINCH Debounce

When `on_sigwinch` returns `RequestRefresh`, the main loop starts or resets a debounce timer (approximately 1 second). The timer is added to the `select!`. When it fires, the loop sends the `RefreshRequest`. If more SIGWINCHes arrive before the timer fires, it resets. The handler doesn't know about the debounce.

### Scrollback

Scrollback remains a main-loop phase, not a render mode handler. When the user triggers `ShowScrollback`, the main loop pauses the active handler, enters the alternate screen, takes over stdin, and runs the scrollback request/response cycle inline. This keeps scrollback's fundamentally modal, synchronous request/response interaction separate from the streaming render mode trait.

### Handler Implementations

**CellHandler** holds `LocalTerminal`, `current_refresh_gen`, `allow_upgrade`, `item`. Hot path: `vt_write(data)` + `render_dirty()` into output buffer.

**RawHandler** holds `refresh_gen`. Hot path: copy data directly to output buffer. The thinnest possible handler.

**RegionHandler** holds `VtFilter`, `current_refresh_gen`, client/server dimensions, `item`. Hot path: `filter.filter(data, out)`.

### Multi-View Future

For multiple simultaneous PTY views, the main loop would hold a `HashMap<String, Box<dyn RenderModeHandler>>` and dispatch by `pty_id`. Each handler writes to its own output region. This is a natural extension of the single-handler design.

### Multi-Server Future

Each termd server gets its own `resp_rx` stream. The main loop (or a coordinator above it) selects across all streams and dispatches to the appropriate handlers. The handler trait is server-agnostic.

## File-Level Changes

### New types (in `mod.rs` or a new `handler.rs`):
- `PtyEvent` enum
- `EventResult` enum
- `RenderModeHandler` trait

### `mod.rs`:
- `run()` restructured: `resp_rx` stays in the main loop, dispatches to active handler
- `RunContext` removed
- `RunOutcome` simplified to `ServerClosed | Detach`
- `subscribe()`, `request_refresh()`, `fetch_list()` become inline phases of the main loop
- SIGWINCH debounce timer added to select loop
- pty_id filtering consolidated into the main loop's dispatch

### `cell.rs`:
- `async fn run(ctx)` replaced by `CellHandler` struct implementing `RenderModeHandler`
- State moves from local variables to struct fields
- All `resp_rx.message()` matching removed
- All `tokio::select!` removed
- All stdout writing removed — fills `out: &mut Vec<u8>`

### `raw.rs`:
- Same transformation as `cell.rs` but simpler — `RawHandler` struct

### `region.rs`:
- Same transformation — `RegionHandler` struct
- `VtFilter` internals untouched

### `input.rs`:
- Unchanged. Keeps its own `cmd_tx` clone.

### `scrollback.rs`:
- Minimal changes. Still borrows `cmd_tx` and `resp_rx` during its phase, received from the main loop rather than from `RunContext`.
