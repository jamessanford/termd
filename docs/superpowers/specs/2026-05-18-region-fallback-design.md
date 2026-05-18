# Region Mode Fallback to Cell Mode

**Date:** 2026-05-18
**Status:** Approved

## Problem

`region::run` currently has no mid-session fallback path. If the server PTY grows
larger than the client terminal (server resize event), or the client terminal shrinks
below the server size (SIGWINCH), region mode continues running with incorrect
dimensions. The initial too-small check at the top of `region::run` falls back to
cell mode, but that guard is only evaluated once at attach time.

## Design

### `RunOutcome` enum

Defined in `src/attach/mod.rs`, visible to all submodules:

```rust
pub(super) enum RunOutcome {
    /// The server PTY exited or closed.
    ServerClosed,
    /// The client disconnected (stdin EOF or `~.` escape).
    ClientDisconnected,
    /// Region mode detected it can no longer handle current dimensions.
    /// `refresh_bytes` is empty — this relies on the server sending a
    /// refresh following a resize event. That holds for resize-triggered
    /// fallbacks but would not hold for arbitrary render-mode changes.
    FallbackToCell(RunContext),
}
```

All four runners (`cell`, `formatter`, `raw`, `region`) change their return type
from `Result<bool>` to `Result<RunOutcome>`. For cell, formatter, and raw the only
change is at each return site: `true` → `ServerClosed`, `false` → `ClientDisconnected`.

### mod.rs dispatch loop

The one-shot `match mode` becomes a loop that re-dispatches on `FallbackToCell`:

```rust
let mut dispatch_mode = mode;
let mut dispatch_ctx = ctx;
let outcome = loop {
    let result = match dispatch_mode {
        RenderMode::Cell      => cell::run(dispatch_ctx).await?,
        RenderMode::Formatter => formatter::run(dispatch_ctx).await?,
        RenderMode::Raw       => raw::run(dispatch_ctx).await?,
        RenderMode::Region    => region::run(dispatch_ctx).await?,
    };
    match result {
        RunOutcome::FallbackToCell(fallback_ctx) => {
            dispatch_mode = RenderMode::Cell;
            dispatch_ctx = fallback_ctx;
        }
        other => break other,
    }
};
let server_closed = matches!(outcome, RunOutcome::ServerClosed);
```

In practice the loop iterates at most twice (region → cell). The existing
`"[Connection closed]"` message is driven by `server_closed` as before.

### Changes to `region::run`

**Destructure fix** — `cmd_tx`, `pty_id`, and `item` are currently dropped via `..`.
They must be kept so the fallback `RunContext` can be constructed:

```rust
let super::RunContext {
    mut resp_rx, cmd_tx, pty_id, mut item,
    refresh_gen, refresh_bytes, buffered, mut shutdown_rx,
} = ctx;
```

**Fallback `RunContext`** — constructed with the live stream and an empty refresh
state. The server's post-resize refresh arrives naturally on the stream:

```rust
RunContext {
    resp_rx, cmd_tx, pty_id, item,  // item.cols/rows updated to new server dims
    refresh_gen: 0,
    refresh_bytes: vec![],
    buffered: vec![],
    shutdown_rx,
}
```

**Two fallback triggers:**

1. **Server resize metadata** — after `filter.update_region(mi.rows, mi.cols)`, if
   the new server dims exceed the client dims, update `item.cols`/`item.rows` to the
   new server size, emit an `eprintln!` message, and return `FallbackToCell`.

2. **SIGWINCH** — after reading the new client size, if the client is now smaller than
   the server, emit an `eprintln!` message and return `FallbackToCell` (`item` already
   holds the current server dims). This replaces the current warn-and-continue behaviour.

Both triggers print a brief message so the user understands why the render mode
switched.

## Scope

- `src/attach/mod.rs`: add `RunOutcome`, update dispatch to a loop
- `src/attach/region.rs`: fix destructure, add two fallback return sites, update return type
- `src/attach/cell.rs`: update return type (one-liner)
- `src/attach/formatter.rs`: update return type (one-liner)
- `src/attach/raw.rs`: update return type (one-liner)

No changes to server-side code, proto definitions, or other modules.

## Future

This design intentionally stops short of a fully generic `FallbackTo(RenderMode,
RunContext)` mechanism. If other render modes need fallback paths, the `RunOutcome`
enum and dispatch loop are straightforward to extend.
