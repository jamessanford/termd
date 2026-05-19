# Attach: Closed-PTY Freeze-in-Place

**Date:** 2026-05-19

## Overview

When the subscribed PTY exits, the attach client currently breaks out of the
render loop with `RunOutcome::ServerClosed`, causing the session to exit and
discarding the input task. The fix keeps the render loop running in a frozen
state so the user can still detach or switch to another PTY via `^A`.

Additionally, switching to an already-closed PTY (via `^A <space>`, `^A "`,
or `^A 0–9`) should continue to work: the session loop's unconditional
`subscribe` + `request_refresh` already handles this correctly assuming the
server responds to both operations on a closed PTY.

## Scope

Changes are confined to `src/attach/cell.rs`, `src/attach/formatter.rs`,
`src/attach/raw.rs`, and `src/attach/region.rs`. No new `RunOutcome` variants,
no changes to `mod.rs` or `input.rs`.

## Render mode changes (all four files)

Add a `pty_closed: bool` flag before the main `select!` loop, initially
`false`.

### `Metadata::Closed` arm

```rust
// before
} else if m.reason == StreamMetadataReason::Closed as i32 {
    break;
}

// after
} else if m.reason == StreamMetadataReason::Closed as i32 {
    pty_closed = true;
    eprintln!("\r\n[PTY closed]");
    // continue — don't break
}
```

### Stream-error arm (`Ok(None)` / `Err`)

Unchanged — this represents a true gRPC stream close and still returns
`RunOutcome::ServerClosed`.

### `action_rx` arm

Unchanged — returns `RunOutcome::Action(action, ctx)` as before, which the
session loop already handles for all actions (Detach, Create, SwitchNext,
SwitchIndex, ShowList).

### `Stream` data while frozen

No explicit guard needed. The existing generation check
(`if s.generation > refresh_gen`) silently drops any stale stream data that
arrives just before or after the `Closed` metadata.

## SIGWINCH while frozen

**`cell.rs` / `formatter.rs`:** Both modes repaint from their local terminal
model, which holds the final screen state. SIGWINCH repaint works correctly
in the frozen state without any change.

**`raw.rs`:** SIGWINCH sends a `Refresh` request to the server. The server
returns the frozen final screen, which gets written to stdout. Works because
the server supports refresh on closed PTYs.

**`region.rs`:** Same as raw — SIGWINCH re-sends `Refresh` and re-establishes
the scroll region from the server's response.

## Session loop (mod.rs) — no changes

`RunOutcome::ServerClosed` continues to mean a true gRPC stream disconnect.
All PTY-switch and detach paths flow through `RunOutcome::Action(...)`, which
the session loop already handles.

## Edge case: Metadata::Closed during subscribe/request_refresh

`subscribe()` and `request_refresh()` both silently drop `Metadata` events
(the former drains for `CommandResponse`, the latter buffers only `Stream`
chunks). If `Metadata::Closed` for a just-subscribed closed PTY arrives during
these phases, it is dropped. The render mode then starts with the frozen screen
bytes and `resp_rx` goes quiet. This is acceptable: `action_rx` is still
polled, so the user can still detach or switch PTYs.

## Files changed

| File | Change |
|---|---|
| `src/attach/cell.rs` | Add `pty_closed` flag; change `Metadata::Closed` arm |
| `src/attach/formatter.rs` | Same |
| `src/attach/raw.rs` | Same |
| `src/attach/region.rs` | Same |
