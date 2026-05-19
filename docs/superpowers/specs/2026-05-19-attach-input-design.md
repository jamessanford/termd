# Attach Input: `^A` Prefix Key and PTY Switching

**Date:** 2026-05-19

## Overview

Extend the `attach` command with a tmux-style prefix key (`^A`) that enables
creating, listing, and switching between PTYs without leaving the session.
`run_stdin` and `EscapeState` move to a new `src/attach/input.rs` module.
The existing `\n~.` detach escape is preserved.

## Keybindings

| Sequence | Action |
|---|---|
| `^A ^A` | Send literal `^A` to PTY |
| `^A c` | Create a new PTY and switch to it |
| `^A "` | Show interactive PTY list; select to switch, Esc to cancel |
| `^A <space>` | Switch to next PTY (wraps) |
| `^A 0`–`^A 9` | Switch to 0th–9th PTY in list |
| `^A d` | Detach and exit (same as `\n~.`) |
| `\n~.` | Detach and exit (unchanged) |

## `src/attach/input.rs` (new file)

Moves from `mod.rs`: `EscapeState`, `run_stdin`.

### `InputAction`

```rust
pub(super) enum InputAction {
    Detach,
    Create,
    SwitchNext,
    SwitchIndex(u8),   // 0–9
    ShowList,
}
```

### `EscapeState`

```rust
enum EscapeState {
    Normal,
    AfterNewline,
    AfterTilde,
    AfterCtrlA,        // new
}
```

### State transitions

| From | Byte | To | Effect |
|---|---|---|---|
| Any | `\x01` (`^A`) | `AfterCtrlA` | flush pending `to_send` |
| `AfterCtrlA` | `\x01` | `Normal` | push `\x01` to send (literal `^A`) |
| `AfterCtrlA` | `c` | exit | send `Create` action |
| `AfterCtrlA` | `"` | exit | send `ShowList` action |
| `AfterCtrlA` | `' '` | exit | send `SwitchNext` action |
| `AfterCtrlA` | `0`–`9` | exit | send `SwitchIndex(n)` action |
| `AfterCtrlA` | `d` | exit | send `Detach` action |
| `AfterCtrlA` | other | `Normal` | push `\x01` + byte to send |
| `AfterTilde` | `.` | exit | flush, send `Detach` action |
| (all other `AfterTilde`/`AfterNewline` transitions unchanged) | | | |

When an action is sent the task returns immediately; the session loop takes over.

### Signature

```rust
pub(super) async fn run_stdin(
    cmd_tx:    mpsc::Sender<TerminalCommand>,
    action_tx: mpsc::Sender<InputAction>,
    pty_id:    String,
)
```

`shutdown_tx: oneshot::Sender<()>` is removed; `Detach` is now an `InputAction`.

## Data structure changes (`mod.rs`)

### `RunContext`

Replace `shutdown_rx: oneshot::Receiver<()>` with:

```rust
pub action_rx: mpsc::Receiver<InputAction>,
```

All other fields unchanged. `refresh_bytes` and `buffered` in the context
returned from a render mode are always `vec![]` — they are stale after initial
display and the session loop does a fresh subscribe+refresh before the next
render pass.

### `RunOutcome`

```rust
pub(super) enum RunOutcome {
    ServerClosed,
    FallbackToCell(RunContext),
    Action(InputAction, RunContext),   // replaces ClientDisconnected
}
```

If `action_rx` closes without a value (stdin EOF), render modes produce
`Action(InputAction::Detach, ctx)`.

## Render mode changes (all four modes)

Mechanical, identical in each file:

1. Destructure `action_rx` explicitly instead of `shutdown_rx`; include `item`
   explicitly (not via `..`) so it can be returned.
2. Replace the `shutdown_rx` select arm:

```rust
// before
_ = &mut shutdown_rx => { break; }

// after
action = action_rx.recv() => {
    let action = action.unwrap_or(InputAction::Detach);
    return Ok(RunOutcome::Action(action, RunContext {
        resp_rx, cmd_tx, pty_id, item,
        refresh_gen, refresh_bytes: vec![], buffered: vec![],
        action_rx,
    }));
}
```

`region.rs` already mutates `item` for resize events and includes it in
`FallbackToCell` — same pattern, just add the `action_rx` arm and remove
`shutdown_rx`.

## Session loop (`run()` in `mod.rs`)

Extract a helper:

```rust
async fn subscribe_and_refresh(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<TerminalResponse>,
    pty_id:  &str,
) -> Result<(u64, Vec<u8>, Vec<(u64, Vec<u8>)>)>
```

Sends `Subscribe`, drains until `CommandResponse { success }`, sends
`Refresh`, drains until `RefreshResponse`, buffering intermediate `Stream`
chunks. Returns `(refresh_gen, refresh_bytes, buffered)`.

`item: PtyItem` (used by cell/formatter/region for cols/rows) is tracked
separately as `current_item` in the session loop, initialized from the `item`
argument to `run()`, and updated from `pty_list` or `CreateResponse` when
switching PTYs.

`run()` becomes a `'session: loop` with a `should_subscribe: bool` flag
(default `true`; set to `false` for ShowList cancel to avoid re-subscribing
to an already-subscribed PTY):

```
let mut should_subscribe = true;
loop {
    if should_subscribe {
        subscribe(cmd_tx, resp_rx, current_pty_id)?;
    }
    should_subscribe = true;
    (refresh_gen, refresh_bytes, buffered) = refresh(cmd_tx, resp_rx, current_pty_id)?;
    let (action_tx, action_rx) = mpsc::channel(4);
    spawn run_stdin(cmd_tx.clone(), action_tx, current_pty_id.clone());
    // render dispatch loop (unchanged inner structure)
    input_task.abort();
    match outcome {
        ServerClosed                → break
        Action(Detach, _)           → break
        Action(Create, ctx)         → create_and_switch(...)
        Action(SwitchNext, ctx)     → switch(next_pty(...))
        Action(SwitchIndex(n), ctx) → switch(nth_pty(n, ...))
        Action(ShowList, ctx)       → show_list(...) // may set should_subscribe = false on cancel
    }
    // continue → subscribe (if needed) + refresh for current_pty_id
}
```

Local state in the loop: `current_pty_id: String`, `pty_list: Vec<PtyItem>`.

### `^A c` — Create

1. Send `CreateRequest { cols, rows }` (TIOCGWINSZ for current terminal size).
2. Read `CreateResponse` from `resp_rx` → new `pty_id`.
3. Send `Unsubscribe { pty_id: old_id }` (fire-and-forget).
4. Set `current_pty_id = new_pty_id`, continue loop.

### `^A <space>` / `^A 0–9` — Switch by index

1. If `pty_list` is empty: send `List`, read `ListResponse`, update list.
2. Find target: next-after-current (wrapping) or nth entry.
3. Send `Unsubscribe` old, set `current_pty_id`, continue loop.

### `^A "` — Interactive list UI

1. Send `List`, read `ListResponse` from `resp_rx`, update `pty_list`.
2. Clear screen (`\x1b[2J\x1b[H`).
3. Print each PTY as one line: `ID  title  COLSxROWS`. Highlight current
   selection with reverse video (`\x1b[7m … \x1b[0m`).
4. Read stdin directly (input task is already aborted):
   - `\x1b[A` / `\x1b[B` (3-byte up/down arrow) → move highlight, redraw list.
   - `\r` / `\n` → select highlighted entry.
   - `\x1b` not followed by `[A` or `[B` → cancel.
5. Arrow-key disambiguation: after reading `\x1b`, attempt to read two more
   bytes within 50 ms; if they don't arrive or are not `[A`/`[B`, treat as
   bare escape (cancel).
6. **Select**: send `Unsubscribe` old, set `current_pty_id`, continue loop.
7. **Cancel**: send `Refresh` for current PTY, use response as new
   `refresh_bytes`; continue loop with `current_pty_id` unchanged (effectively
   a no-op switch that redraws).

## Files changed

| File | Change |
|---|---|
| `src/attach/input.rs` | New — `InputAction`, `EscapeState`, `run_stdin` |
| `src/attach/mod.rs` | `RunContext`/`RunOutcome` fields; session loop; `subscribe_and_refresh`; list UI |
| `src/attach/cell.rs` | Swap `shutdown_rx` → `action_rx`; explicit `item` in destructure |
| `src/attach/formatter.rs` | Same |
| `src/attach/raw.rs` | Same |
| `src/attach/region.rs` | Same + `action_rx` in `FallbackToCell` context |
