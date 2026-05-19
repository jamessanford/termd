# Attach Input: `^A` Prefix Key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `^A` prefix key to `attach` that enables creating, listing, and switching between PTYs from within a live session, by evolving `run()` into a session manager loop that reuses the single gRPC bidi stream.

**Architecture:** Extract the input state machine to `src/attach/input.rs` with a pure `process_byte` function (unit tested). Replace `shutdown_rx: oneshot::Receiver<()>` in `RunContext` with `action_rx: mpsc::Receiver<InputAction>`. Render modes return `RunOutcome::Action(InputAction, RunContext)` so the outer `run()` can handle PTY switching and respawn the input task.

**Tech Stack:** Rust, tokio, tonic gRPC, nix/libc

---

## File Map

| File | Change |
|---|---|
| `src/attach/input.rs` | **Create** — `EscapeState`, `process_byte` (unit tested), `run_stdin` |
| `src/attach/mod.rs` | **Modify** — add `InputAction`; update `RunContext`/`RunOutcome`; rewrite `run()` as session loop with helpers; add `get_terminal_size` |
| `src/attach/cell.rs` | **Modify** — swap `shutdown_rx` → `action_rx` in destructure + select |
| `src/attach/formatter.rs` | **Modify** — same |
| `src/attach/raw.rs` | **Modify** — same + add `item` to explicit destructure |
| `src/attach/region.rs` | **Modify** — same + `action_rx` in `FallbackToCell` contexts; remove `get_terminal_size` (moves to `mod.rs`) |

---

### Task 1: Create `src/attach/input.rs` and add `InputAction` to `mod.rs`

**Files:**
- Create: `src/attach/input.rs`
- Modify: `src/attach/mod.rs` (add `mod input;`, add `InputAction`, remove `EscapeState` + `run_stdin`, remove `oneshot` import)

- [ ] **Step 1: Add `InputAction` enum to `src/attach/mod.rs`**

In `src/attach/mod.rs`, after the existing `use` blocks at the top, insert:

```rust
mod input;

pub(super) enum InputAction {
    Detach,
    Create,
    SwitchNext,
    SwitchIndex(u8),
    ShowList,
}
```

Also remove the `use tokio::sync::oneshot;` line (or any standalone `oneshot` import — it was used for `shutdown_tx/rx` which no longer exist at the `mod.rs` level).

- [ ] **Step 2: Write the failing tests in `src/attach/input.rs`**

Create the file. Write the tests first; `process_byte` does not exist yet so these will fail to compile:

```rust
use tokio::sync::mpsc;
use termd::proto::{terminal_command::Command, TerminalCommand, WriteRequest};

#[derive(Clone, Copy)]
pub(super) enum EscapeState {
    Normal,
    AfterNewline,
    AfterTilde,
    AfterCtrlA,
}

// process_byte — not yet implemented, tests below will fail to compile until Step 3

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::InputAction;

    fn run_from(initial: EscapeState, bytes: &[u8]) -> (EscapeState, Vec<u8>, Option<InputAction>) {
        let mut state = initial;
        let mut to_send = Vec::new();
        let mut action = None;
        for &byte in bytes {
            if let Some(a) = process_byte(&mut state, byte, &mut to_send) {
                action = Some(a);
                break;
            }
        }
        (state, to_send, action)
    }

    #[test]
    fn ctrl_a_c_creates() {
        let (_, bytes, action) = run_from(EscapeState::Normal, &[0x01, b'c']);
        assert!(matches!(action, Some(InputAction::Create)));
        assert!(bytes.is_empty());
    }

    #[test]
    fn ctrl_a_ctrl_a_sends_literal() {
        let (state, bytes, action) = run_from(EscapeState::Normal, &[0x01, 0x01]);
        assert!(action.is_none());
        assert_eq!(bytes, &[0x01]);
        assert!(matches!(state, EscapeState::Normal));
    }

    #[test]
    fn ctrl_a_d_detaches() {
        let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b'd']);
        assert!(matches!(action, Some(InputAction::Detach)));
    }

    #[test]
    fn ctrl_a_space_switches_next() {
        let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b' ']);
        assert!(matches!(action, Some(InputAction::SwitchNext)));
    }

    #[test]
    fn ctrl_a_digit_switches_index() {
        for n in 0u8..=9 {
            let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b'0' + n]);
            assert!(
                matches!(action, Some(InputAction::SwitchIndex(i)) if i == n),
                "^A {} should produce SwitchIndex({})", n, n,
            );
        }
    }

    #[test]
    fn ctrl_a_quote_shows_list() {
        let (_, _, action) = run_from(EscapeState::Normal, &[0x01, b'"']);
        assert!(matches!(action, Some(InputAction::ShowList)));
    }

    #[test]
    fn ctrl_a_unknown_passes_through() {
        let (state, bytes, action) = run_from(EscapeState::Normal, &[0x01, b'x']);
        assert!(action.is_none());
        assert_eq!(bytes, &[0x01, b'x']);
        assert!(matches!(state, EscapeState::Normal));
    }

    #[test]
    fn tilde_dot_detaches() {
        let (_, bytes, action) = run_from(EscapeState::AfterNewline, &[b'~', b'.']);
        assert!(matches!(action, Some(InputAction::Detach)));
        assert!(bytes.is_empty());
    }

    #[test]
    fn tilde_other_passes_through() {
        let (_, bytes, action) = run_from(EscapeState::AfterNewline, &[b'~', b'x']);
        assert!(action.is_none());
        assert_eq!(bytes, &[b'~', b'x']);
    }

    #[test]
    fn ctrl_a_mid_stream_preserves_prior_bytes() {
        // Bytes before ^A stay in to_send; only the action fires.
        let (_, bytes, action) = run_from(EscapeState::Normal, &[b'h', b'i', 0x01, b'c']);
        assert!(matches!(action, Some(InputAction::Create)));
        assert_eq!(bytes, &[b'h', b'i']);
    }

    #[test]
    fn ctrl_a_works_from_after_newline_state() {
        let (_, _, action) = run_from(EscapeState::AfterNewline, &[0x01, b'c']);
        assert!(matches!(action, Some(InputAction::Create)));
    }
}
```

- [ ] **Step 3: Implement `process_byte` in `src/attach/input.rs`**

Add after the `EscapeState` enum:

```rust
pub(super) fn process_byte(
    state:   &mut EscapeState,
    byte:    u8,
    to_send: &mut Vec<u8>,
) -> Option<super::InputAction> {
    use super::InputAction;
    match state {
        EscapeState::Normal => match byte {
            0x01 => { *state = EscapeState::AfterCtrlA; None }
            b'\r' | b'\n' => { to_send.push(byte); *state = EscapeState::AfterNewline; None }
            _ => { to_send.push(byte); None }
        },
        EscapeState::AfterNewline => match byte {
            0x01 => { *state = EscapeState::AfterCtrlA; None }
            b'~' => { *state = EscapeState::AfterTilde; None }
            b'\r' | b'\n' => { to_send.push(byte); None }
            _ => { to_send.push(byte); *state = EscapeState::Normal; None }
        },
        EscapeState::AfterTilde => match byte {
            b'.' => Some(InputAction::Detach),
            b'\r' | b'\n' => {
                to_send.push(b'~');
                to_send.push(byte);
                *state = EscapeState::AfterNewline;
                None
            }
            _ => {
                to_send.push(b'~');
                to_send.push(byte);
                *state = EscapeState::Normal;
                None
            }
        },
        EscapeState::AfterCtrlA => match byte {
            0x01     => { to_send.push(0x01); *state = EscapeState::Normal; None }
            b'c'     => Some(InputAction::Create),
            b'"'     => Some(InputAction::ShowList),
            b' '     => Some(InputAction::SwitchNext),
            b'd'     => Some(InputAction::Detach),
            b'0'..=b'9' => Some(InputAction::SwitchIndex(byte - b'0')),
            _ => {
                to_send.push(0x01);
                to_send.push(byte);
                *state = EscapeState::Normal;
                None
            }
        },
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```
cargo test attach::input::tests
```

Expected: all 11 tests pass.

- [ ] **Step 5: Add `run_stdin` to `src/attach/input.rs`**

Append after `process_byte`:

```rust
pub(super) async fn run_stdin(
    cmd_tx:    mpsc::Sender<TerminalCommand>,
    action_tx: mpsc::Sender<super::InputAction>,
    pty_id:    String,
) {
    use tokio::io::AsyncReadExt;
    let mut stdin = tokio::io::stdin();
    let mut state = EscapeState::AfterNewline;
    let mut buf = [0u8; 256];

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let mut to_send: Vec<u8> = Vec::new();
        for &byte in &buf[..n] {
            if let Some(action) = process_byte(&mut state, byte, &mut to_send) {
                if !to_send.is_empty() {
                    let _ = cmd_tx.send(TerminalCommand {
                        command: Some(Command::Write(WriteRequest {
                            pty_id: pty_id.clone(),
                            data: to_send,
                        })),
                    }).await;
                }
                let _ = action_tx.send(action).await;
                return;
            }
        }
        if !to_send.is_empty() {
            if cmd_tx.send(TerminalCommand {
                command: Some(Command::Write(WriteRequest {
                    pty_id: pty_id.clone(),
                    data: to_send,
                })),
            }).await.is_err() {
                break;
            }
        }
    }
}
```

- [ ] **Step 6: Remove old `EscapeState` and `run_stdin` from `src/attach/mod.rs`**

Delete the `EscapeState` enum (lines ~121–125) and `run_stdin` function (lines ~127–197) from `mod.rs`. These have moved to `input.rs`.

- [ ] **Step 7: Run tests to verify nothing broke**

```
cargo test attach::input::tests
```

Expected: same 11 tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/attach/input.rs src/attach/mod.rs
git commit -m "feat(attach): add input.rs with ^A state machine and InputAction"
```

---

### Task 2: Update `RunContext`, `RunOutcome`, and all four render modes

**Files:**
- Modify: `src/attach/mod.rs` (update `RunContext`, `RunOutcome`)
- Modify: `src/attach/cell.rs`
- Modify: `src/attach/formatter.rs`
- Modify: `src/attach/raw.rs`
- Modify: `src/attach/region.rs`

Note: steps 1 and 2 introduce compile errors that are fixed by steps 3–7. Run `cargo check` only at step 7.

- [ ] **Step 1: Update `RunContext` in `src/attach/mod.rs`**

Replace the `shutdown_rx` field:

```rust
// BEFORE
pub shutdown_rx: tokio::sync::oneshot::Receiver<()>,

// AFTER
pub action_rx: tokio::sync::mpsc::Receiver<InputAction>,
```

- [ ] **Step 2: Update `RunOutcome` in `src/attach/mod.rs`**

```rust
// BEFORE
pub(super) enum RunOutcome {
    ServerClosed,
    ClientDisconnected,
    FallbackToCell(RunContext),
}

// AFTER
pub(super) enum RunOutcome {
    ServerClosed,
    FallbackToCell(RunContext),
    Action(InputAction, RunContext),
}
```

- [ ] **Step 3: Update `src/attach/cell.rs`**

Line 16 — add `cmd_tx` and `pty_id` to the destructure (they're needed for the returned `RunContext`), replace `shutdown_rx` with `action_rx`:

```rust
// BEFORE (line 16)
let super::RunContext { mut resp_rx, item, refresh_gen, refresh_bytes, buffered, mut shutdown_rx, .. } = ctx;

// AFTER
let super::RunContext { mut resp_rx, cmd_tx, pty_id, item, refresh_gen, refresh_bytes, buffered, mut action_rx } = ctx;
```

Line 73 — replace `shutdown_rx` select arm with `action_rx` arm that returns early:

```rust
// BEFORE (line 73)
_ = &mut shutdown_rx => break,

// AFTER
action = action_rx.recv() => {
    let action = action.unwrap_or(super::InputAction::Detach);
    return Ok(super::RunOutcome::Action(action, super::RunContext {
        resp_rx, cmd_tx, pty_id, item,
        refresh_gen, refresh_bytes: vec![], buffered: vec![],
        action_rx,
    }));
}
```

Line 84 — replace the trailing return (the loop can now only exit via `ServerClosed` or stdout failure):

```rust
// BEFORE (line 84)
Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })

// AFTER
Ok(super::RunOutcome::ServerClosed)
```

- [ ] **Step 4: Update `src/attach/formatter.rs`**

Identical changes to `cell.rs`. Line 17, line 71, line 82:

```rust
// Line 17 BEFORE
let super::RunContext { mut resp_rx, item, refresh_gen, refresh_bytes, buffered, mut shutdown_rx, .. } = ctx;

// Line 17 AFTER
let super::RunContext { mut resp_rx, cmd_tx, pty_id, item, refresh_gen, refresh_bytes, buffered, mut action_rx } = ctx;
```

```rust
// Line 71 BEFORE
_ = &mut shutdown_rx => break,

// Line 71 AFTER
action = action_rx.recv() => {
    let action = action.unwrap_or(super::InputAction::Detach);
    return Ok(super::RunOutcome::Action(action, super::RunContext {
        resp_rx, cmd_tx, pty_id, item,
        refresh_gen, refresh_bytes: vec![], buffered: vec![],
        action_rx,
    }));
}
```

```rust
// Line 82 BEFORE
Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })

// Line 82 AFTER
Ok(super::RunOutcome::ServerClosed)
```

- [ ] **Step 5: Update `src/attach/raw.rs`**

Line 13 — add `mut item` and `action_rx`, remove `shutdown_rx` and `..`:

```rust
// BEFORE (line 13)
let super::RunContext { mut resp_rx, cmd_tx, pty_id, mut refresh_gen, refresh_bytes, buffered, mut shutdown_rx, .. } = ctx;

// AFTER
let super::RunContext { mut resp_rx, cmd_tx, pty_id, item, mut refresh_gen, refresh_bytes, buffered, mut action_rx } = ctx;
```

Line 61 — replace `shutdown_rx` arm:

```rust
// BEFORE (line 61)
_ = &mut shutdown_rx => break,

// AFTER
action = action_rx.recv() => {
    let action = action.unwrap_or(super::InputAction::Detach);
    return Ok(super::RunOutcome::Action(action, super::RunContext {
        resp_rx, cmd_tx, pty_id, item,
        refresh_gen, refresh_bytes: vec![], buffered: vec![],
        action_rx,
    }));
}
```

Line 71 — replace trailing return:

```rust
// BEFORE (line 71)
Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })

// AFTER
Ok(super::RunOutcome::ServerClosed)
```

- [ ] **Step 6: Update `src/attach/region.rs`**

Line 286 — replace `shutdown_rx` with `action_rx` in the full destructure:

```rust
// BEFORE (lines 284–287)
let super::RunContext {
    mut resp_rx, cmd_tx, pty_id, mut item,
    refresh_gen, refresh_bytes, buffered, mut shutdown_rx,
} = ctx;

// AFTER
let super::RunContext {
    mut resp_rx, cmd_tx, pty_id, mut item,
    refresh_gen, refresh_bytes, buffered, mut action_rx,
} = ctx;
```

Lines 340–346 — update the first `FallbackToCell` context (Metadata resize overflow):

```rust
// BEFORE (lines 340–346)
fallback_ctx = Some(super::RunContext {
    resp_rx, cmd_tx, pty_id, item,
    refresh_gen: 0,
    refresh_bytes: vec![],
    buffered: vec![],
    shutdown_rx,
});

// AFTER
fallback_ctx = Some(super::RunContext {
    resp_rx, cmd_tx, pty_id, item,
    refresh_gen: 0,
    refresh_bytes: vec![],
    buffered: vec![],
    action_rx,
});
```

Lines 363 — replace `shutdown_rx` select arm. The action arm must also emit the region cleanup sequence before returning, since `region::run` normally does that after the loop:

```rust
// BEFORE (line 363)
_ = &mut shutdown_rx => break,

// AFTER
action = action_rx.recv() => {
    let action = action.unwrap_or(super::InputAction::Detach);
    // Restore client terminal margins before handing back context.
    let _ = stdout.write_all(b"\x1b[r").await;
    if filter.declrmm_active {
        let _ = stdout.write_all(b"\x1b[?69l").await;
    }
    let _ = stdout.flush().await;
    return Ok(super::RunOutcome::Action(action, super::RunContext {
        resp_rx, cmd_tx, pty_id, item,
        refresh_gen: current_refresh_gen,
        refresh_bytes: vec![], buffered: vec![],
        action_rx,
    }));
}
```

Lines 372–378 — update the second `FallbackToCell` context (SIGWINCH shrink):

```rust
// BEFORE (lines 372–378)
fallback_ctx = Some(super::RunContext {
    resp_rx, cmd_tx, pty_id, item,
    refresh_gen: 0,
    refresh_bytes: vec![],
    buffered: vec![],
    shutdown_rx,
});

// AFTER
fallback_ctx = Some(super::RunContext {
    resp_rx, cmd_tx, pty_id, item,
    refresh_gen: 0,
    refresh_bytes: vec![],
    buffered: vec![],
    action_rx,
});
```

Line 401 — replace trailing return:

```rust
// BEFORE (line 401)
Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })

// AFTER
Ok(if let Some(ctx) = fallback_ctx {
    super::RunOutcome::FallbackToCell(ctx)
} else {
    super::RunOutcome::ServerClosed
})
```

Wait — line 398–400 already handle `fallback_ctx` with an early return. The trailing line 401 only runs when `fallback_ctx` is `None`. So the replacement is simply:

```rust
// AFTER (line 401, only reached when fallback_ctx is None)
Ok(super::RunOutcome::ServerClosed)
```

- [ ] **Step 7: Verify compilation**

```
cargo check
```

Expected: no errors. (Tests may fail until `run()` is updated in Task 3 — that's OK, `cargo check` is enough here.)

- [ ] **Step 8: Run existing tests**

```
cargo test
```

Expected: all existing tests pass (render logic tests in `cell.rs`, `formatter.rs`, `region.rs` plus the input state machine tests).

- [ ] **Step 9: Commit**

```bash
git add src/attach/mod.rs src/attach/cell.rs src/attach/formatter.rs \
        src/attach/raw.rs src/attach/region.rs
git commit -m "refactor(attach): replace shutdown_rx with action_rx across render modes"
```

---

### Task 3: Rewrite `run()` as a session loop

**Files:**
- Modify: `src/attach/mod.rs`

This task rewrites the `run()` function and adds helper functions. The `ShowList` action is stubbed (sets `should_subscribe = false`, does nothing else) and is implemented fully in Task 4.

- [ ] **Step 1: Add imports to `src/attach/mod.rs`**

In the `use termd::proto::{...}` block, add `CreateRequest`, `ListRequest`, `UnsubscribeRequest`:

```rust
use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    CreateRequest, ListRequest, PtyItem, RefreshRequest,
    SubscribeRequest, UnsubscribeRequest,
    TerminalCommand, WriteRequest,
    StreamMetadataReason,
    terminal_service_client::TerminalServiceClient,
};
```

- [ ] **Step 2: Move `get_terminal_size` from `region.rs` to `mod.rs`**

In `src/attach/region.rs`, delete lines 265–269 (`fn get_terminal_size`).

In `src/attach/mod.rs`, add this function (accessible as `super::get_terminal_size()` from region.rs):

```rust
pub(super) fn get_terminal_size() -> (u32, u32) {
    let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws); }
    (ws.ws_col as u32, ws.ws_row as u32)
}
```

Also add `use libc;` (or check that `libc` is already in scope — region.rs already uses it, so the crate is available). In `mod.rs`, add at the top of the file: `use libc;` if not already present.

Update `region.rs` call site (line 272, 365) from `get_terminal_size()` to `super::get_terminal_size()`.

- [ ] **Step 3: Add helper functions to `src/attach/mod.rs`**

Add these five helpers anywhere before `run()` in `mod.rs`:

```rust
async fn subscribe(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  &str,
) -> anyhow::Result<()> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.to_owned() })),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during subscribe"),
            Some(r) => match r.response {
                Some(Response::Command(c)) if c.success => return Ok(()),
                Some(Response::Command(c)) => {
                    anyhow::bail!("subscribe failed: {}", c.error.unwrap_or_default())
                }
                _ => {}
            }
        }
    }
}

async fn request_refresh(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  &str,
) -> anyhow::Result<(u64, Vec<u8>, Vec<(u64, Vec<u8>)>)> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.to_owned() })),
    }).await?;
    let mut buffered: Vec<(u64, Vec<u8>)> = Vec::new();
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during refresh"),
            Some(r) => match r.response {
                Some(Response::Refresh(rf)) => return Ok((rf.generation, rf.data, buffered)),
                Some(Response::Stream(s))   => buffered.push((s.generation, s.data)),
                _ => {}
            }
        }
    }
}

async fn fetch_list(
    cmd_tx:   &mpsc::Sender<TerminalCommand>,
    resp_rx:  &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list: &mut Vec<PtyItem>,
) -> anyhow::Result<()> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::List(ListRequest {})),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during list"),
            Some(r) => match r.response {
                Some(Response::List(lr)) => { *pty_list = lr.items; return Ok(()); }
                _ => {}
            }
        }
    }
}

fn next_pty<'a>(list: &'a [PtyItem], current_id: &str) -> Option<&'a PtyItem> {
    if list.is_empty() { return None; }
    let pos = list.iter().position(|p| p.pty_id == current_id).unwrap_or(0);
    Some(&list[(pos + 1) % list.len()])
}
```

- [ ] **Step 4: Replace `run()` in `src/attach/mod.rs` with the session loop**

Delete the existing `run()` function body and replace it entirely with:

```rust
pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
    mode: RenderMode,
) -> Result<()> {
    if debug {
        return run_debug(client, item.pty_id).await;
    }

    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    let _guard = setup_raw_mode()?;

    let mut current_pty_id = item.pty_id.clone();
    let mut current_item = item;
    let mut pty_list: Vec<PtyItem> = Vec::new();
    let mut should_subscribe = true;

    'session: loop {
        if should_subscribe {
            subscribe(&cmd_tx, &mut resp_rx, &current_pty_id).await?;
        }
        should_subscribe = true;

        let (refresh_gen, refresh_bytes, buffered) =
            request_refresh(&cmd_tx, &mut resp_rx, &current_pty_id).await?;

        let (action_tx, action_rx) = mpsc::channel::<InputAction>(4);
        let input_task = tokio::spawn(input::run_stdin(
            cmd_tx.clone(),
            action_tx,
            current_pty_id.clone(),
        ));

        let ctx = RunContext {
            resp_rx,
            cmd_tx: cmd_tx.clone(),
            pty_id: current_pty_id.clone(),
            item: current_item.clone(),
            refresh_gen,
            refresh_bytes,
            buffered,
            action_rx,
        };

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

        input_task.abort();

        match outcome {
            RunOutcome::ServerClosed => {
                eprintln!("[Connection closed]");
                break 'session;
            }
            RunOutcome::FallbackToCell(_) => unreachable!(),
            RunOutcome::Action(action, ctx) => {
                resp_rx = ctx.resp_rx;
                match action {
                    InputAction::Detach => break 'session,

                    InputAction::Create => {
                        let (cols, rows) = get_terminal_size();
                        cmd_tx.send(TerminalCommand {
                            command: Some(Command::Create(CreateRequest {
                                cols, rows, command: None,
                            })),
                        }).await?;
                        'create: loop {
                            match resp_rx.message().await? {
                                None => { eprintln!("[server disconnected]"); break 'session; }
                                Some(r) => if let Some(Response::Create(cr)) = r.response {
                                    if let Some(new_item) = cr.item {
                                        let _ = cmd_tx.send(TerminalCommand {
                                            command: Some(Command::Unsubscribe(
                                                UnsubscribeRequest { pty_id: current_pty_id.clone() }
                                            )),
                                        }).await;
                                        current_pty_id = new_item.pty_id.clone();
                                        current_item = new_item;
                                        break 'create;
                                    }
                                }
                            }
                        }
                    }

                    InputAction::SwitchNext => {
                        if pty_list.is_empty() {
                            fetch_list(&cmd_tx, &mut resp_rx, &mut pty_list).await?;
                        }
                        if let Some(target) = next_pty(&pty_list, &current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                let _ = cmd_tx.send(TerminalCommand {
                                    command: Some(Command::Unsubscribe(
                                        UnsubscribeRequest { pty_id: current_pty_id.clone() }
                                    )),
                                }).await;
                                current_pty_id = target.pty_id.clone();
                                current_item = target;
                            }
                        }
                    }

                    InputAction::SwitchIndex(n) => {
                        if pty_list.is_empty() {
                            fetch_list(&cmd_tx, &mut resp_rx, &mut pty_list).await?;
                        }
                        if let Some(target) = pty_list.get(n as usize).cloned() {
                            if target.pty_id != current_pty_id {
                                let _ = cmd_tx.send(TerminalCommand {
                                    command: Some(Command::Unsubscribe(
                                        UnsubscribeRequest { pty_id: current_pty_id.clone() }
                                    )),
                                }).await;
                                current_pty_id = target.pty_id.clone();
                                current_item = target;
                            }
                        }
                    }

                    InputAction::ShowList => {
                        // Implemented in Task 4. For now: no-op, stay on current PTY.
                        should_subscribe = false;
                    }
                }
            }
        }
    }

    drop(_guard);
    Ok(())
}
```

- [ ] **Step 5: Build and verify**

```
cargo build
```

Expected: compiles cleanly. Fix any import or lifetime errors.

- [ ] **Step 6: Run tests**

```
cargo test
```

Expected: all tests pass.

- [ ] **Step 7: Integration smoke test**

Start the daemon in one terminal:
```
cargo run -- start
```

In a second terminal, create two PTYs and attach to the first:
```
cargo run -- create
cargo run -- create
cargo run -- list          # note the two pty IDs
cargo run -- attach <first-pty-id>
```

Verify:
- Type normally — text reaches the PTY.
- Press `^A c` — a new PTY is created and you're switched to it (screen clears, shell prompt appears).
- Press `^A <space>` — switches to the next PTY.
- Press `^A 0` — switches to the first PTY in the list.
- Press `^A d` — detaches and exits attach.
- `\n~.` still works for detach.
- `^A ^A` sends a literal `^A` character.

- [ ] **Step 8: Commit**

```bash
git add src/attach/mod.rs src/attach/region.rs
git commit -m "feat(attach): session loop with ^A prefix key for create/switch/detach"
```

---

### Task 4: Implement `show_list` — interactive PTY list UI

**Files:**
- Modify: `src/attach/mod.rs`

- [ ] **Step 1: Add `draw_list` helper to `src/attach/mod.rs`**

Add before `run()`:

```rust
fn draw_list(items: &[PtyItem], selected: usize) {
    use std::io::Write;
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    for (i, item) in items.iter().enumerate() {
        if i == selected { out.extend_from_slice(b"\x1b[7m"); }
        let title = if item.title.is_empty() { &item.pts_name } else { &item.title };
        let line = format!(
            " {:<16}  {:<32}  {}x{}\r\n",
            &item.pty_id[..item.pty_id.len().min(16)],
            &title[..title.len().min(32)],
            item.cols, item.rows,
        );
        out.extend_from_slice(line.as_bytes());
        if i == selected { out.extend_from_slice(b"\x1b[0m"); }
    }
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}
```

- [ ] **Step 2: Add `show_list` helper to `src/attach/mod.rs`**

Add before `run()`:

```rust
async fn show_list(
    cmd_tx:          &mpsc::Sender<TerminalCommand>,
    resp_rx:         &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list:        &mut Vec<PtyItem>,
    current_pty_id:  &str,
) -> anyhow::Result<Option<String>> {
    // Returns Some(new_pty_id) on selection, None on cancel.
    use tokio::io::AsyncReadExt;

    fetch_list(cmd_tx, resp_rx, pty_list).await?;
    if pty_list.is_empty() {
        return Ok(None);
    }

    let mut selected = pty_list
        .iter()
        .position(|p| p.pty_id == current_pty_id)
        .unwrap_or(0);

    draw_list(pty_list, selected);

    let mut stdin = tokio::io::stdin();
    let mut buf = [0u8; 8];

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => return Ok(None),
            Ok(n) => n,
        };

        match &buf[..n] {
            // Enter — select
            [b'\r'] | [b'\n'] => {
                return Ok(Some(pty_list[selected].pty_id.clone()));
            }
            // Arrow keys arrive as 3-byte ESC sequences; match the whole read
            [0x1b, b'[', b'A', ..] => {
                if selected > 0 { selected -= 1; }
                draw_list(pty_list, selected);
            }
            [0x1b, b'[', b'B', ..] => {
                if selected + 1 < pty_list.len() { selected += 1; }
                draw_list(pty_list, selected);
            }
            // Bare ESC: try to read 2 more bytes within 50 ms to rule out
            // a split arrow-key sequence. Timeout means it really was bare ESC.
            [0x1b] => {
                let mut rest = [0u8; 2];
                let is_arrow = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    stdin.read(&mut rest),
                ).await
                .ok()
                .and_then(|r| r.ok())
                .map(|n2| &rest[..n2] == b"[A" || &rest[..n2] == b"[B")
                .unwrap_or(false);

                if is_arrow {
                    if rest[1] == b'A' {
                        if selected > 0 { selected -= 1; }
                    } else {
                        if selected + 1 < pty_list.len() { selected += 1; }
                    }
                    draw_list(pty_list, selected);
                } else {
                    // Bare escape — cancel
                    return Ok(None);
                }
            }
            _ => {}
        }
    }
}
```

- [ ] **Step 3: Wire `show_list` into the session loop in `run()`**

Replace the `InputAction::ShowList` stub in the `match action` block:

```rust
// BEFORE
InputAction::ShowList => {
    // Implemented in Task 4. For now: no-op, stay on current PTY.
    should_subscribe = false;
}

// AFTER
InputAction::ShowList => {
    match show_list(&cmd_tx, &mut resp_rx, &mut pty_list, &current_pty_id).await? {
        Some(new_id) if new_id != current_pty_id => {
            let _ = cmd_tx.send(TerminalCommand {
                command: Some(Command::Unsubscribe(
                    UnsubscribeRequest { pty_id: current_pty_id.clone() }
                )),
            }).await;
            // Look up item from list so current_item has correct cols/rows.
            if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                current_item = target;
            }
            current_pty_id = new_id;
        }
        _ => {
            // Cancel or selected same PTY — skip resubscribe, just refresh.
            should_subscribe = false;
        }
    }
}
```

- [ ] **Step 4: Build**

```
cargo build
```

Expected: compiles cleanly.

- [ ] **Step 5: Integration test for `^A "`**

With daemon running and two PTYs created:
```
cargo run -- attach <first-pty-id>
```

- Press `^A "` — screen clears, list of PTYs appears with the current one highlighted.
- Press down arrow — next PTY highlights.
- Press Enter — switches to highlighted PTY (screen redraws with that PTY's content).
- Press `^A "` again — list appears.
- Press Escape — returns to current PTY (screen redraws via Refresh).
- Confirm that typing in each PTY works normally after switching.

- [ ] **Step 6: Commit**

```bash
git add src/attach/mod.rs
git commit -m "feat(attach): implement ^A \" interactive PTY list UI"
```
