# Region Mode Fallback to Cell Mode — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When region mode can no longer handle the current terminal dimensions mid-session (server grew larger than client, or client shrank below server), fall back gracefully to cell mode without disconnecting.

**Architecture:** Add a `RunOutcome` enum to `mod.rs`. All four runners return `Result<RunOutcome>` instead of `Result<bool>`. The dispatch in `mod.rs::run` becomes a small loop that re-dispatches on `FallbackToCell(RunContext)`. Region mode keeps the fields it previously dropped (`cmd_tx`, `pty_id`, `item`) so it can reconstruct a live `RunContext` on fallback.

**Tech Stack:** Rust, Tokio async, existing `src/attach/` module structure.

---

## File Map

| File | Change |
|---|---|
| `src/attach/mod.rs` | Add `RunOutcome` enum; replace one-shot dispatch `match` with a loop |
| `src/attach/cell.rs` | Return type `Result<bool>` → `Result<RunOutcome>`; update two return sites |
| `src/attach/formatter.rs` | Same as cell.rs |
| `src/attach/raw.rs` | Same as cell.rs |
| `src/attach/region.rs` | Return type change; fix destructure; track `item` updates; two new fallback break sites; replace `server_closed` flag with `Option<RunContext>` |

---

### Task 1: Add `RunOutcome` to `mod.rs` and update the dispatch loop

**Files:**
- Modify: `src/attach/mod.rs`

- [ ] **Step 1: Add the `RunOutcome` enum**

Insert after the `RunContext` struct definition (after line 29, before `use termd::proto`):

```rust
/// Outcome returned by every render-mode runner.
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

- [ ] **Step 2: Replace the one-shot dispatch `match` with a loop**

Find this block in `pub async fn run` (currently lines 260–265):

```rust
    let server_closed = match mode {
        RenderMode::Cell      => cell::run(ctx).await?,
        RenderMode::Formatter => formatter::run(ctx).await?,
        RenderMode::Raw       => raw::run(ctx).await?,
        RenderMode::Region    => region::run(ctx).await?,
    };

    stdin_task.abort();
    drop(_guard);
    if server_closed {
        eprintln!("[Connection closed]");
    }
```

Replace with:

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

    stdin_task.abort();
    drop(_guard);
    if matches!(outcome, RunOutcome::ServerClosed) {
        eprintln!("[Connection closed]");
    }
```

- [ ] **Step 3: Verify it does not yet compile (runners still return `Result<bool>`)**

```bash
cargo build 2>&1 | grep "error"
```

Expected: type mismatch errors on `cell::run`, `formatter::run`, `raw::run`, `region::run` — confirming the dispatch loop is wired up and waiting for the runners to be updated.

---

### Task 2: Update `cell.rs`, `formatter.rs`, and `raw.rs` return types

**Files:**
- Modify: `src/attach/cell.rs`
- Modify: `src/attach/formatter.rs`
- Modify: `src/attach/raw.rs`

All three files follow an identical pattern: change the function signature and update two return sites.

- [ ] **Step 1: Update `cell.rs`**

Change the signature (line 15):
```rust
// Before
pub(super) async fn run(ctx: super::RunContext) -> Result<bool> {
// After
pub(super) async fn run(ctx: super::RunContext) -> Result<super::RunOutcome> {
```

Find the two return sites and update them. The `server_closed = true; break` path (stream error / Closed metadata) and the final `Ok(server_closed)` (line 84):

```rust
// Before (line 84)
    Ok(server_closed)
// After
    Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })
```

- [ ] **Step 2: Update `formatter.rs`**

Same two changes — signature (line 16) and final return (line 82):

```rust
// Signature
pub(super) async fn run(ctx: super::RunContext) -> Result<super::RunOutcome> {

// Final return (line 82)
    Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })
```

- [ ] **Step 3: Update `raw.rs`**

Same two changes — signature (line 12) and final return (line 71):

```rust
// Signature
pub(super) async fn run(ctx: super::RunContext) -> Result<super::RunOutcome> {

// Final return (line 71)
    Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })
```

- [ ] **Step 4: Verify only `region.rs` errors remain**

```bash
cargo build 2>&1 | grep "error"
```

Expected: only errors mentioning `region.rs` — cell, formatter, and raw are satisfied.

- [ ] **Step 5: Run existing tests to confirm no regressions**

```bash
cargo test 2>&1 | tail -20
```

Expected: all existing tests pass (cell and formatter unit tests in their respective modules).

- [ ] **Step 6: Commit**

```bash
git add src/attach/mod.rs src/attach/cell.rs src/attach/formatter.rs src/attach/raw.rs
git commit -m "feat(attach): add RunOutcome enum and update dispatch loop and simple runners"
```

---

### Task 3: Update `region.rs` — fix destructure, update item tracking, add fallback triggers

**Files:**
- Modify: `src/attach/region.rs`

- [ ] **Step 1: Update the function signature**

Line 261 of `region.rs`:
```rust
// Before
pub(super) async fn run(ctx: super::RunContext) -> Result<bool> {
// After
pub(super) async fn run(ctx: super::RunContext) -> Result<super::RunOutcome> {
```

- [ ] **Step 2: Update the initial too-small fallback at the top of the function**

The early return at lines 266–272 calls `super::cell::run(ctx).await` which now returns `Result<super::RunOutcome>` — this line needs no code change since the return type already matches. Verify it still reads:

```rust
        return super::cell::run(ctx).await;
```

No edit needed; just confirm it's there.

- [ ] **Step 3: Fix the `RunContext` destructure**

Lines 274–278 currently drop `cmd_tx`, `pty_id`, and `item` via `..`. Replace the entire destructure:

```rust
// Before
    let super::RunContext {
        mut resp_rx, refresh_gen,
        refresh_bytes, buffered, mut shutdown_rx, ..
    } = ctx;

// After
    let super::RunContext {
        mut resp_rx, cmd_tx, pty_id, mut item,
        refresh_gen, refresh_bytes, buffered, mut shutdown_rx,
    } = ctx;
```

`item` is `mut` so its `cols`/`rows` fields can be updated when a server resize arrives.

- [ ] **Step 4: Replace `server_closed` flag with `fallback_ctx` option**

Find:
```rust
    let mut server_closed = false;
```

Replace with:
```rust
    let mut server_closed = false;
    // Set to Some when region mode needs to hand off to cell mode.
    let mut fallback_ctx: Option<super::RunContext> = None;
```

- [ ] **Step 5: Add server-resize fallback trigger in the metadata handler**

Find the metadata Resize arm inside the loop (currently around line 313–320):

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        filter.update_region(mi.rows, mi.cols);
                                        out.extend_from_slice(b"\x1b[2J");
                                        filter.emit_region_setup(&mut out);
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                server_closed = true;
                                break;
                            }
                        }
```

Replace with:

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        // Keep item in sync so a subsequent fallback
                                        // hands cell mode the current server dimensions.
                                        item.cols = mi.cols;
                                        item.rows = mi.rows;
                                        filter.update_region(mi.rows, mi.cols);
                                        if mi.cols > client_cols || mi.rows > client_rows {
                                            eprintln!(
                                                "[region: server resized to ({}x{}), larger than \
                                                 client ({}x{}); switching to cell mode]",
                                                mi.cols, mi.rows, client_cols, client_rows
                                            );
                                            fallback_ctx = Some(super::RunContext {
                                                resp_rx, cmd_tx, pty_id, item,
                                                refresh_gen: 0,
                                                refresh_bytes: vec![],
                                                buffered: vec![],
                                                shutdown_rx,
                                            });
                                            break;
                                        }
                                        out.extend_from_slice(b"\x1b[2J");
                                        filter.emit_region_setup(&mut out);
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                server_closed = true;
                                break;
                            }
                        }
```

Note: assigning `resp_rx`, `cmd_tx`, etc. into the `RunContext` struct moves them, so after this `break` the loop can't use them — which is fine since we're breaking immediately.

- [ ] **Step 6: Add SIGWINCH fallback trigger**

Find the SIGWINCH arm (currently around line 333–340):

```rust
            _ = sigwinch.recv() => {
                let (new_cols, new_rows) = get_terminal_size();
                if new_rows < filter.server_rows || new_cols < filter.server_cols {
                    eprintln!("[region: client shrank below server PTY size, display may be incomplete]");
                }
                filter.update_client_size(new_rows, new_cols);
                filter.emit_region_setup(&mut out);
            }
```

Replace with:

```rust
            _ = sigwinch.recv() => {
                let (new_cols, new_rows) = get_terminal_size();
                if new_rows < filter.server_rows || new_cols < filter.server_cols {
                    eprintln!(
                        "[region: client shrank to ({}x{}), smaller than server ({}x{}); \
                         switching to cell mode]",
                        new_cols, new_rows, filter.server_cols, filter.server_rows
                    );
                    fallback_ctx = Some(super::RunContext {
                        resp_rx, cmd_tx, pty_id, item,
                        refresh_gen: 0,
                        refresh_bytes: vec![],
                        buffered: vec![],
                        shutdown_rx,
                    });
                    break;
                }
                filter.update_client_size(new_rows, new_cols);
                filter.emit_region_setup(&mut out);
            }
```

- [ ] **Step 7: Update the return at the end of the function**

After the loop, the cleanup block (restore margins) runs unconditionally. Find the existing final `Ok(server_closed)` (currently line 355) and replace it:

```rust
// Before
    Ok(server_closed)

// After
    if let Some(ctx) = fallback_ctx {
        return Ok(super::RunOutcome::FallbackToCell(ctx));
    }
    Ok(if server_closed { super::RunOutcome::ServerClosed } else { super::RunOutcome::ClientDisconnected })
```

- [ ] **Step 8: Verify full compilation**

```bash
cargo build 2>&1
```

Expected: no errors.

- [ ] **Step 9: Run all tests**

```bash
cargo test 2>&1
```

Expected: all tests pass including the existing `VtFilter` unit tests in `region.rs`.

- [ ] **Step 10: Commit**

```bash
git add src/attach/region.rs
git commit -m "feat(attach): region mode falls back to cell mode on mid-session resize"
```

---

### Task 4: Smoke test the fallback paths

The fallback logic lives in async event-loop code that is not unit-testable without a live gRPC stream. Verify the two paths manually:

- [ ] **Path 1 — server grows larger than client:**
  1. Start `termd` (`cargo run -- start` or the release binary).
  2. In a second terminal sized to e.g. 120×40, attach with `--mode region` to a PTY that was created at 80×24.
  3. From another session, resize the server PTY to 130×24 (wider than the client) using `termd resize`.
  4. Expected: client prints `[region: server resized to (130x24), larger than client (120x40); switching to cell mode]` and continues displaying output in cell mode.

- [ ] **Path 2 — client shrinks below server:**
  1. Attach as above (client 120×40, server 80×24).
  2. Resize the client terminal window to be narrower than 80 columns.
  3. Expected: client prints `[region: client shrank to (…), smaller than server (…); switching to cell mode]` and continues in cell mode.

- [ ] **Final commit if smoke tests pass**

No additional code changes expected; this step just confirms the binary works end-to-end.
