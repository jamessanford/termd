# Attach Scrollback Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `^A s` scrollback viewer to the attach session loop that issues `ScrollbackRequest` to the server and lets the user navigate history one row at a time with up/down/ESC.

**Architecture:** The viewer lives at the session-loop level in `mod.rs` (same pattern as `show_list`), keeping it render-mode-agnostic. A new `attach/scrollback.rs` holds all viewer logic; it writes the server-formatted VT bytes directly to stdout — no client-side libghostty needed. Each arrow key press issues one fresh `ScrollbackRequest`; ESC clears the screen and returns, after which the session loop re-requests a `RefreshResponse` to restore the active display.

**Tech Stack:** Rust, Tokio, tonic gRPC, `termd::proto::{ScrollbackRequest, ScrollbackResponse}`, `std::io::Write`, `tokio::io::AsyncReadExt`.

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/attach/mod.rs` | Modify | Add `ShowScrollback` to `InputAction`; declare `mod scrollback`; add dispatch arm |
| `src/attach/input.rs` | Modify | Add `b's'` → `ShowScrollback` in `AfterCtrlA`; add test |
| `src/attach/scrollback.rs` | **Create** | `show_scrollback`, `fetch_scrollback`, `display_page`, `format_page`, `max_row_offset` |
| `tests/integration.rs` | Modify | Add `test_scrollback_via_grpc` smoke test |

---

### Task 1: `ShowScrollback` variant, key binding, and stub dispatch

**Files:**
- Modify: `src/attach/mod.rs:10-16` (enum) and `src/attach/mod.rs:387-480` (dispatch)
- Modify: `src/attach/input.rs:45-57` (binding) and `src/attach/input.rs:161-165` (tests)

- [ ] **Step 1: Write the failing test in `input.rs`**

Add this test to the `tests` module in `src/attach/input.rs`, after the `ctrl_a_quote_shows_list` test:

```rust
#[test]
fn ctrl_a_s_shows_scrollback() {
    let (_, bytes, action) = run_from(EscapeState::Normal, &[0x01, b's']);
    assert!(matches!(action, Some(InputAction::ShowScrollback)));
    assert!(bytes.is_empty());
}
```

- [ ] **Step 2: Run to confirm it fails**

```bash
cargo test --lib attach::input::tests::ctrl_a_s_shows_scrollback
```

Expected: compile error — `InputAction::ShowScrollback` does not exist yet.

- [ ] **Step 3: Add `ShowScrollback` to `InputAction` in `mod.rs`**

In `src/attach/mod.rs`, the enum currently reads:

```rust
pub(super) enum InputAction {
    Detach,
    Create,
    SwitchNext,
    SwitchIndex(u8),
    ShowList,
}
```

Change it to:

```rust
pub(super) enum InputAction {
    Detach,
    Create,
    SwitchNext,
    SwitchIndex(u8),
    ShowList,
    ShowScrollback,
}
```

- [ ] **Step 4: Add the key binding in `input.rs`**

In `src/attach/input.rs`, the `AfterCtrlA` arm currently reads:

```rust
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
```

Add `b's'` after `b'"'`:

```rust
EscapeState::AfterCtrlA => match byte {
    0x01     => { to_send.push(0x01); *state = EscapeState::Normal; None }
    b'c'     => Some(InputAction::Create),
    b'"'     => Some(InputAction::ShowList),
    b's'     => Some(InputAction::ShowScrollback),
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
```

- [ ] **Step 5: Add stub dispatch arm in `mod.rs`**

Adding `ShowScrollback` to the enum makes the existing `match action { ... }` in `run()` non-exhaustive — it won't compile until every variant is covered. Add an empty stub arm. Find the `InputAction::ShowList` arm (around line 459) and add after its closing brace:

```rust
InputAction::ShowScrollback => {}
```

The new arm to add (place it as the last arm in the `match action { ... }` block, after the `InputAction::ShowList` arm's closing brace):

```rust
InputAction::ShowScrollback => {}
```

- [ ] **Step 6: Run the test to confirm it passes**

```bash
cargo test --lib attach::input::tests::ctrl_a_s_shows_scrollback
```

Expected:
```
test attach::input::tests::ctrl_a_s_shows_scrollback ... ok
test result: ok. 1 passed
```

- [ ] **Step 7: Run full lib suite to confirm no regressions**

```bash
cargo test --lib
```

Expected: all tests pass, same count as before plus the new test.

- [ ] **Step 8: Commit**

```bash
git add src/attach/mod.rs src/attach/input.rs
git commit -m "feat(attach): add ShowScrollback action and ^A s key binding"
```

---

### Task 2: `scrollback.rs` pure helpers and unit tests

**Files:**
- Create: `src/attach/scrollback.rs`
- Modify: `src/attach/mod.rs` (add `mod scrollback;`)

- [ ] **Step 1: Write the failing tests**

Create `src/attach/scrollback.rs` with just the test stubs and placeholder function skeletons:

```rust
fn max_row_offset(total: u32, rows: u32) -> u32 {
    todo!()
}

fn format_page(data: &[u8], row_offset: u32, total: u32, rows: u32) -> Vec<u8> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_row_offset_exact_fit() {
        assert_eq!(max_row_offset(24, 24), 0);
    }

    #[test]
    fn max_row_offset_more_than_screen() {
        assert_eq!(max_row_offset(100, 24), 76);
    }

    #[test]
    fn max_row_offset_less_than_screen() {
        assert_eq!(max_row_offset(10, 24), 0);
    }

    #[test]
    fn format_page_starts_with_clear_and_home() {
        let out = format_page(b"hello", 0, 50, 24);
        assert!(out.starts_with(b"\x1b[2J\x1b[H"));
    }

    #[test]
    fn format_page_includes_content_bytes() {
        let out = format_page(b"some scrollback content", 0, 50, 24);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("some scrollback content"));
    }

    #[test]
    fn format_page_status_bar_on_last_row() {
        let out = format_page(b"", 0, 50, 24);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[24;1H"), "status bar should move cursor to row 24");
    }

    #[test]
    fn format_page_status_shows_offset_and_total() {
        let out = format_page(b"", 4, 100, 24);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("row 5 / 100"), "status should show 1-based offset and total");
    }
}
```

- [ ] **Step 2: Declare the module in `mod.rs`**

In `src/attach/mod.rs`, find the module declarations:

```rust
mod cell;
mod formatter;
mod raw;
mod region;
```

Add `mod scrollback;`:

```rust
mod cell;
mod formatter;
mod raw;
mod region;
mod scrollback;
```

- [ ] **Step 3: Run to confirm tests fail (not just panic — compile succeeds)**

```bash
cargo test --lib attach::scrollback::tests
```

Expected: 7 test failures with `called todo!()` panics.

- [ ] **Step 4: Implement `max_row_offset`**

```rust
fn max_row_offset(total: u32, rows: u32) -> u32 {
    total.saturating_sub(rows)
}
```

- [ ] **Step 5: Implement `format_page`**

```rust
fn format_page(data: &[u8], row_offset: u32, total: u32, rows: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    out.extend_from_slice(data);
    let status = format!(
        "\x1b[{rows};1H\x1b[2K\x1b[7m SCROLLBACK  row {} / {}  (ESC to exit) \x1b[0m",
        row_offset + 1, total,
    );
    out.extend_from_slice(status.as_bytes());
    out
}
```

- [ ] **Step 6: Run tests to confirm they pass**

```bash
cargo test --lib attach::scrollback::tests
```

Expected:
```
test attach::scrollback::tests::format_page_includes_content_bytes ... ok
test attach::scrollback::tests::format_page_starts_with_clear_and_home ... ok
test attach::scrollback::tests::format_page_status_bar_on_last_row ... ok
test attach::scrollback::tests::format_page_status_shows_offset_and_total ... ok
test attach::scrollback::tests::max_row_offset_exact_fit ... ok
test attach::scrollback::tests::max_row_offset_less_than_screen ... ok
test attach::scrollback::tests::max_row_offset_more_than_screen ... ok
test result: ok. 7 passed
```

- [ ] **Step 7: Commit**

```bash
git add src/attach/scrollback.rs src/attach/mod.rs
git commit -m "feat(attach/scrollback): add format_page and max_row_offset helpers with tests"
```

---

### Task 3: Complete `scrollback.rs` with async functions

**Files:**
- Modify: `src/attach/scrollback.rs`

No new unit tests — `fetch_scrollback` and `show_scrollback` require a live gRPC stream; they are covered by manual testing and the integration test added in Task 4.

- [ ] **Step 1: Add imports at the top of `scrollback.rs`**

Replace any existing `use` lines with:

```rust
use std::io::Write as IoWrite;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

use termd::proto::{
    terminal_command::Command,
    terminal_response::Response,
    ScrollbackRequest, ScrollbackResponse,
    TerminalCommand, TerminalResponse,
};
```

- [ ] **Step 2: Add `show_scrollback`**

Add this after the imports (before `fetch_scrollback`):

```rust
pub(super) async fn show_scrollback(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<TerminalResponse>,
    pty_id:  &str,
    rows:    u32,
) -> Result<()> {
    let mut row_offset: u32 = 0;
    let mut stdin = tokio::io::stdin();
    let mut buf = [0u8; 8];

    let resp = fetch_scrollback(cmd_tx, resp_rx, pty_id, row_offset, rows).await?;
    let mut total = resp.total_scrollback_rows;

    if total == 0 {
        let _ = std::io::stdout().write_all(
            b"\x1b[2J\x1b[H[No scrollback available]\r\n(ESC to exit)"
        );
        let _ = std::io::stdout().flush();
        let _ = stdin.read(&mut buf).await;
        let _ = std::io::stdout().write_all(b"\x1b[2J\x1b[H");
        let _ = std::io::stdout().flush();
        return Ok(());
    }

    display_page(&resp.data, row_offset, total, rows);

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };

        match &buf[..n] {
            [0x1b, b'[', b'A', ..] => {
                if row_offset < max_row_offset(total, rows) {
                    row_offset += 1;
                    let resp = fetch_scrollback(cmd_tx, resp_rx, pty_id, row_offset, rows).await?;
                    total = resp.total_scrollback_rows;
                    display_page(&resp.data, row_offset, total, rows);
                }
            }
            [0x1b, b'[', b'B', ..] => {
                if row_offset > 0 {
                    row_offset -= 1;
                    let resp = fetch_scrollback(cmd_tx, resp_rx, pty_id, row_offset, rows).await?;
                    total = resp.total_scrollback_rows;
                    display_page(&resp.data, row_offset, total, rows);
                }
            }
            [0x1b] => {
                let mut rest = [0u8; 2];
                let extra = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    stdin.read(&mut rest),
                ).await.ok().and_then(|r| r.ok());
                match extra {
                    Some(2) if rest[0] == b'[' && rest[1] == b'A' => {
                        if row_offset < max_row_offset(total, rows) {
                            row_offset += 1;
                            let resp = fetch_scrollback(cmd_tx, resp_rx, pty_id, row_offset, rows).await?;
                            total = resp.total_scrollback_rows;
                            display_page(&resp.data, row_offset, total, rows);
                        }
                    }
                    Some(2) if rest[0] == b'[' && rest[1] == b'B' => {
                        if row_offset > 0 {
                            row_offset -= 1;
                            let resp = fetch_scrollback(cmd_tx, resp_rx, pty_id, row_offset, rows).await?;
                            total = resp.total_scrollback_rows;
                            display_page(&resp.data, row_offset, total, rows);
                        }
                    }
                    _ => break,
                }
            }
            _ => {}
        }
    }

    let _ = std::io::stdout().write_all(b"\x1b[2J\x1b[H");
    let _ = std::io::stdout().flush();
    Ok(())
}
```

- [ ] **Step 3: Add `fetch_scrollback` and `display_page`**

Add these after `show_scrollback`:

```rust
async fn fetch_scrollback(
    cmd_tx:     &mpsc::Sender<TerminalCommand>,
    resp_rx:    &mut tonic::Streaming<TerminalResponse>,
    pty_id:     &str,
    row_offset: u32,
    row_count:  u32,
) -> Result<ScrollbackResponse> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Scrollback(ScrollbackRequest {
            pty_id: pty_id.to_owned(),
            row_offset,
            row_count,
        })),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during scrollback fetch"),
            Some(r) => match r.response {
                Some(Response::Scrollback(s)) => return Ok(s),
                Some(Response::Stream(_)) => {}
                _ => {}
            }
        }
    }
}

fn display_page(data: &[u8], row_offset: u32, total: u32, rows: u32) {
    let out = format_page(data, row_offset, total, rows);
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}
```

- [ ] **Step 4: Confirm it compiles**

```bash
cargo build 2>&1 | grep -E "^error"
```

Expected: no output (zero errors). Warnings about unused imports are fine; they'll clear when the dispatch is wired in Task 4.

- [ ] **Step 5: Run lib tests to confirm helpers still pass**

```bash
cargo test --lib attach::scrollback::tests
```

Expected: 7 tests pass (unchanged from Task 2).

- [ ] **Step 6: Commit**

```bash
git add src/attach/scrollback.rs
git commit -m "feat(attach/scrollback): add show_scrollback and fetch_scrollback"
```

---

### Task 4: Wire dispatch, integration test, full green

**Files:**
- Modify: `src/attach/mod.rs` (replace stub dispatch with real call)
- Modify: `tests/integration.rs` (add gRPC smoke test)

- [ ] **Step 1: Write the failing integration test**

In `tests/integration.rs`, find the end of the file and add:

```rust
#[tokio::test]
async fn test_scrollback_via_grpc() {
    let (_dir, mut client) = test_server().await;

    let resp = send_recv(
        &mut client,
        terminal_command::Command::Create(CreateRequest { cols: 80, rows: 24, command: None }),
    ).await;
    let pty_id = match resp.response.unwrap() {
        termd::proto::terminal_response::Response::Create(c) => c.item.unwrap().pty_id,
        other => panic!("unexpected: {other:?}"),
    };

    let resp = send_recv(
        &mut client,
        terminal_command::Command::Scrollback(termd::proto::ScrollbackRequest {
            pty_id: pty_id.clone(),
            row_offset: 0,
            row_count: 24,
        }),
    ).await;

    match resp.response.unwrap() {
        termd::proto::terminal_response::Response::Scrollback(sr) => {
            assert_eq!(sr.pty_id, pty_id);
        }
        other => panic!("unexpected: {other:?}"),
    }
}
```

- [ ] **Step 2: Run to confirm it passes**

```bash
cargo test test_scrollback_via_grpc
```

Expected: the test passes immediately. The server-side `Command::Scrollback` dispatch was implemented in a prior session, so the gRPC path already works. This test adds regression coverage for that path.

- [ ] **Step 3: Replace the stub dispatch in `mod.rs`**

Find the stub added in Task 1:

```rust
InputAction::ShowScrollback => {}
```

Replace it with the real call:

```rust
InputAction::ShowScrollback => {
    scrollback::show_scrollback(
        &cmd_tx,
        &mut resp_rx,
        &current_pty_id,
        current_item.rows,
    ).await?;
    should_subscribe = false;
}
```

- [ ] **Step 4: Run the full test suite**

```bash
cargo test
```

Expected:
```
test result: ok. 18 passed; 0 failed
```

(17 existing + 1 new `test_scrollback_via_grpc`; the 7 new lib tests from Task 2 bring the `--lib` count to 11, but `cargo test` counts all targets.)

If the integration test still fails with a compile error because `terminal_command::Command::Scrollback` is not imported in `integration.rs`, add it to the existing import block in that file. Look for:

```rust
use termd::proto::{TerminalCommand, terminal_command};
use termd::proto::{ListRequest, CreateRequest, DestroyRequest};
```

The `terminal_command::Command::Scrollback` variant should be accessible via `termd::proto::terminal_command::Command::Scrollback` without any new imports, since `terminal_command` is already imported. Verify the `termd::proto::ScrollbackRequest` struct is accessible — it lives in the top-level `termd::proto` namespace and needs no additional import beyond what's there.

- [ ] **Step 5: Commit**

```bash
git add src/attach/mod.rs tests/integration.rs
git commit -m "feat(attach): wire ShowScrollback dispatch; add gRPC integration test"
```

---

## Manual Smoke Test

After all tasks complete:

1. Start the daemon: `./run-termd`
2. In a second terminal, attach: `cargo run -- attach <pty_id>`
3. Run a command that produces several screenfuls of output, e.g. `seq 1 200`
4. Press `^A s` — screen should clear and show the most recent scrollback page with a status bar at the bottom
5. Press up arrow repeatedly — status counter should increment and content should shift further back
6. Press down arrow — counter should decrement back toward `row 1 / N`
7. Press ESC — active terminal screen should restore
8. Attach to a PTY running `vim` (alt screen), press `^A s` — should show `[No scrollback available]` and return on any keypress
