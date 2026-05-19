# Attach: Client-Side Scrollback Viewer

**Date:** 2026-05-19

## Overview

Add a scrollback viewer to the `attach` session loop, bound to `^A s`. The
viewer issues `ScrollbackRequest` to the server, displays the server-formatted
VT response directly to stdout, and lets the user navigate with up/down arrows.
ESC exits and the session loop requests a fresh `RefreshResponse` to restore
the active screen.

Scope: `src/attach/input.rs`, `src/attach/mod.rs`, new
`src/attach/scrollback.rs`. No changes to any renderer (`cell.rs`,
`formatter.rs`, `raw.rs`, `region.rs`).

## Key binding

`^A s` → `InputAction::ShowScrollback`

Add `b's' => Some(InputAction::ShowScrollback)` to the `AfterCtrlA` arm in
`input::process_byte`, alongside the existing `b'"' => ShowList` entry.

Add the variant to the `InputAction` enum in `mod.rs`.

## Session loop integration

In `run()` in `mod.rs`, handle `ShowScrollback` in the
`RunOutcome::Action` branch:

```rust
InputAction::ShowScrollback => {
    show_scrollback(
        &cmd_tx, &mut resp_rx,
        &current_pty_id,
        current_item.rows,
    ).await?;
    should_subscribe = false;  // skip re-subscribe; refresh restores screen
}
```

This mirrors the ShowList-cancel path exactly. After `show_scrollback` returns,
the session loop continues to the next iteration, skips `subscribe()`, and
calls `request_refresh()` to restore the active screen.

## `show_scrollback` — `attach/scrollback.rs`

```rust
pub(super) async fn show_scrollback(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<TerminalResponse>,
    pty_id:  &str,
    rows:    u32,
) -> Result<()>
```

No `LocalTerminal` or libghostty in the viewer. The server returns VT-encoded
content via `ScrollbackResponse.data`; those bytes are display-ready.

### State

```rust
let mut row_offset: u32 = 0;  // 0 = most recent history
```

`row_count` is always `rows` (one full screen per request).

### Fetch helper

```rust
async fn fetch_scrollback(
    cmd_tx:     &mpsc::Sender<TerminalCommand>,
    resp_rx:    &mut tonic::Streaming<TerminalResponse>,
    pty_id:     &str,
    row_offset: u32,
    row_count:  u32,
) -> Result<ScrollbackResponse>
```

Sends `ScrollbackRequest` and awaits `Response::Scrollback`. Intermediate
`Stream` responses are discarded (they accumulate in the server buffer).

### Alt-screen / empty scrollback guard

After the first fetch, if `total_scrollback_rows == 0`:

```
\x1b[2J\x1b[H[No scrollback available]\r\n(ESC to exit)
```

Wait for any keypress and return.

### Display

```rust
fn display_page(data: &[u8], row_offset: u32, total: u32, rows: u32) {
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    out.extend_from_slice(data);
    // Status bar on the last row
    let status = format!(
        "\x1b[{rows};1H\x1b[2K\x1b[7m SCROLLBACK  \
         row {} / {}  (ESC to exit) \x1b[0m",
        row_offset + 1, total,
    );
    out.extend_from_slice(status.as_bytes());
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}
```

### Navigation loop

```rust
loop {
    let n = stdin.read(&mut buf).await?;
    match &buf[..n] {
        [0x1b, b'[', b'A', ..] => {  // up — further back in history
            let max_offset = total.saturating_sub(rows);
            if row_offset < max_offset {
                row_offset += 1;
                let resp = fetch_scrollback(...).await?;
                total = resp.total_scrollback_rows;
                display_page(&resp.data, row_offset, total, rows);
            }
        }
        [0x1b, b'[', b'B', ..] => {  // down — towards active screen
            if row_offset > 0 {
                row_offset -= 1;
                let resp = fetch_scrollback(...).await?;
                total = resp.total_scrollback_rows;
                display_page(&resp.data, row_offset, total, rows);
            }
        }
        [0x1b] => {
            // Bare ESC: try to read 2 more bytes within 50 ms.
            // Timeout means it was a real ESC, not a split arrow sequence.
            // Same pattern as show_list.
            let mut rest = [0u8; 2];
            let extra = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                stdin.read(&mut rest),
            ).await.ok().and_then(|r| r.ok());
            match extra {
                Some(2) if rest[0] == b'[' && rest[1] == b'A' => { /* up */ }
                Some(2) if rest[0] == b'[' && rest[1] == b'B' => { /* down */ }
                _ => break,  // real ESC — exit scrollback
            }
        }
        _ => {}
    }
}
```

On exit: `\x1b[2J\x1b[H` to clear, then return. The session loop calls
`request_refresh` which delivers the current active screen.

## Alt-screen behaviour

When a full-screen app (vim, htop) is active, `terminal.scrollback_rows()`
returns 0. `ScrollbackResponse.total_scrollback_rows` will be 0 and `data`
will be empty. The viewer shows the "no scrollback" notice and exits.

## Files changed

| File | Change |
|---|---|
| `src/attach/input.rs` | Add `b's'` → `ShowScrollback` in `AfterCtrlA`; add test |
| `src/attach/mod.rs` | Add `ShowScrollback` variant to `InputAction`; dispatch in session loop |
| `src/attach/scrollback.rs` | New file: `show_scrollback`, `fetch_scrollback`, `display_page` |
