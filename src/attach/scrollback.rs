use std::io::Write as IoWrite;

use anyhow::Result;

use termd::proto::{
    ScrollbackOpKind, ScrollbackRequest, ScrollbackResponse,
};

use crate::AuthedClient;

// This is a fairly naive version of scrollback, more a proof of concept.
// Right now the scrollback pages get "dumped" out to the screen, instead
// of rendered (they probably need to go through the cell formatter)
pub(super) async fn show_scrollback(
    client:        &mut AuthedClient,
    pty_id:        u64,
    subscriber_id: String,
    rows:          u32,
    stdin:         &mut tokio::io::Stdin,
) -> Result<()> {
    let _ = std::io::stdout().write_all(b"\x1b[?1049h");
    let _ = std::io::stdout().flush();

    let result = run_scrollback(client, pty_id, &subscriber_id, rows, stdin).await;

    let _ = std::io::stdout().write_all(b"\x1b[?1049l");
    let _ = std::io::stdout().flush();

    if let Err(e) = result {
        super::show_error(&e.to_string()).await;
    }
    Ok(())
}

/// Map a raw input chunk to a scrollback intent. `rows` is the viewport height
/// (a page). Returns None for keys that exit the pager.
fn key_to_op(buf: &[u8], rows: u32) -> Option<(ScrollbackOpKind, i32)> {
    let page = rows as i32;
    match buf {
        [0x1b, b'[', b'A', ..] => Some((ScrollbackOpKind::ScrollbackMove, 1)),   // Up: older
        [0x1b, b'[', b'B', ..] => Some((ScrollbackOpKind::ScrollbackMove, -1)),  // Down: newer
        [0x02] | [b'b']        => Some((ScrollbackOpKind::ScrollbackMove, page)),  // Ctrl-B / b: page older
        [0x06] | [b'f']        => Some((ScrollbackOpKind::ScrollbackMove, -page)), // Ctrl-F / f: page newer
        [b'g']                 => Some((ScrollbackOpKind::ScrollbackMove, i32::MAX)), // g: oldest (server clamps)
        [b'G']                 => Some((ScrollbackOpKind::ScrollbackOpen, 0)),     // G: jump to live tail
        _ => None,
    }
}

async fn run_scrollback(
    client:        &mut AuthedClient,
    pty_id:        u64,
    subscriber_id: &str,
    rows:          u32,
    stdin:         &mut tokio::io::Stdin,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 8];

    // Enter: place the pin at the live tail.
    let resp = fetch_scrollback(client, pty_id, subscriber_id, ScrollbackOpKind::ScrollbackOpen, 0, rows).await?;
    let total = resp.total_scrollback_rows;

    if total == 0 {
        let _ = std::io::stdout().write_all(
            b"\x1b[2J\x1b[H[No scrollback available]\r\n(any key to exit)"
        );
        let _ = std::io::stdout().flush();
        let _ = stdin.read(&mut buf).await;
        // No pin to release in the empty case, but CLOSE is harmless/cheap.
        let _ = fetch_scrollback(client, pty_id, subscriber_id, ScrollbackOpKind::ScrollbackClose, 0, 0).await;
        return Ok(());
    }

    display_page(&resp.data, resp.row_offset, total, rows);

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let chunk = &buf[..n];

        // ESC alone (not an arrow sequence) exits; ESC[ A/B are arrows.
        if chunk == [0x1b] {
            let mut rest = [0u8; 2];
            let extra = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                stdin.read(&mut rest),
            ).await.ok().and_then(|r| r.ok());
            match extra {
                Some(2) if rest[0] == b'[' && rest[1] == b'A' => {
                    let resp = fetch_scrollback(client, pty_id, subscriber_id, ScrollbackOpKind::ScrollbackMove, 1, rows).await?;
                    display_page(&resp.data, resp.row_offset, resp.total_scrollback_rows, rows);
                }
                Some(2) if rest[0] == b'[' && rest[1] == b'B' => {
                    let resp = fetch_scrollback(client, pty_id, subscriber_id, ScrollbackOpKind::ScrollbackMove, -1, rows).await?;
                    display_page(&resp.data, resp.row_offset, resp.total_scrollback_rows, rows);
                }
                _ => break, // bare ESC: exit
            }
            continue;
        }

        // Ctrl-Q / q: exit.
        if chunk == [0x11] || chunk == [b'q'] {
            break;
        }

        if let Some((op, amount)) = key_to_op(chunk, rows) {
            let resp = fetch_scrollback(client, pty_id, subscriber_id, op, amount, rows).await?;
            display_page(&resp.data, resp.row_offset, resp.total_scrollback_rows, rows);
        }
    }

    // Exit: release the pin (best-effort).
    let _ = fetch_scrollback(client, pty_id, subscriber_id, ScrollbackOpKind::ScrollbackClose, 0, 0).await;
    Ok(())
}

async fn fetch_scrollback(
    client:        &mut AuthedClient,
    pty_id:        u64,
    subscriber_id: &str,
    kind:          ScrollbackOpKind,
    amount:        i32,
    row_count:     u32,
) -> Result<ScrollbackResponse> {
    let resp = client.scrollback(ScrollbackRequest {
        pty_id,
        subscriber_id: subscriber_id.to_string(),
        kind: kind as i32,
        row_count,
        amount,
    }).await?;
    Ok(resp.into_inner())
}

fn display_page(data: &[u8], row_offset: u32, total: u32, rows: u32) {
    let out = format_page(data, row_offset, total, rows);
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}

fn format_page(data: &[u8], row_offset: u32, total: u32, rows: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    out.extend_from_slice(data);
    let status = format!(
        "\x1b[{rows};1H\x1b[2K\x1b[7m SCROLLBACK  row {} / {}  (q/ESC exit  ^B/^F page  ↑↓ line  g/G top/tail) \x1b[0m",
        row_offset + 1, total,
    );
    out.extend_from_slice(status.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_up_moves_one_row_older() {
        assert_eq!(key_to_op(&[0x1b, b'[', b'A'], 24), Some((ScrollbackOpKind::ScrollbackMove, 1)));
    }

    #[test]
    fn key_down_moves_one_row_newer() {
        assert_eq!(key_to_op(&[0x1b, b'[', b'B'], 24), Some((ScrollbackOpKind::ScrollbackMove, -1)));
    }

    #[test]
    fn key_page_back_moves_a_screenful_older() {
        assert_eq!(key_to_op(&[0x02], 24), Some((ScrollbackOpKind::ScrollbackMove, 24)));   // Ctrl-B
        assert_eq!(key_to_op(b"b", 24), Some((ScrollbackOpKind::ScrollbackMove, 24)));
    }

    #[test]
    fn key_page_forward_moves_a_screenful_newer() {
        assert_eq!(key_to_op(&[0x06], 24), Some((ScrollbackOpKind::ScrollbackMove, -24)));  // Ctrl-F
        assert_eq!(key_to_op(b"f", 24), Some((ScrollbackOpKind::ScrollbackMove, -24)));
    }

    #[test]
    fn key_home_jumps_to_oldest_end_jumps_to_tail() {
        assert_eq!(key_to_op(b"g", 24), Some((ScrollbackOpKind::ScrollbackMove, i32::MAX)));   // g: top
        assert_eq!(key_to_op(b"G", 24), Some((ScrollbackOpKind::ScrollbackOpen, 0)));          // G: tail
    }

    #[test]
    fn key_quit_returns_none() {
        assert_eq!(key_to_op(b"q", 24), None);
        assert_eq!(key_to_op(&[0x11], 24), None); // Ctrl-Q
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
