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

// This is a fairly naive version of scrollback, more a proof of concept.
// Right now the scrollback pages get "dumped" out to the screen, instead
// of rendered (they probably need to go through cell/region formatters)
pub(super) async fn show_scrollback(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<TerminalResponse>,
    pty_id:  &str,
    rows:    u32,
) -> Result<()> {
    let _ = std::io::stdout().write_all(b"\x1b[?1049h");
    let _ = std::io::stdout().flush();

    let result = run_scrollback(cmd_tx, resp_rx, pty_id, rows).await;

    let _ = std::io::stdout().write_all(b"\x1b[?1049l");
    let _ = std::io::stdout().flush();

    if let Err(e) = result {
        super::show_error(&e.to_string()).await;
    }
    Ok(())
}

async fn run_scrollback(
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
            b"\x1b[2J\x1b[H[No scrollback available]\r\n(any key to exit)"
        );
        let _ = std::io::stdout().flush();
        let _ = stdin.read(&mut buf).await;
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
            [0x02] => {  // Ctrl-B: page back (further into history)
                let max = max_row_offset(total, rows);
                if row_offset < max {
                    row_offset = (row_offset + rows).min(max);
                    let resp = fetch_scrollback(cmd_tx, resp_rx, pty_id, row_offset, rows).await?;
                    total = resp.total_scrollback_rows;
                    display_page(&resp.data, row_offset, total, rows);
                }
            }
            [0x06] => {  // Ctrl-F: page forward (towards active screen)
                if row_offset > 0 {
                    row_offset = row_offset.saturating_sub(rows);
                    let resp = fetch_scrollback(cmd_tx, resp_rx, pty_id, row_offset, rows).await?;
                    total = resp.total_scrollback_rows;
                    display_page(&resp.data, row_offset, total, rows);
                }
            }
            [b'q'] => break,
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

    Ok(())
}

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
                Some(Response::Command(c)) if !c.success => {
                    anyhow::bail!("scrollback error: {}", c.error.unwrap_or_default())
                }
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

fn max_row_offset(total: u32, rows: u32) -> u32 {
    total.saturating_sub(rows)
}

fn format_page(data: &[u8], row_offset: u32, total: u32, rows: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    out.extend_from_slice(data);
    let status = format!(
        "\x1b[{rows};1H\x1b[2K\x1b[7m SCROLLBACK  row {} / {}  (q/ESC exit  ^B/^F page  ↑↓ line) \x1b[0m",
        row_offset + 1, total,
    );
    out.extend_from_slice(status.as_bytes());
    out
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
