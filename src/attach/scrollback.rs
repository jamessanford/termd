fn max_row_offset(total: u32, rows: u32) -> u32 {
    total.saturating_sub(rows)
}

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
