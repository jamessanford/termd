use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions};

pub(super) struct WrapInjector {
    term: Terminal<'static, 'static>,
    /// Partial trailing unit fed to `term` but not yet emitted, carried to the
    /// next `process` call so a glyph split across calls is never sliced.
    carry: Vec<u8>,
    server_rows: u32,
}

impl WrapInjector {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self {
            term: Terminal::new(TerminalOptions {
                cols: server_cols as u16,
                rows: server_rows as u16,
                max_scrollback: 0,
            })?,
            carry: Vec::new(),
            server_rows,
        })
    }

    /// Tell the tracking terminal its dimensions changed, preserving cursor and
    /// screen state (unlike `reset`, which rebuilds from scratch). Used on a
    /// Resize event, which carries no repaint data: any Stream bytes that arrive
    /// before the server's follow-up Refresh must still be tracked against the
    /// real cursor position so wrap injection stays correct.
    pub(super) fn resize(&mut self, server_cols: u32, server_rows: u32) -> Result<()> {
        self.term.resize(server_cols as u16, server_rows as u16, 0, 0)?;
        self.server_rows = server_rows;
        Ok(())
    }

    pub(super) fn reset(&mut self, server_cols: u32, server_rows: u32) -> Result<()> {
        self.term = Terminal::new(TerminalOptions {
            cols: server_cols as u16,
            rows: server_rows as u16,
            max_scrollback: 0,
        })?;
        self.carry.clear();
        self.server_rows = server_rows;
        Ok(())
    }

    /// Emit the DECSTBM that establishes the vertical scroll region the
    /// client must use: top margin always 1, bottom margin the server rows.
    pub(super) fn emit_region_setup(&self, out: &mut Vec<u8>) {
        use std::io::Write as _;
        write!(out, "\x1b[1;{}r", self.server_rows).ok();
    }

    pub(super) fn process(&mut self, input: &[u8], out: &mut Vec<u8>) {
        // Prepend the carried partial-unit tail (already fed last call) and mark
        // it as the skip prefix so it is not re-fed; it lives in `buf` only to
        // keep one offset frame spanning the carry.
        let mut buf = std::mem::take(&mut self.carry);
        let mut skip = buf.len();
        buf.extend_from_slice(input);

        let mut emit = 0usize; // next unemitted offset within buf
        loop {
            let r = self.term.vt_write_until_wrap(&buf, skip);
            match r.wrap {
                Some(off) => {
                    out.extend_from_slice(&buf[emit..off]); // up to wrapping glyph
                    out.extend_from_slice(b"\r\n"); // injected break
                    out.extend_from_slice(&buf[off..r.committed]); // the wrapping glyph
                    emit = r.committed;
                    skip = r.committed; // everything up to here is now fed
                    // loop: more of buf may remain unfed past the wrap
                }
                None => {
                    out.extend_from_slice(&buf[emit..r.committed]); // complete units
                    self.carry = buf[r.committed..].to_vec(); // partial tail (already fed)
                    break;
                }
            }
        }
    }
}

pub(super) struct AutowrapHandler {
    inj: WrapInjector,
    server_cols: u32,
    server_rows: u32,
}

impl AutowrapHandler {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self {
            inj: WrapInjector::new(server_cols, server_rows)?,
            server_cols,
            server_rows,
        })
    }

    fn fits_client(&self) -> bool {
        let (client_cols, client_rows) = super::get_terminal_size();
        super::server_fits_client(self.server_cols, self.server_rows, client_cols, client_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_bytes(server_cols: u32, server_rows: u32, chunks: &[&[u8]]) -> Vec<u8> {
        let mut wi = WrapInjector::new(server_cols, server_rows).unwrap();
        let mut out = Vec::new();
        for c in chunks {
            wi.process(c, &mut out);
        }
        out
    }

    #[test]
    fn plain_text_passes_through() {
        assert_eq!(run_bytes(80, 24, &[b"hello world"]), b"hello world");
    }

    #[test]
    fn escape_sequence_passes_through() {
        // A CUP and an SGR sequence, no printing past the edge: identity.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[5;5Hhi\x1b[0m"]), b"\x1b[5;5Hhi\x1b[0m");
    }

    #[test]
    fn utf8_split_across_chunks_passes_through() {
        // "é" = 0xC3 0xA9 split across two process() calls.
        assert_eq!(run_bytes(80, 24, &[b"a\xc3", b"\xa9b"]), b"a\xc3\xa9b");
    }

    // 4 columns wide, 3 rows tall server.

    #[test]
    fn full_line_then_printable_injects_break() {
        // "abcd" fills cols 0..3 (deferred wrap), "e" then wraps.
        // Expect a break injected before "e".
        assert_eq!(run_bytes(4, 3, &[b"abcde"]), b"abcd\r\ne");
    }

    #[test]
    fn full_line_then_control_does_not_inject() {
        // "abcd" sets pending wrap; a CUP move clears it without wrapping.
        // No spurious break may be injected.
        assert_eq!(run_bytes(4, 3, &[b"abcd\x1b[2;1Hx"]), b"abcd\x1b[2;1Hx");
    }

    #[test]
    fn exact_fill_no_premature_break() {
        // Exactly cols glyphs and nothing after: no break at all.
        assert_eq!(run_bytes(4, 3, &[b"abcd"]), b"abcd");
    }

    #[test]
    fn wide_char_at_edge_injects_break() {
        // "abc" fills cols 0..2, leaving one column. A 2-wide glyph cannot fit,
        // so the server wraps it to the next line. Detected by cursor movement
        // even though pending-wrap was not set. Expect a break before "世".
        let out = run_bytes(4, 3, &["abc世".as_bytes()]);
        assert_eq!(out, "abc\r\n世".as_bytes());
    }

    #[test]
    fn bottom_margin_scroll_injects_break() {
        // Fill row 0 ("abcd"), wrap to row 1 with "efgh", wrap to row 2 (last)
        // with "ijkl", then "m" wraps again — at the bottom the screen scrolls
        // and cursor_y does not increase, but cursor_x moves left. Expect a
        // break before each wrap, including the scrolling one before "m".
        let out = run_bytes(4, 3, &[b"abcdefghijklm"]);
        assert_eq!(out, b"abcd\r\nefgh\r\nijkl\r\nm");
    }

    #[test]
    fn combining_mark_does_not_inject() {
        // base 'e' + combining acute U+0301 (0xCC 0x81), width 0, attaches to
        // the same cell — no wrap, no break.
        let out = run_bytes(4, 3, &["e\u{0301}f".as_bytes()]);
        assert_eq!(out, "e\u{0301}f".as_bytes());
    }

    #[test]
    fn tab_does_not_inject_a_break() {
        // HT is a C0 control on the control path; it advances the cursor but
        // never itself causes a wrap, so no break may be injected for it. (The
        // exact column HT lands on is terminal-defined; we only assert that the
        // tab passes through untouched. A printable that wraps *after* a tab is
        // covered by the cursor-movement detector, exercised in the other
        // injection tests.)
        assert_eq!(run_bytes(4, 3, &[b"\t"]), b"\t");
    }

    #[test]
    fn wrap_survives_chunk_split() {
        // Same as full_line_then_printable, but "e" arrives in a later chunk and
        // the 4th glyph is split mid-stream. The break must still land before "e".
        assert_eq!(run_bytes(4, 3, &[b"abc", b"d", b"e"]), b"abcd\r\ne");
    }

    #[test]
    fn resize_preserves_cursor_state_for_wrap_detection() {
        // Print "abc" into a 4-col terminal (cursor at col 3), then resize the
        // server to 5 cols. resize() must preserve the cursor so the next glyph
        // wraps at the *new* width: "ab" fills cols 3..4 (deferred wrap at col 5)
        // and "c" then wraps. A reset() would zero the cursor and mis-detect.
        let mut wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.process(b"abc", &mut out);
        wi.resize(5, 3).unwrap();
        wi.process(b"de", &mut out); // now at col 5 (cols 0..4 filled): pending wrap
        wi.process(b"f", &mut out); // wraps
        assert_eq!(out, b"abcde\r\nf");
    }

    #[test]
    fn wide_glyph_split_across_calls_not_sliced_at_wrap() {
        // 世 (E4 B8 96) is 2 wide. "abc" leaves one column, so 世 wraps. The glyph
        // arrives split across two process() calls. The break must land before the
        // whole glyph — its bytes must never be sliced by the \r\n.
        let mut wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.process(b"abc\xe4", &mut out); // up to the first byte of 世
        wi.process(b"\xb8\x96", &mut out); // the rest of 世
        assert_eq!(out, "abc\r\n世".as_bytes());
    }

    #[test]
    fn region_setup_emits_decstbm() {
        let wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.emit_region_setup(&mut out);
        assert_eq!(out, b"\x1b[1;3r");
    }

    use super::super::{EventResult, PtyEvent, RenderMode, RenderModeHandler};

    #[test]
    fn falls_back_to_cell_when_client_too_small_on_resize() {
        // Server grows wider than the client on resize -> hand off to cell mode.
        // get_terminal_size() reads the real terminal; to keep this deterministic
        // we drive the Resize arm with a server size guaranteed larger than any
        // plausible test terminal.
        let mut h = AutowrapHandler::new(80, 24).unwrap();
        let mut out = Vec::new();
        let r = h.on_pty_event(PtyEvent::Resize { cols: 100_000, rows: 100_000 }, &mut out).unwrap();
        match r {
            EventResult::ChangeRenderMode(RenderMode::Cell) => {}
            _ => panic!("expected fallback to Cell"),
        }
    }
}

impl super::RenderModeHandler for AutowrapHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !self.fits_client() {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        self.inj.reset(self.server_cols, self.server_rows)?;
        self.inj.emit_region_setup(out);
        self.inj.process(refresh_data, out);
        for (_gen, data) in buffered {
            self.inj.process(data, out);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                self.inj.process(data, out);
            }
            super::PtyEvent::Refresh { cols, rows, data, .. } => {
                self.server_cols = cols;
                self.server_rows = rows;
                if !self.fits_client() {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                // Refresh is a full reset/redraw: rebuild the tracking terminal
                // and re-emit the region setup before replaying the repaint.
                self.inj.reset(cols, rows)?;
                self.inj.emit_region_setup(out);
                self.inj.process(data, out);
            }
            super::PtyEvent::Resize { cols, rows } => {
                self.server_cols = cols;
                self.server_rows = rows;
                if !self.fits_client() {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                // No repaint data accompanies a Resize; resize the tracking
                // terminal in place (preserving cursor/screen state) so any
                // Stream bytes that arrive before the server's follow-up Refresh
                // are tracked against the real cursor and wrap correctly.
                self.inj.resize(cols, rows)?;
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, cols: u32, rows: u32, _out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !super::server_fits_client(self.server_cols, self.server_rows, cols, rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        // The client width/height changed but the transformed stream is
        // width-agnostic for any client >= server, so there is nothing to
        // re-emit; the existing region setup and injected breaks remain valid.
        Ok(super::EventResult::Continue)
    }

    fn cleanup(&mut self, out: &mut Vec<u8>) {
        // Release the vertical scroll region on detach.
        out.extend_from_slice(b"\x1b[r");
    }
}
