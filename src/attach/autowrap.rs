use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions};

/// Where the classifier is between `process` calls.
enum State {
    /// Parser is at a clean boundary; next byte starts a fresh unit.
    Ground,
    /// Accumulating the bytes of one printable glyph (multi-byte UTF-8 or
    /// base+combining run). `x0`/`y0` are the cursor position before the glyph.
    InGlyph { x0: u16, y0: u16 },
    /// Inside an escape/CSI/OSC sequence; bytes buffered in `seq` until boundary.
    InEscape,
}

pub(super) struct WrapInjector {
    term: Terminal<'static, 'static>,
    state: State,
    /// Buffered bytes of the in-progress glyph, not yet emitted.
    glyph: Vec<u8>,
    /// Buffered bytes of the in-progress escape sequence, not yet emitted.
    seq: Vec<u8>,
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
            state: State::Ground,
            glyph: Vec::new(),
            seq: Vec::new(),
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
        self.state = State::Ground;
        self.glyph.clear();
        self.seq.clear();
        self.server_rows = server_rows;
        Ok(())
    }

    /// True if `b`, at a parser boundary, begins a printable glyph (as opposed
    /// to a C0 control or escape sequence). UTF-8 lead/continuation bytes and
    /// invalid bytes are all >= 0x20; the parser/`vt_at_boundary` assembles them.
    fn is_printable_start(b: u8) -> bool {
        b >= 0x20 && b != 0x7f
    }

    /// Feed one byte to the tracking terminal.
    fn feed(&mut self, b: u8) {
        self.term.vt_write(&[b]);
    }

    /// Called when a complete glyph has been fed (parser back at boundary).
    /// If feeding the glyph moved the cursor to a new row (row advanced) or back
    /// toward the left edge (a wrap that scrolled at the bottom margin), the
    /// server soft-wrapped — inject `\r\n` before the glyph so the wider client
    /// wraps at the same point. Otherwise emit the glyph unchanged.
    fn flush_glyph(&mut self, x0: u16, y0: u16, out: &mut Vec<u8>) {
        let x1 = self.term.cursor_x().unwrap_or(x0);
        let y1 = self.term.cursor_y().unwrap_or(y0);
        let wrapped = y1 > y0 || x1 < x0;
        if wrapped {
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(&self.glyph);
        self.glyph.clear();
    }

    /// Emit a completed escape sequence (in `self.seq`), clamping a DECSTBM
    /// bottom margin to the server row count. All other sequences pass verbatim.
    fn emit_sequence(&mut self, out: &mut Vec<u8>) {
        let seq = std::mem::take(&mut self.seq);
        if let Some(clamped) = self.clamp_decstbm(&seq) {
            out.extend_from_slice(&clamped);
        } else {
            out.extend_from_slice(&seq);
        }
    }

    /// If `seq` is a DECSTBM (`\x1b[<top>;<bottom>r`, no private marker) whose
    /// bottom exceeds `server_rows`, return a clamped rewrite. A bare `\x1b[r`
    /// (reset) and in-bounds regions return `None` (emit verbatim).
    fn clamp_decstbm(&self, seq: &[u8]) -> Option<Vec<u8>> {
        // Must be CSI ... 'r' with a non-private parameter body.
        if seq.len() < 3 || seq[0] != 0x1b || seq[1] != b'[' || *seq.last().unwrap() != b'r' {
            return None;
        }
        let params = &seq[2..seq.len() - 1];
        if params.first() == Some(&b'?') {
            return None; // private mode, not DECSTBM
        }
        if params.is_empty() {
            return None; // bare reset \x1b[r — pass through
        }
        // Parse "top;bottom" (either may be empty/absent).
        let mut parts = params.split(|&c| c == b';');
        let top = parts.next().unwrap_or(b"");
        let bottom = parts.next().unwrap_or(b"");
        if parts.next().is_some() {
            return None; // more than two params: not a DECSTBM we model
        }
        // Validate digits only.
        if !top.iter().all(|c| c.is_ascii_digit()) || !bottom.iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
        if bottom.is_empty() {
            return None; // default bottom = screen
        }
        let bottom_val: u32 = std::str::from_utf8(bottom)
            .ok()?
            .parse()
            .unwrap_or(u32::MAX);
        if bottom_val <= self.server_rows {
            return None; // already in-bounds (or default bottom = screen)
        }
        let top_str = std::str::from_utf8(top).ok()?;
        let mut rewritten = Vec::new();
        use std::io::Write as _;
        write!(rewritten, "\x1b[{};{}r", top_str, self.server_rows).ok();
        Some(rewritten)
    }

    /// Emit the DECSTBM that establishes the vertical scroll region the
    /// client must use: top margin always 1, bottom margin the server rows.
    pub(super) fn emit_region_setup(&self, out: &mut Vec<u8>) {
        use std::io::Write as _;
        write!(out, "\x1b[1;{}r", self.server_rows).ok();
    }

    pub(super) fn process(&mut self, input: &[u8], out: &mut Vec<u8>) {
        for &b in input {
            match self.state {
                State::Ground => {
                    if Self::is_printable_start(b) {
                        let x0 = self.term.cursor_x().unwrap_or(0);
                        let y0 = self.term.cursor_y().unwrap_or(0);
                        self.glyph.push(b);
                        self.feed(b);
                        if self.term.vt_at_boundary() {
                            self.flush_glyph(x0, y0, out);
                            // stay in Ground
                        } else {
                            self.state = State::InGlyph { x0, y0 };
                        }
                    } else if b == 0x1b {
                        self.seq.clear();
                        self.seq.push(b);
                        self.feed(b);
                        // A lone ESC leaves the parser mid-sequence; if the
                        // sequence is somehow already complete (it never is for
                        // a bare ESC) we'd still be safe, but normally we wait.
                        if self.term.vt_at_boundary() {
                            self.emit_sequence(out);
                        } else {
                            self.state = State::InEscape;
                        }
                    } else {
                        // Other C0 control (CR, LF, HT, BEL, BS, ...): forward.
                        self.feed(b);
                        out.push(b);
                    }
                }
                State::InGlyph { x0, y0 } => {
                    self.glyph.push(b);
                    self.feed(b);
                    if self.term.vt_at_boundary() {
                        self.flush_glyph(x0, y0, out);
                        self.state = State::Ground;
                    }
                }
                State::InEscape => {
                    self.seq.push(b);
                    self.feed(b);
                    if self.term.vt_at_boundary() {
                        self.emit_sequence(out);
                        self.state = State::Ground;
                    }
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
    fn region_setup_emits_decstbm() {
        let wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.emit_region_setup(&mut out);
        assert_eq!(out, b"\x1b[1;3r");
    }

    #[test]
    fn app_decstbm_bottom_is_clamped_to_server_rows() {
        // App tries to set a scroll region 1..10 on a 3-row server; clamp to 1..3.
        assert_eq!(run_bytes(4, 3, &[b"\x1b[1;10r"]), b"\x1b[1;3r");
    }

    #[test]
    fn app_decstbm_within_bounds_passes_through() {
        assert_eq!(run_bytes(4, 3, &[b"\x1b[2;3r"]), b"\x1b[2;3r");
    }

    #[test]
    fn app_decstbm_reset_passes_through() {
        // A bare \x1b[r (reset scroll region) passes through unchanged.
        assert_eq!(run_bytes(4, 3, &[b"\x1b[r"]), b"\x1b[r");
    }

    #[test]
    fn app_decstbm_overflow_bottom_is_clamped() {
        // A bottom too large to parse as u32 must still clamp, not pass through.
        assert_eq!(run_bytes(4, 3, &[b"\x1b[1;99999999999r"]), b"\x1b[1;3r");
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
