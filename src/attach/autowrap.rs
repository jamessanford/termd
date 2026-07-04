use std::io::Write as _;

use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions};

/// Control units are held only this long (enough for any CSI we rewrite);
/// longer control strings (OSC/DCS/APC payloads) stream out eagerly.
const CONTROL_HOLD_MAX: usize = 64;

#[derive(Clone, Copy, PartialEq)]
enum UnitKind {
    Printable,
    Control { streamed: bool },
}

pub(super) struct WrapInjector {
    term: Terminal<'static, 'static>,
    /// Bytes of the in-progress unit (a codepoint or an escape sequence).
    /// Printable units are held whole so a break can be injected before them;
    /// control units are held only up to CONTROL_HOLD_MAX for CSI rewriting.
    unit: Vec<u8>,
    kind: Option<UnitKind>,
    prev_x: u16,
    prev_y: u16,
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
            unit: Vec::new(),
            kind: None,
            prev_x: 0,
            prev_y: 0,
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
        // The resize may clamp the cursor; rebaseline so the next printable
        // isn't misread as a wrap.
        self.prev_x = self.term.cursor_x()?;
        self.prev_y = self.term.cursor_y()?;
        Ok(())
    }

    pub(super) fn reset(&mut self, server_cols: u32, server_rows: u32) -> Result<()> {
        self.term = Terminal::new(TerminalOptions {
            cols: server_cols as u16,
            rows: server_rows as u16,
            max_scrollback: 0,
        })?;
        self.unit.clear();
        self.kind = None;
        self.prev_x = 0;
        self.prev_y = 0;
        self.server_rows = server_rows;
        Ok(())
    }

    /// Emit the DECSTBM that establishes the vertical scroll region the
    /// client must use: top margin always 1, bottom margin the server rows.
    pub(super) fn emit_region_setup(&self, out: &mut Vec<u8>) {
        write!(out, "\x1b[1;{}r", self.server_rows).ok();
    }

    /// Emit any held unit bytes verbatim. Only valid at end-of-life (Closed or
    /// cleanup): after a mid-unit flush the classifier and the parser disagree,
    /// so `process` must not be called again.
    pub(super) fn flush(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.unit);
        self.unit.clear();
        self.kind = None;
    }

    pub(super) fn process(&mut self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        for &b in input {
            let kind = *self.kind.get_or_insert(if b >= 0x20 && b != 0x7f {
                UnitKind::Printable
            } else {
                UnitKind::Control { streamed: false }
            });
            self.term.vt_write(&[b]);
            match kind {
                UnitKind::Printable => self.unit.push(b),
                UnitKind::Control { streamed: false } => {
                    self.unit.push(b);
                    if self.unit.len() > CONTROL_HOLD_MAX {
                        // Too long to be a CSI we rewrite: stream from here on.
                        out.extend_from_slice(&self.unit);
                        self.unit.clear();
                        self.kind = Some(UnitKind::Control { streamed: true });
                    }
                }
                UnitKind::Control { streamed: true } => out.push(b),
            }
            if !self.term.vt_at_boundary() {
                continue;
            }
            // A complete unit just landed.
            let cx = self.term.cursor_x()?;
            let cy = self.term.cursor_y()?;
            match self.kind.take().expect("kind set above") {
                UnitKind::Printable => {
                    if cy > self.prev_y || cx < self.prev_x {
                        // The glyph soft-wrapped on the server: inject a break,
                        // then move to the column it actually landed on (the
                        // left margin, which is column 1 unless the app set
                        // DECSLRM — hence the CHA). Width comes from
                        // unicode-width, which can disagree with ghostty on
                        // ambiguous-width codepoints; acceptable drift.
                        out.extend_from_slice(b"\r\n");
                        let left = cx.saturating_sub(unit_width(&self.unit));
                        if left > 0 {
                            write!(out, "\x1b[{}G", left + 1).ok();
                        }
                    }
                    out.extend_from_slice(&self.unit);
                    self.unit.clear();
                }
                UnitKind::Control { streamed: false } => {
                    self.rewrite_control(cx, cy, out);
                    self.unit.clear();
                }
                UnitKind::Control { streamed: true } => {}
            }
            self.prev_x = cx;
            self.prev_y = cy;
        }
        Ok(())
    }

    /// Pass a completed, held control unit to the client, rewriting the few
    /// sequences that contend with our region setup. Filled in by the
    /// DECSTBM/RIS/DECSTR task; passthrough until then.
    fn rewrite_control(&self, _cx: u16, _cy: u16, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.unit);
    }
}

/// Terminal-column width of the first codepoint of a printable unit.
fn unit_width(unit: &[u8]) -> u16 {
    use unicode_width::UnicodeWidthChar;
    std::str::from_utf8(unit)
        .ok()
        .and_then(|s| s.chars().next())
        .and_then(|c| c.width())
        .unwrap_or(1) as u16
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
            wi.process(c, &mut out).unwrap();
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

    #[test]
    fn long_osc_streams_eagerly() {
        // An OSC payload longer than the control hold cap must be emitted
        // before its terminator arrives — no unbounded buffering (gap 1).
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        let mut osc = b"\x1b]52;c;".to_vec();
        osc.extend(std::iter::repeat(b'A').take(4096)); // no ST yet
        wi.process(&osc, &mut out).unwrap();
        assert!(
            out.len() >= 4000,
            "payload must stream out before the terminator, got {} bytes",
            out.len()
        );
        // Terminator arrives later; everything matches the original stream.
        wi.process(b"\x1b\\", &mut out).unwrap();
        osc.extend_from_slice(b"\x1b\\");
        assert_eq!(out, osc);
    }

    #[test]
    fn carry_is_bounded() {
        // Only an incomplete printable unit (max 4 bytes of UTF-8) or a short
        // control prefix may be held across calls.
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        wi.process(b"ab\xf0\x9f\x98", &mut out).unwrap(); // incomplete 4-byte emoji
        assert_eq!(out, b"ab");
        wi.process(b"\x80", &mut out).unwrap(); // 😀 completes
        assert_eq!(out, "ab😀".as_bytes());
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
        wi.process(b"abc", &mut out).unwrap();
        wi.resize(5, 3).unwrap();
        wi.process(b"de", &mut out).unwrap(); // now at col 5 (cols 0..4 filled): pending wrap
        wi.process(b"f", &mut out).unwrap(); // wraps
        assert_eq!(out, b"abcde\r\nf");
    }

    #[test]
    fn wide_glyph_split_across_calls_not_sliced_at_wrap() {
        // 世 (E4 B8 96) is 2 wide. "abc" leaves one column, so 世 wraps. The glyph
        // arrives split across two process() calls. The break must land before the
        // whole glyph — its bytes must never be sliced by the \r\n.
        let mut wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.process(b"abc\xe4", &mut out).unwrap(); // up to the first byte of 世
        wi.process(b"\xb8\x96", &mut out).unwrap(); // the rest of 世
        assert_eq!(out, "abc\r\n世".as_bytes());
    }

    #[test]
    fn wrap_with_left_margin_emits_cha() {
        // App enables DECLRMM and sets left margin 2, right margin 4 on an
        // 8-col server. Cursor to row 1 col 2; "abc" fills cols 2..4 (pending
        // wrap at the right margin); "x" wraps to row 2 *column 2*, not column
        // 1. The injected break must be \r\n followed by CHA to column 2.
        let mut wi = WrapInjector::new(8, 4).unwrap();
        let mut out = Vec::new();
        wi.process(b"\x1b[?69h\x1b[2;4s\x1b[1;2Habcx", &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(
            s.ends_with("abc\r\n\x1b[2Gx"),
            "expected break + CHA to the left margin before 'x', got {:?}", s
        );
    }

    #[test]
    fn wrap_without_margin_has_no_cha() {
        // Plain wrap at column 1 must stay a bare \r\n (no redundant CHA).
        assert_eq!(run_bytes(4, 3, &[b"abcde"]), b"abcd\r\ne");
    }

    #[test]
    fn wide_char_wrap_with_left_margin_emits_cha() {
        // Same margins; 世 (2 wide) can't fit in the last column and wraps to
        // the left margin. left = cx_after - 2.
        let mut wi = WrapInjector::new(8, 4).unwrap();
        let mut out = Vec::new();
        wi.process("\x1b[?69h\x1b[2;5s\x1b[1;4H".as_bytes(), &mut out).unwrap();
        wi.process("a世".as_bytes(), &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(
            s.ends_with("a\r\n\x1b[2G世"),
            "expected CHA to left margin before the wide glyph, got {:?}", s
        );
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
        self.inj.process(refresh_data, out)?;
        for (_gen, data) in buffered {
            self.inj.process(data, out)?;
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                self.inj.process(data, out)?;
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
                self.inj.process(data, out)?;
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
