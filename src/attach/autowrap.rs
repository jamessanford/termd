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
    /// The DECSTBM region currently established on the client (1-based),
    /// tracked so it can be re-established after a client resize (terminals
    /// reset scroll margins to full screen when the window is resized).
    region_top: u32,
    region_bottom: u32,
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
            region_top: 1,
            region_bottom: server_rows,
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
        // The resize resets the tracked terminal's scroll margins (as any
        // terminal's does); mirror that in the region we consider established.
        self.region_top = 1;
        self.region_bottom = server_rows;
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
        self.region_top = 1;
        self.region_bottom = server_rows;
        Ok(())
    }

    /// Emit the DECSTBM that establishes the vertical scroll region the
    /// client must use: top margin always 1, bottom margin the server rows.
    pub(super) fn emit_region_setup(&mut self, out: &mut Vec<u8>) {
        self.region_top = 1;
        self.region_bottom = self.server_rows;
        write!(out, "\x1b[1;{}r", self.server_rows).ok();
    }

    /// Region setup plus screen clear, for init/Refresh: DECSTBM homes the
    /// cursor, then ED2 clears any stale client content (from a previous
    /// render mode, or client columns/rows beyond the server's) before the
    /// repaint is replayed.
    pub(super) fn emit_screen_setup(&mut self, out: &mut Vec<u8>) {
        self.emit_region_setup(out);
        out.extend_from_slice(b"\x1b[2J");
    }

    /// Re-establish the scroll region after the *client* terminal was resized:
    /// terminals reset DECSTBM to the full screen on resize, which would let
    /// output run past the server's last row instead of scrolling there.
    /// Re-emit the tracked region (which homes the cursor) and put the cursor
    /// back where the tracked terminal says it is. (A DECSLRM left margin was
    /// also reset by the resize and is not restored; the follow-up repaint is
    /// what cleans up such deeper state.)
    pub(super) fn restore_client_region(&mut self, out: &mut Vec<u8>) -> Result<()> {
        write!(out, "\x1b[{};{}r", self.region_top, self.region_bottom).ok();
        let cx = self.term.cursor_x()? as u32;
        let cy = self.term.cursor_y()? as u32;
        // Under DECOM, CUP rows are relative to the region top.
        let row = if self.term.mode(libghostty_vt::terminal::Mode::ORIGIN)? {
            cy + 2 - self.region_top
        } else {
            cy + 1
        };
        write!(out, "\x1b[{};{}H", row, cx + 1).ok();
        Ok(())
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
    /// sequences that contend with our region setup.
    fn rewrite_control(&mut self, cx: u16, cy: u16, out: &mut Vec<u8>) {
        let u = &self.unit;
        if u.as_slice() == b"\x1bc" {
            // RIS: pass through (clears + homes the client), then re-establish
            // the region. DECSTBM homes again, matching RIS's own homing.
            out.extend_from_slice(u);
            self.emit_region_setup(out);
            return;
        }
        if u.starts_with(b"\x1b[") && u.len() >= 3 {
            let body = &u[2..u.len() - 1];
            let fin = *u.last().expect("unit non-empty");
            if fin == b'p' && body.first() == Some(&b'!') {
                // DECSTR: resets margins without moving the cursor. Re-emit the
                // region (which homes the cursor) and put the cursor back where
                // the tracked terminal says it is.
                out.extend_from_slice(u);
                self.emit_region_setup(out);
                write!(out, "\x1b[{};{}H", cy + 1, cx + 1).ok();
                return;
            }
            if fin == b'r' && body.iter().all(|&b| b.is_ascii_digit() || b == b';') {
                // DECSTBM: the app's "full screen" is the server screen. Clamp
                // the bottom margin so it never spills into client rows the
                // server doesn't have.
                let mut params = body
                    .split(|&b| b == b';')
                    .map(|s| std::str::from_utf8(s).ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0));
                let top = params.next().filter(|&p| p != 0).unwrap_or(1);
                let bottom = params.next().unwrap_or(0);
                let bottom = if bottom == 0 || bottom > self.server_rows {
                    self.server_rows
                } else {
                    bottom
                };
                let top = if top >= bottom { 1 } else { top };
                self.region_top = top;
                self.region_bottom = bottom;
                write!(out, "\x1b[{};{}r", top, bottom).ok();
                return;
            }
        }
        out.extend_from_slice(u);
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

impl super::RenderModeHandler for AutowrapHandler {
    fn init(&mut self, refresh_data: &[u8], out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !self.fits_client() {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        self.inj.reset(self.server_cols, self.server_rows)?;
        self.inj.emit_screen_setup(out);
        self.inj.process(refresh_data, out)?;
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
                self.inj.emit_screen_setup(out);
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
            super::PtyEvent::Closed => {
                // The stream is over; a partial unit held for wrap/rewrite
                // inspection will never complete. Emit it verbatim.
                self.inj.flush(out);
            }
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> Result<super::EventResult> {
        if !super::server_fits_client(self.server_cols, self.server_rows, cols, rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        // The transformed stream is width-agnostic for any client >= server,
        // but the resize reset the client's scroll margins (terminals reset
        // DECSTBM to the full screen on resize), so output would stop
        // scrolling at the server's last row. Restore the region and cursor
        // inline so the live stream stays correct, then request a repaint to
        // clean up whatever the resize reflow did to the screen.
        self.inj.restore_client_region(out)?;
        Ok(super::EventResult::RequestRefresh)
    }

    fn cleanup(&mut self, out: &mut Vec<u8>) {
        // Flush any held partial unit, then release the vertical scroll region.
        self.inj.flush(out);
        out.extend_from_slice(b"\x1b[r");
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
        osc.extend(std::iter::repeat_n(b'A', 4096)); // no ST yet
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
        let mut wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.emit_region_setup(&mut out);
        assert_eq!(out, b"\x1b[1;3r");
    }

    #[test]
    fn app_decstbm_bottom_clamped_to_server_rows() {
        // App asks for rows 1..50 on a 24-row server: clamp bottom to 24.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[1;50r"]), b"\x1b[1;24r");
    }

    #[test]
    fn app_decstbm_full_reset_becomes_server_region() {
        // Bare ESC[r means "full screen" to the app — which is the *server*
        // screen, not the taller client. Rewrite to 1..server_rows.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[r"]), b"\x1b[1;24r");
    }

    #[test]
    fn app_decstbm_within_bounds_passes() {
        assert_eq!(run_bytes(80, 24, &[b"\x1b[5;20r"]), b"\x1b[5;20r");
    }

    #[test]
    fn private_csi_r_not_rewritten() {
        // ESC[?...r is DEC private mode *restore*, not DECSTBM.
        assert_eq!(run_bytes(80, 24, &[b"\x1b[?1049r"]), b"\x1b[?1049r");
    }

    #[test]
    fn ris_reestablishes_region() {
        let out = run_bytes(80, 24, &[b"\x1bc"]);
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("\x1bc"), "RIS passes through first");
        assert!(s.contains("\x1b[1;24r"), "region setup must follow RIS");
    }

    #[test]
    fn decstr_reestablishes_region_and_restores_cursor() {
        // DECSTR resets margins but does NOT move the cursor; our re-emitted
        // DECSTBM homes it, so a CUP back to the tracked position must follow.
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        wi.process(b"\x1b[5;10H\x1b[!p", &mut out).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\x1b[!p"), "DECSTR passes through");
        assert!(s.contains("\x1b[1;24r"), "region setup follows DECSTR");
        assert!(s.ends_with("\x1b[5;10H"), "cursor restored after region setup, got {:?}", s);
    }

    use super::super::{EventResult, PtyEvent, RenderMode, RenderModeHandler};

    #[test]
    fn closed_flushes_held_bytes() {
        // A partial CSI held for rewriting must not be swallowed when the PTY
        // closes mid-sequence.
        let mut h = AutowrapHandler::new(80, 24).unwrap();
        let mut out = Vec::new();
        h.init(b"", &mut out).unwrap();
        out.clear();
        h.on_pty_event(PtyEvent::Stream { data: b"\x1b[5;10" }, &mut out).unwrap();
        assert_eq!(out, b"", "partial CSI must be held");
        h.on_pty_event(PtyEvent::Closed, &mut out).unwrap();
        assert_eq!(out, b"\x1b[5;10", "Closed must flush the held bytes");
    }

    #[test]
    fn cleanup_flushes_then_releases_region() {
        let mut h = AutowrapHandler::new(80, 24).unwrap();
        let mut out = Vec::new();
        h.init(b"", &mut out).unwrap();
        out.clear();
        h.on_pty_event(PtyEvent::Stream { data: b"ab\xc3" }, &mut out).unwrap(); // partial é
        out.clear();
        h.cleanup(&mut out);
        assert_eq!(out, b"\xc3\x1b[r", "cleanup must flush the carry before releasing the region");
    }

    #[test]
    fn screen_setup_sets_region_then_clears() {
        // init/Refresh use this: DECSTBM (which homes) followed by ED2 so
        // stale client content never survives a (re)attach. We test the
        // injector method directly because handler.init consults the real
        // terminal size, which is (0,0) in the test environment.
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        wi.emit_screen_setup(&mut out);
        assert_eq!(out, b"\x1b[1;24r\x1b[2J");
    }

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

#[cfg(test)]
mod client_view {
    //! Tests that replay the injector's output into a second (larger)
    //! libghostty terminal standing in for the client, verifying the
    //! vertical behavior the raw byte assertions above can't see.
    use super::*;

    fn screen_rows(client: &Terminal<'_, '_>, cols: u16, rows: u16) -> Vec<String> {
        let mut out = Vec::new();
        for y in 0..rows {
            let mut line = String::new();
            for x in 0..cols {
                let gr = client
                    .grid_ref(libghostty_vt::terminal::Point::Active(
                        libghostty_vt::terminal::PointCoordinate { x, y: y as u32 },
                    ))
                    .unwrap();
                let cp = gr.cell().unwrap().codepoint().unwrap();
                line.push(char::from_u32(cp).filter(|c| *c != '\0').unwrap_or(' '));
            }
            out.push(line.trim_end().to_string());
        }
        out
    }

    #[test]
    fn taller_client_scrolls_at_server_bottom() {
        // Server 4x3, client 10x6: the region setup must make the client
        // scroll at row 3, keeping the last three lines in rows 0..2.
        let mut wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.emit_screen_setup(&mut out);
        wi.process(b"a\r\nb\r\nc\r\nd\r\ne", &mut out).unwrap();
        let mut client = Terminal::new(TerminalOptions { cols: 10, rows: 6, max_scrollback: 0 }).unwrap();
        client.vt_write(&out);
        assert_eq!(screen_rows(&client, 10, 6), vec!["c", "d", "e", "", "", ""]);
        assert!(client.cursor_y().unwrap() <= 2);
    }

    #[test]
    fn client_resize_restores_region_and_scrolling() {
        // A client window resize resets the client's scroll margins; the
        // restore emitted for SIGWINCH must re-establish the region (and the
        // cursor position) so subsequent output keeps scrolling at the
        // server's last row instead of walking into the extra client rows.
        let mut wi = WrapInjector::new(4, 3).unwrap();
        let mut out = Vec::new();
        wi.emit_screen_setup(&mut out);
        wi.process(b"a\r\nb\r\nc", &mut out).unwrap();
        let mut client = Terminal::new(TerminalOptions { cols: 10, rows: 6, max_scrollback: 0 }).unwrap();
        client.vt_write(&out);

        // The user makes the client window even taller: margins reset.
        client.resize(10, 8, 0, 0).unwrap();
        out.clear();
        wi.restore_client_region(&mut out).unwrap();
        client.vt_write(&out);

        out.clear();
        wi.process(b"\r\nd\r\ne", &mut out).unwrap();
        client.vt_write(&out);
        assert_eq!(
            screen_rows(&client, 10, 8),
            vec!["c", "d", "e", "", "", "", "", ""],
            "output must keep scrolling inside the server's 3 rows"
        );
        assert!(client.cursor_y().unwrap() <= 2);
    }

    #[test]
    fn restore_preserves_app_scroll_region() {
        // The app set its own DECSTBM; the restore must re-emit that region,
        // not the full-server one.
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        wi.emit_screen_setup(&mut out);
        wi.process(b"\x1b[5;20r\x1b[10;3H", &mut out).unwrap();
        out.clear();
        wi.restore_client_region(&mut out).unwrap();
        assert_eq!(out, b"\x1b[5;20r\x1b[10;3H");
    }

    #[test]
    fn restore_cursor_is_region_relative_under_decom() {
        // With origin mode on, the restoring CUP must be relative to the
        // region top (absolute row 10 inside a region starting at row 5 is
        // region-relative row 6).
        let mut wi = WrapInjector::new(80, 24).unwrap();
        let mut out = Vec::new();
        wi.emit_screen_setup(&mut out);
        wi.process(b"\x1b[5;20r\x1b[?6h\x1b[6;3H", &mut out).unwrap();
        out.clear();
        wi.restore_client_region(&mut out).unwrap();
        assert_eq!(out, b"\x1b[5;20r\x1b[6;3H");
    }
}
