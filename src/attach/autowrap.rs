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
        })
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
    /// Emits any injected break (Task 3) followed by the buffered glyph bytes.
    fn flush_glyph(&mut self, _x0: u16, _y0: u16, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.glyph);
        self.glyph.clear();
    }

    /// Called when a complete escape sequence has been fed. Emits it verbatim.
    /// (Task 4 replaces this with DECSTBM-clamping logic.)
    fn emit_sequence(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seq);
        self.seq.clear();
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
    server_cols: u32,
    server_rows: u32,
}

impl AutowrapHandler {
    pub(super) fn new(server_cols: u32, server_rows: u32) -> Result<Self> {
        Ok(Self { server_cols, server_rows })
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
}

impl super::RenderModeHandler for AutowrapHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        out.extend_from_slice(refresh_data);
        for (_gen, data) in buffered {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        if let super::PtyEvent::Stream { data, .. } = event {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, _cols: u32, _rows: u32, _out: &mut Vec<u8>) -> Result<super::EventResult> {
        Ok(super::EventResult::Continue)
    }
}
