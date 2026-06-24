use std::io::Write as IoWrite;

use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;

struct LocalTerminal {
    terminal: Terminal<'static, 'static>,
    render_state: RenderState<'static>,
    row_iter: RowIterator<'static>,
    cell_iter: CellIterator<'static>,
}

impl LocalTerminal {
    fn new(cols: u32, rows: u32) -> Result<Self> {
        Ok(Self {
            terminal: Terminal::new(TerminalOptions {
                cols: cols as u16,
                rows: rows as u16,
                max_scrollback: 0,
            })?,
            render_state: RenderState::new()?,
            row_iter: RowIterator::new()?,
            cell_iter: CellIterator::new()?,
        })
    }

    fn resize(&mut self, cols: u32, rows: u32) -> Result<()> {
        Ok(self.terminal.resize(cols as u16, rows as u16, 0, 0)?)
    }
}

pub(super) struct CellHandler {
    lt: LocalTerminal,
    upgrade_to: Option<super::RenderMode>,
    server_cols: u32,
    server_rows: u32,
}

impl CellHandler {
    pub(super) fn new(cols: u32, rows: u32, upgrade_to: Option<super::RenderMode>) -> Result<Self> {
        Ok(Self {
            lt: LocalTerminal::new(cols, rows)?,
            upgrade_to,
            server_cols: cols,
            server_rows: rows,
        })
    }
}

impl super::RenderModeHandler for CellHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        self.lt.terminal.vt_write(refresh_data);
        render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, true, out)?;
        for (_gen, data) in buffered {
            self.lt.terminal.vt_write(data);
            render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, false, out)?;
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                self.lt.terminal.vt_write(data);
                render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, false, out)?;
            }
            super::PtyEvent::Refresh { cols, rows, data, .. } => {
                self.server_cols = cols;
                self.server_rows = rows;
                let (client_cols, client_rows) = super::get_terminal_size();
                if let Some(target) = self.upgrade_to {
                    if super::server_fits_client(cols, rows, client_cols, client_rows) {
                        return Ok(super::EventResult::ChangeRenderMode(target));
                    }
                }
                self.lt.resize(cols, rows)?;
                self.lt.terminal.vt_write(data);
                render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, true, out)?;
            }
            super::PtyEvent::Resize { cols, rows } => {
                self.server_cols = cols;
                self.server_rows = rows;
                let (client_cols, client_rows) = super::get_terminal_size();
                if let Some(target) = self.upgrade_to {
                    if super::server_fits_client(cols, rows, client_cols, client_rows) {
                        return Ok(super::EventResult::ChangeRenderMode(target));
                    }
                }
                self.lt.resize(cols, rows)?;
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> Result<super::EventResult> {
        if let Some(target) = self.upgrade_to {
            if super::server_fits_client(self.server_cols, self.server_rows, cols, rows) {
                return Ok(super::EventResult::ChangeRenderMode(target));
            }
        }
        render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, true, out)?;
        Ok(super::EventResult::Continue)
    }
}

fn render_dirty(
    terminal: &libghostty_vt::Terminal<'static, 'static>,
    render_state: &mut libghostty_vt::RenderState<'static>,
    row_iter: &mut RowIterator<'static>,
    cell_iter: &mut CellIterator<'static>,
    force_full: bool,
    out: &mut Vec<u8>,
) -> Result<bool> {
    let snapshot = render_state.update(terminal)?;

    let global_dirty = if force_full {
        snapshot.set_dirty(Dirty::Full)?;
        Dirty::Full
    } else {
        snapshot.dirty()?
    };

    if global_dirty == Dirty::Clean {
        return Ok(false);
    }

    if global_dirty == Dirty::Full {
        write!(out, "\x1b[2J\x1b[H").ok();
    }

    let cursor_visible = snapshot.cursor_visible().unwrap_or(true);
    let (cursor_x, cursor_y) = match snapshot.cursor_viewport().ok().flatten() {
        Some(cv) => (cv.x as u32, cv.y as u32),
        None => (
            terminal.cursor_x().unwrap_or(0) as u32,
            terminal.cursor_y().unwrap_or(0) as u32,
        ),
    };

    out.extend_from_slice(b"\x1b[?7l");

    let mut row_iter_active = row_iter.update(&snapshot)?;
    let mut row_idx: u32 = 0;
    let mut char_enc = [0u8; 4];
    let mut grapheme_buf: Vec<char> = Vec::new();

    while let Some(row) = row_iter_active.next() {
        if global_dirty != Dirty::Full && !row.dirty()? {
            row_idx += 1;
            continue;
        }
        write!(out, "\x1b[{};1H", row_idx + 1).ok();

        let mut cell_iter_active = cell_iter.update(row)?;
        while let Some(cell) = cell_iter_active.next() {
            let len = cell.graphemes_len()?;
            if len == 0 {
                let bg = cell.bg_color().ok().flatten();
                if let Some(c) = bg {
                    out.extend_from_slice(b"\x1b[0");
                    write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok();
                    out.extend_from_slice(b"m ");
                } else {
                    out.push(b' ');
                }
            } else {
                if grapheme_buf.len() < len {
                    grapheme_buf.resize(len, '\0');
                }
                cell.graphemes_buf(&mut grapheme_buf[..len])?;
                let graphemes = &grapheme_buf[..len];
                let style = cell.style()?;
                let fg = cell.fg_color().ok().flatten();
                let bg = cell.bg_color().ok().flatten();

                out.extend_from_slice(b"\x1b[0");
                if style.bold          { out.extend_from_slice(b";1"); }
                if style.faint         { out.extend_from_slice(b";2"); }
                if style.italic        { out.extend_from_slice(b";3"); }
                match style.underline {
                    Underline::None   => {}
                    Underline::Double => out.extend_from_slice(b";21"),
                    _                 => out.extend_from_slice(b";4"),
                }
                if style.blink         { out.extend_from_slice(b";5"); }
                if style.inverse       { out.extend_from_slice(b";7"); }
                if style.invisible     { out.extend_from_slice(b";8"); }
                if style.strikethrough { out.extend_from_slice(b";9"); }
                if style.overline      { out.extend_from_slice(b";53"); }
                if let Some(c) = fg { write!(out, ";38;2;{};{};{}", c.r, c.g, c.b).ok(); }
                if let Some(c) = bg { write!(out, ";48;2;{};{};{}", c.r, c.g, c.b).ok(); }
                out.push(b'm');

                for ch in graphemes {
                    out.extend_from_slice(ch.encode_utf8(&mut char_enc).as_bytes());
                }
            }
        }
        row.set_dirty(false)?;
        row_idx += 1;
    }

    out.extend_from_slice(b"\x1b[0m\x1b[?7h");
    if cursor_visible { out.extend_from_slice(b"\x1b[?25h"); } else { out.extend_from_slice(b"\x1b[?25l"); }
    write!(out, "\x1b[{};{}H", cursor_y + 1, cursor_x + 1).ok();

    snapshot.set_dirty(Dirty::Clean)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{PtyEvent, EventResult, RenderModeHandler};

    #[test]
    fn render_dirty_produces_no_output_when_clean() {
        let mut lt = super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(!changed, "clean terminal should produce no output");
        assert!(out.is_empty());
    }

    #[test]
    fn render_dirty_emits_only_changed_rows() {
        let mut lt = super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        lt.terminal.vt_write(b"Hello");

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(changed);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"), "row 1 cursor-goto expected");
        assert!(!s.contains("\x1b[2;1H"), "row 2 should not be rendered");
    }

    #[test]
    fn force_full_renders_all_rows() {
        let mut lt = super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed, "force_full should always produce output");
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"));
        assert!(s.contains("\x1b[24;1H"));
    }

    #[test]
    fn cell_init_renders_content() {
        let mut h = CellHandler::new(80, 24, None).unwrap();
        let mut out = Vec::new();
        let result = h.init(b"Hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(!out.is_empty(), "init should render refresh data");
    }

    #[test]
    fn cell_init_replays_buffered() {
        let mut h = CellHandler::new(80, 24, None).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        let initial_len = out.len();

        let buffered = vec![(2, b"World".to_vec())];
        out.clear();
        let mut h2 = CellHandler::new(80, 24, None).unwrap();
        h2.init(b"", &buffered, &mut out).unwrap();
        assert!(out.len() > initial_len, "init with buffered should produce more output");
    }

    #[test]
    fn cell_stream_renders_dirty() {
        let mut h = CellHandler::new(80, 24, None).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(PtyEvent::Stream { data: b"Test" }, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(!out.is_empty(), "stream data should produce render output");
    }

    #[test]
    fn cell_refresh_resizes_and_renders() {
        let mut h = CellHandler::new(80, 24, None).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { cols: 100, rows: 30, data: b"New" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(!out.is_empty());
    }

    #[test]
    fn cell_no_upgrade_when_not_allowed() {
        let mut h = CellHandler::new(80, 24, None).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { cols: 80, rows: 24, data: b"content" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
    }

    #[test]
    fn cell_sigwinch_re_renders() {
        let mut h = CellHandler::new(80, 24, None).unwrap();
        let mut out = Vec::new();
        h.init(b"Hello", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_sigwinch(120, 40, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(!out.is_empty());
    }
}
