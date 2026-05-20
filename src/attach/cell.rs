use std::io::Write as IoWrite;

use anyhow::Result;
use libghostty_vt::{Terminal, RenderState};
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;
use tokio::io::AsyncWriteExt;
use tokio::signal::unix::{signal, SignalKind};

use termd::proto::{
    terminal_response::Response,
    StreamMetadataReason,
};

pub(super) async fn run(ctx: super::RunContext) -> Result<super::RunOutcome> {
    let super::RunContext { mut resp_rx, cmd_tx, pty_id, item, refresh_gen, refresh_bytes, buffered, mut action_rx } = ctx;

    let mut lt = super::LocalTerminal::new(item.cols, item.rows)?;
    lt.terminal.vt_write(&refresh_bytes);

    let mut stdout = tokio::io::stdout();
    let mut out = Vec::new();

    render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out)?;
    stdout.write_all(&out).await?;

    // Replay stream chunks buffered while awaiting the initial Refresh response
    for (gen, data) in &buffered {
        if *gen > refresh_gen {
            lt.terminal.vt_write(data);
            out.clear();
            render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out)?;
            stdout.write_all(&out).await?;
        }
    }
    stdout.flush().await?;

    let mut sigwinch = signal(SignalKind::window_change())?;
    out.clear();

    let mut pty_closed = false;
    loop {
        out.clear();
        tokio::select! {
            msg = resp_rx.message() => {
                match msg {
                    Ok(Some(r)) => match r.response {
                        Some(Response::Stream(s)) => {
                            if s.generation > refresh_gen {
                                lt.terminal.vt_write(&s.data);
                                render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out)?;
                            }
                        }
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        lt.resize(mi.cols, mi.rows)?;
                                        out.extend_from_slice(b"\x1b[2J");
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                if !pty_closed {
                                    pty_closed = true;
                                    super::move_terminal_end();
                                    eprint!("\r\n[PTY closed]\r\n");
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => { break; }
                }
            }
            action = action_rx.recv() => {
                let action = action.unwrap_or(super::InputAction::Detach);
                return Ok(super::RunOutcome::Action(action, super::RunContext {
                    resp_rx, cmd_tx, pty_id, item,
                    refresh_gen, refresh_bytes: vec![], buffered: vec![],
                    action_rx,
                }));
            }
            _ = sigwinch.recv() => {
                render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out)?;
            }
        }
        if !out.is_empty() {
            if stdout.write_all(&out).await.is_err() { break; }
            let _ = stdout.flush().await;
        }
    }

    Ok(super::RunOutcome::ServerClosed)
}

fn render_dirty(
    terminal: &Terminal<'static, 'static>,
    render_state: &mut RenderState<'static>,
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
        // Full repaint seeded state — clear first so content outside the PTY
        // dimensions (e.g. larger host terminal, or previous PTY of different size)
        // doesn't bleed through.
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

    out.extend_from_slice(b"\x1b[0m");
    if cursor_visible { out.extend_from_slice(b"\x1b[?25h"); } else { out.extend_from_slice(b"\x1b[?25l"); }
    write!(out, "\x1b[{};{}H", cursor_y + 1, cursor_x + 1).ok();

    snapshot.set_dirty(Dirty::Clean)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_dirty_produces_no_output_when_clean() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();
        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        assert!(!changed, "clean terminal should produce no output");
        assert!(out.is_empty());
    }

    #[test]
    fn render_dirty_emits_only_changed_rows() {
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
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
        let mut lt = super::super::LocalTerminal::new(80, 24).unwrap();
        let mut out = Vec::new();
        render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, false, &mut out).unwrap();
        out.clear();

        let changed = render_dirty(&lt.terminal, &mut lt.render_state, &mut lt.row_iter, &mut lt.cell_iter, true, &mut out).unwrap();
        assert!(changed, "force_full should always produce output");
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;1H"));
        assert!(s.contains("\x1b[24;1H"));
    }
}
