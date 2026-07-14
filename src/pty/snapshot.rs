use std::collections::HashMap;

use anyhow::Result;
use bytes::Bytes;
use libghostty_vt::{Terminal, RenderState, ffi};
use libghostty_vt::fmt::{Format, Formatter, FormatterOptions};
use libghostty_vt::render::CursorVisualStyle;
use libghostty_vt::screen::TrackedGridRef;
use libghostty_vt::selection::Selection;
use libghostty_vt::terminal::{Point, PointCoordinate, PointSpace};

use super::{RefreshData, ScrollbackData, ScrollbackOp};

// Note: on-demand refresh only renders the active screen at call time.  The screen-switch
// broadcast in the reader loop (screen-switch path) mitigates the primary↔alternate gap by
// pushing a full render of the new screen to all subscribers immediately after the switch.
pub(crate) fn do_refresh(
    terminal: &Terminal<'static, 'static>,
    generation: u64,
) -> Result<RefreshData> {

    let cols = terminal.cols()? as u32;
    let rows = terminal.rows()? as u32;

    // Snapshot cursor visibility and shape; the formatter emits neither ?25h nor DECSCUSR for
    // default values, so we emit them explicitly at the end to guarantee correct state after a
    // PTY switch.
    let (cursor_visible, cursor_shape) = {
        let mut rs = RenderState::new()?;
        let rs = rs.update(terminal)?;
        let visible = rs.cursor_visible().unwrap_or(true);
        // Map (visual style, blink) back to a DECSCUSR code (CSI Ps SP q).  The model abstracts
        // DECSCUSR into shape + DEC mode 12 (blink) with no "unset" state, so the default (steady
        // block) is indistinguishable from an app that set it explicitly — we emit nothing for it,
        // leaving the client terminal's own configured default cursor intact (reset_terminal_modes
        // already reset it to default via DECSCUSR CSI 0 SP q).
        let shape: Option<&'static [u8]> = match (
            rs.cursor_visual_style().ok(),
            rs.cursor_blinking().unwrap_or(false),
        ) {
            (Some(CursorVisualStyle::Underline), true)  => Some(&b"\x1b[3 q"[..]),
            (Some(CursorVisualStyle::Underline), false) => Some(&b"\x1b[4 q"[..]),
            (Some(CursorVisualStyle::Bar), true)        => Some(&b"\x1b[5 q"[..]),
            (Some(CursorVisualStyle::Bar), false)       => Some(&b"\x1b[6 q"[..]),
            // Block/BlockHollow: shape matches the default, so restore only the blink difference.
            (Some(CursorVisualStyle::Block | CursorVisualStyle::BlockHollow), true) => Some(&b"\x1b[1 q"[..]),
            // Steady block (the default) or an unreadable style: emit nothing.
            _ => None,
        };
        (visible, shape)
    };

    // Restrict to the active screen — the server terminal has scrollback and we don't want it.
    // Point::Active resolves within the visible grid only, ignoring history rows.
    let top_left = terminal.grid_ref(Point::Active(PointCoordinate { x: 0, y: 0 }))?;
    let bottom_right = terminal.grid_ref(Point::Active(PointCoordinate {
        x: cols.saturating_sub(1) as u16,
        y: rows.saturating_sub(1),
    }))?;
    let selection = Selection::new(top_left, bottom_right, false);

    let extra = ffi::FormatterTerminalExtra {
        size: std::mem::size_of::<ffi::FormatterTerminalExtra>(),
        scrolling_region: true, // restore server app's DECSTBM/DECSLRM state
        modes: true,            // restore terminal modes (mouse tracking, cursor visibility, etc.)
        palette: false,         // don't override the host terminal's color palette
        tabstops: false,        // tabstop restoration moves cursor, corrupting final position
        pwd: false,
        keyboard: true,         // restore keyboard modes (xterm modifyOtherKeys) — CSI > 4 ; Pv m
        title: true,            // restore OSC 0 window title
        colors: true,           // restore OSC 10/11/12 dynamic fg/bg/cursor overrides (only app-set ones)
        screen: ffi::FormatterScreenExtra {
            size: std::mem::size_of::<ffi::FormatterScreenExtra>(),
            cursor: true,        // emit final cursor position at end of output
            style: true,         // restore SGR attributes at cursor so subsequent output is styled correctly
            hyperlink: false,
            protection: false,
            kitty_keyboard: true, // restore kitty keyboard protocol flags — CSI = flags ; 1 u
            charsets: true,      // restore G0-G3 charset designations (e.g. DEC line-drawing)
            saved_cursor: true,  // re-establish DECSC save slot for cursor restore
            // TODO: pending_wrap is not restored — CUP clears it, so if the server
            // cursor was at the last column with pending_wrap=true the client will
            // overwrite instead of wrapping on the next print.  Fixing this likely
            // requires a formatter-level mechanism (e.g. print+backspace at the last
            // column) and careful testing.
        },
    };

    let mut fmt = Formatter::new(terminal, FormatterOptions {
        format: Format::Vt,
        trim: false,
        unwrap: false,
        selection: Some(selection),
        extra,
    })?;

    let mut out: Vec<u8> = Vec::new();
    // Soft reset (DECSTR) + explicit mouse-mode disables + keyboard-mode clears + clear screen
    // + cursor home.  DECSTR alone does not reliably disable mouse-reporting or keyboard
    // protocol modes on all terminals, so we disable them explicitly before the formatter
    // re-enables whatever the server PTY has set (via modes:true / keyboard:true /
    // kitty_keyboard:true).  The formatter output is sent as one blob, so no cursor flicker.
    out.extend_from_slice(b"\x1b[!p");
    out.extend_from_slice(b"\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l");
    // Keyboard protocols emit CSI-u-style key codes; clear both to a known baseline so the
    // formatter's absolute restore is exact (it emits nothing when the PTY has none set).
    // CSI = 0 ; 1 u resets the current kitty stack entry's flags to 0 (depth-independent,
    // mirroring how the formatter restores via CSI = flags ; 1 u); CSI > 4 ; 0 m turns off
    // xterm modifyOtherKeys.
    out.extend_from_slice(b"\x1b[=0;1u\x1b[>4;0m");
    // Dynamic colors and title are the same shape as the keyboard modes: the formatter
    // emits OSC 10/11/12 / OSC 0 only when the PTY has a non-default value, so residual
    // state from the previous PTY must be cleared explicitly.  The bg reset (OSC 111)
    // must precede the 2J below so the screen clears to the true default background.
    out.extend_from_slice(b"\x1b]110\x1b\\\x1b]111\x1b\\\x1b]112\x1b\\");
    out.extend_from_slice(b"\x1b]0;\x1b\\");
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    let vt = fmt.format_alloc(None)?;
    out.extend_from_slice(&vt);
    out.extend_from_slice(b"\x1b[0m"); // trailing SGR reset
    // Explicit cursor visibility — formatter modes:true may not emit ?25h when cursor is visible
    // (treating it as the default, no-op), which leaves the cursor hidden after a switch from a
    // PTY that had it hidden.
    if cursor_visible {
        out.extend_from_slice(b"\x1b[?25h");
    } else {
        out.extend_from_slice(b"\x1b[?25l");
    }
    // Explicit cursor shape (DECSCUSR) — like visibility, the formatter doesn't emit it and
    // reset_terminal_modes reset it to the client default, so restore any non-default shape the
    // server PTY set (e.g. Neovim's bar cursor in insert mode).
    if let Some(scusr) = cursor_shape {
        out.extend_from_slice(scusr);
    }

    Ok(RefreshData {
        generation,
        data: Bytes::from(out),
        cols,
        rows,
        degraded: false,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn do_scrollback(
    terminal:      &mut Terminal<'static, 'static>,
    pins:          &mut HashMap<String, TrackedGridRef>,
    subscriber_id: &str,
    op:            ScrollbackOp,
    amount:        i32,
    row_count:     u32,
    generation:    u64,
    cols:          u32,
) -> Result<ScrollbackData> {
    let total = terminal.total_rows()? as u32;

    // CLOSE: drop the pin, return nothing.
    if op == ScrollbackOp::Close {
        pins.remove(subscriber_id);
        return Ok(ScrollbackData { generation, data: Bytes::new(), total_scrollback_rows: total, row_offset: 0 });
    }

    if total == 0 || row_count == 0 {
        return Ok(ScrollbackData { generation, data: Bytes::new(), total_scrollback_rows: total, row_offset: 0 });
    }

    // Screen space: y=0 oldest history, y=total-1 live tail. The pin marks the
    // viewport's top row. max_start keeps a full viewport ending at the tail.
    let max_start = total.saturating_sub(row_count);

    let start_y: u32 = match op {
        ScrollbackOp::Open => max_start,
        ScrollbackOp::Move => {
            // Current pin row (Screen y); falls back to the tail if there's no
            // pin yet or it can't be resolved (treat as OPEN).
            let base = pins.get(subscriber_id)
                .and_then(|p| p.point(PointSpace::Screen).ok().flatten())
                .map(|c| c.y.min(total - 1))
                .unwrap_or(max_start);
            // + amount = older/up = decrease y; - amount = newer/down = increase y.
            let moved = (base as i64) - (amount as i64);
            moved.clamp(0, max_start as i64) as u32
        }
        ScrollbackOp::Close => unreachable!("handled above"),
    };

    // (Re)place the pin at the resolved top row.
    let pin_point = Point::Screen(PointCoordinate { x: 0, y: start_y });
    match pins.get_mut(subscriber_id) {
        Some(p) => { p.set(terminal, pin_point)?; }
        None    => { let p = terminal.track_grid_ref(pin_point)?; pins.insert(subscriber_id.to_owned(), p); }
    }

    // Helper closure: FormatterTerminalExtra with all flags false.
    // Both FormatterTerminalExtra and FormatterScreenExtra use the sized-struct ABI —
    // the `size` field must be set explicitly; Default::default() would leave size=0.
    let make_extra = || ffi::FormatterTerminalExtra {
        size: std::mem::size_of::<ffi::FormatterTerminalExtra>(),
        scrolling_region: false,
        modes: false,
        palette: false,
        tabstops: false,
        pwd: false,
        keyboard: false,
        title: false,
        colors: false,
        screen: ffi::FormatterScreenExtra {
            size: std::mem::size_of::<ffi::FormatterScreenExtra>(),
            cursor: false,
            style: false,
            hyperlink: false,
            protection: false,
            kitty_keyboard: false,
            charsets: false,
            saved_cursor: false,
        },
    };

    // NOTE: grid_ref(Point::Screen(...)) traverses the internal scrollback page list to
    // locate the target row, which is O(scrollback_depth). If scrollback requests become a
    // latency concern (do_scrollback runs on the reader thread, blocking live PTY I/O),
    // consider offloading to a background thread.
    let render_rows = row_count.min(total - start_y);
    let end_y = start_y + render_rows - 1;

    let top_left = terminal.grid_ref(Point::Screen(PointCoordinate { x: 0, y: start_y }))?;
    let bot_right = terminal.grid_ref(Point::Screen(PointCoordinate {
        x: cols.saturating_sub(1) as u16,
        y: end_y,
    }))?;
    let selection = Selection::new(top_left, bot_right, false);

    let mut fmt = Formatter::new(terminal, FormatterOptions {
        format: Format::Vt,
        trim: false,
        unwrap: false,
        selection: Some(selection),
        extra: make_extra(),
    })?;
    let vt = fmt.format_alloc(None)?;

    let row_offset = total - 1 - end_y;
    Ok(ScrollbackData {
        generation,
        data: Bytes::from(vt.to_vec()),
        total_scrollback_rows: total,
        row_offset,
    })
}

#[cfg(test)]
mod scrollback_tests {
    use super::*;
    use libghostty_vt::TerminalOptions;

    fn make_terminal(cols: u16, rows: u16, scrollback: usize) -> Terminal<'static, 'static> {
        Terminal::new(TerminalOptions { cols, rows, max_scrollback: scrollback }).unwrap()
    }

    fn write_lines(t: &mut Terminal<'static, 'static>, n: usize) {
        for i in 0..n {
            t.vt_write(format!("line{i}\n").as_bytes());
        }
    }

    #[test]
    fn do_scrollback_empty_when_row_count_zero() {
        let mut terminal = make_terminal(80, 24, 1000);
        let mut pins = std::collections::HashMap::new();
        let result = do_scrollback(&mut terminal, &mut pins, "s", ScrollbackOp::Open, 0, 0, 42, 80).unwrap();
        assert_eq!(result.generation, 42);
        assert!(result.data.is_empty());
    }

    #[test]
    fn do_scrollback_handles_more_rows_than_history() {
        let mut terminal = make_terminal(80, 5, 1000);
        let mut pins = std::collections::HashMap::new();
        for i in 0..15u8 { terminal.vt_write(format!("line{}\n", i).as_bytes()); }
        let total = terminal.total_rows().unwrap() as u32;
        assert!(total > 0, "expected rows");
        let result = do_scrollback(&mut terminal, &mut pins, "s", ScrollbackOp::Open, 0, 1000, 7, 80).unwrap();
        assert_eq!(result.total_scrollback_rows, total);
        assert_eq!(result.row_offset, 0);
    }

    #[test]
    fn scrollback_open_anchors_at_live_tail() {
        let mut t = make_terminal(80, 5, 1_000_000);
        write_lines(&mut t, 20);
        let mut pins = std::collections::HashMap::new();
        let r = do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Open, 0, 5, 1, 80).unwrap();
        assert_eq!(r.row_offset, 0, "OPEN shows the live tail");
        assert!(!r.data.is_empty());
        assert_eq!(pins.len(), 1, "OPEN creates a pin");
    }

    #[test]
    fn scrollback_move_up_increases_offset() {
        let mut t = make_terminal(80, 5, 1_000_000);
        write_lines(&mut t, 20);
        let mut pins = std::collections::HashMap::new();
        do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Open, 0, 5, 1, 80).unwrap();
        let r = do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Move, 3, 5, 2, 80).unwrap();
        assert_eq!(r.row_offset, 3, "MOVE +3 scrolls 3 rows up from the tail");
    }

    #[test]
    fn scrollback_content_stays_put_while_streaming() {
        // The core win: after parking the view, new output must not slide it.
        let mut t = make_terminal(80, 5, 1_000_000);
        write_lines(&mut t, 20);           // line0..line19
        let mut pins = std::collections::HashMap::new();
        do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Open, 0, 5, 1, 80).unwrap();
        // Park 8 rows up; capture what we see.
        let parked = do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Move, 8, 5, 2, 80).unwrap();
        // Stream more output.
        write_lines(&mut t, 30);
        // Re-render with no movement (MOVE 0). Content must be identical.
        let after = do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Move, 0, 5, 3, 80).unwrap();
        assert_eq!(parked.data, after.data, "parked content drifted while streaming");
    }

    #[test]
    fn scrollback_move_clamps_at_top() {
        let mut t = make_terminal(80, 5, 1_000_000);
        write_lines(&mut t, 20);
        let mut pins = std::collections::HashMap::new();
        do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Open, 0, 5, 1, 80).unwrap();
        // Move way past the top; must clamp without panicking.
        let r = do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Move, i32::MAX, 5, 2, 80).unwrap();
        let total = t.total_rows().unwrap() as u32;
        assert_eq!(r.row_offset, total - 5, "top clamp: bottom edge sits row_count above oldest");
    }

    #[test]
    fn scrollback_close_drops_pin() {
        let mut t = make_terminal(80, 5, 1_000_000);
        write_lines(&mut t, 20);
        let mut pins = std::collections::HashMap::new();
        do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Open, 0, 5, 1, 80).unwrap();
        let r = do_scrollback(&mut t, &mut pins, "s", ScrollbackOp::Close, 0, 5, 2, 80).unwrap();
        assert!(r.data.is_empty());
        assert!(pins.is_empty(), "CLOSE removes the pin");
    }

    // --- FFI selection-pointer regression guards ----------------------------
    //
    // do_refresh and do_scrollback build a `Selection` from grid_refs and hand
    // it across the FFI boundary to the libghostty-vt formatter. A dangling
    // selection pointer on the Rust side (e.g. a libghostty-rs change that
    // reintroduces the `&s.inner`-from-a-match-arm use-after-free) corrupts the
    // page-list pin and segfaults inside Ghostty's page iterator. These render
    // real content through both paths so a reintroduced bug fails the suite.
    //
    // The use-after-free faults *reliably* only under `--release`; in a debug
    // build the freed stack slot usually still holds valid bytes, so a debug run
    // may not fault. Run `cargo test --release` for the dependable check.

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn do_refresh_renders_selection_content() {
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"Hello World");
        let r = do_refresh(&terminal, 7).unwrap();
        assert_eq!(r.generation, 7);
        assert!(!r.data.is_empty(), "do_refresh produced no output");
        assert!(
            contains(&r.data, b"Hello World"),
            "rendered output missing the screen content (selection path broken?)"
        );
    }

    #[test]
    fn do_refresh_empty_terminal_does_not_crash() {
        let terminal = make_terminal(80, 24, 1000);
        let r = do_refresh(&terminal, 1).unwrap();
        assert_eq!(r.generation, 1);
    }

    // index of `needle`'s first occurrence in `haystack`, for ordering assertions
    fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    #[test]
    fn do_refresh_restores_bar_cursor_with_blink() {
        // Blinking bar (DECSCUSR 5) — what Neovim sets in insert mode.
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"\x1b[5 q");
        let r = do_refresh(&terminal, 1).unwrap();
        assert!(contains(&r.data, b"\x1b[5 q"), "missing blinking-bar DECSCUSR restore");
    }

    #[test]
    fn do_refresh_restores_steady_underline_cursor() {
        // Steady underline (DECSCUSR 4) — exercises shape + the steady (non-blink) code.
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"\x1b[4 q");
        let r = do_refresh(&terminal, 1).unwrap();
        assert!(contains(&r.data, b"\x1b[4 q"), "missing steady-underline DECSCUSR restore");
    }

    #[test]
    fn do_refresh_restores_blinking_block_cursor() {
        // Blinking block (DECSCUSR 1) differs from the model default (steady block),
        // so blink must be restored even though the shape is the default block.
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"\x1b[1 q");
        let r = do_refresh(&terminal, 1).unwrap();
        assert!(contains(&r.data, b"\x1b[1 q"), "missing blinking-block DECSCUSR restore");
    }

    #[test]
    fn do_refresh_omits_decscusr_for_default_cursor() {
        // A fresh PTY (steady block) must NOT emit DECSCUSR, so the client's own
        // configured default cursor is preserved across a switch.
        let terminal = make_terminal(80, 24, 1000);
        let r = do_refresh(&terminal, 1).unwrap();
        assert!(!contains(&r.data, b" q"), "default cursor must not emit a DECSCUSR code");
    }

    #[test]
    fn do_refresh_restores_kitty_keyboard_state() {
        // A PTY whose app pushed kitty keyboard flags (what Neovim does on Ghostty).
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"\x1b[>5u");
        let r = do_refresh(&terminal, 1).unwrap();
        // Preamble must clear kitty to a known baseline (absolute set of current entry to 0)...
        let clear = find(&r.data, b"\x1b[=0;1u").expect("preamble missing kitty clear");
        // ...and the formatter must then restore the target PTY's flags.
        let restore = find(&r.data, b"\x1b[=5;1u").expect("missing kitty keyboard restore");
        assert!(clear < restore, "kitty clear must precede restore");
    }

    #[test]
    fn do_refresh_restores_modify_other_keys() {
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"\x1b[>4;2m"); // enable xterm modifyOtherKeys
        let r = do_refresh(&terminal, 1).unwrap();
        let clear = find(&r.data, b"\x1b[>4;0m").expect("preamble missing modifyOtherKeys disable");
        let restore = find(&r.data, b"\x1b[>4;2m").expect("missing modifyOtherKeys restore");
        assert!(clear < restore, "modifyOtherKeys clear must precede restore");
    }

    #[test]
    fn do_refresh_clears_keyboard_when_pty_has_none() {
        // A PTY with no special keyboard mode: the refresh must still neutralize any
        // residual state on the client (the formatter emits nothing, so the preamble
        // clears must carry it).
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"plain shell");
        let r = do_refresh(&terminal, 1).unwrap();
        assert!(find(&r.data, b"\x1b[=0;1u").is_some(), "missing kitty clear for plain PTY");
        assert!(find(&r.data, b"\x1b[>4;0m").is_some(), "missing modifyOtherKeys clear for plain PTY");
    }

    #[test]
    fn do_refresh_clears_dynamic_colors_when_pty_has_none() {
        // A PTY with no OSC 10/11/12 overrides: the formatter emits nothing, so the
        // preamble resets must clear any residual colors on the client (same pattern
        // as the keyboard-mode clears).
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"plain shell");
        let r = do_refresh(&terminal, 1).unwrap();
        assert!(find(&r.data, b"\x1b]110\x1b\\").is_some(), "missing default-fg reset (OSC 110)");
        assert!(find(&r.data, b"\x1b]111\x1b\\").is_some(), "missing default-bg reset (OSC 111)");
        assert!(find(&r.data, b"\x1b]112\x1b\\").is_some(), "missing cursor-color reset (OSC 112)");
    }

    #[test]
    fn do_refresh_restores_cursor_color_after_clear() {
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"\x1b]12;#ff8800\x1b\\");
        let r = do_refresh(&terminal, 1).unwrap();
        let clear = find(&r.data, b"\x1b]112\x1b\\").expect("preamble missing cursor-color reset");
        let restore = find(&r.data, b"\x1b]12;rgb:ff/88/00").expect("missing cursor-color restore");
        assert!(clear < restore, "cursor-color reset must precede restore");
    }

    #[test]
    fn do_refresh_clears_bg_before_erasing_screen() {
        // The 2J in the preamble must run against the terminal's true default
        // background, not a stale OSC 11 override from the previous PTY.
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"plain shell");
        let r = do_refresh(&terminal, 1).unwrap();
        let reset = find(&r.data, b"\x1b]111\x1b\\").expect("missing default-bg reset");
        let erase = find(&r.data, b"\x1b[2J").expect("missing clear-screen");
        assert!(reset < erase, "default-bg reset must precede clear-screen");
    }

    #[test]
    fn do_refresh_clears_title_when_pty_has_none() {
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"plain shell");
        let r = do_refresh(&terminal, 1).unwrap();
        assert!(find(&r.data, b"\x1b]0;\x1b\\").is_some(), "missing empty-title clear");
    }

    #[test]
    fn do_refresh_restores_title_after_clear() {
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"\x1b]0;my window\x1b\\");
        let r = do_refresh(&terminal, 1).unwrap();
        let clear = find(&r.data, b"\x1b]0;\x1b\\").expect("preamble missing empty-title clear");
        let restore = find(&r.data, b"\x1b]0;my window").expect("missing title restore");
        assert!(clear < restore, "title clear must precede restore");
    }

    #[test]
    fn do_scrollback_renders_history_content() {
        let mut terminal = make_terminal(80, 5, 1000);
        // 12 lines into a 5-row screen pushes 7 rows of history above the screen.
        for i in 0..12u8 {
            terminal.vt_write(format!("line{}\r\n", i).as_bytes());
        }
        let total = terminal.total_rows().unwrap() as u32;
        // A viewport as tall as the whole buffer (OPEN) covers history + active and
        // exercises the Point::Screen selection path from the oldest row.
        let mut pins = std::collections::HashMap::new();
        let r = do_scrollback(&mut terminal, &mut pins, "s", ScrollbackOp::Open, 0, total, 9, 80).unwrap();
        assert_eq!(r.generation, 9);
        assert_eq!(r.total_scrollback_rows, total);
        assert_eq!(r.row_offset, 0, "full-height OPEN sits at the tail with the oldest row visible");
        assert!(!r.data.is_empty(), "do_scrollback produced no output");
        assert!(
            contains(&r.data, b"line0"),
            "scrollback output missing an early history line (selection path broken?)"
        );
    }
}
