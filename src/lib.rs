pub mod auth;
pub mod client;
pub mod commands;
pub mod pty;
pub mod server;
pub mod utmp;

pub mod proto {
    tonic::include_proto!("terminal");
}

/// Create a libghostty terminal with a scrollback budget in bytes (0 disables
/// scrollback). libghostty's own default is a tiny 10 KB, so every terminal
/// sets this explicitly; there is no line limit.
pub fn new_terminal(
    cols: u16,
    rows: u16,
    max_scrollback_bytes: usize,
) -> libghostty_vt::error::Result<libghostty_vt::Terminal<'static, 'static>> {
    let mut terminal = libghostty_vt::Terminal::new(cols, rows)?;
    terminal.set_scrollback_max_bytes(Some(max_scrollback_bytes))?;
    Ok(terminal)
}
