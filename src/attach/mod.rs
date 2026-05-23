use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{RowIterator, CellIterator};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

mod input;

pub(super) enum InputAction {
    Detach,
    Destroy,
    Create,
    ForceResize,
    ForceRefresh,
    SwitchNext,
    SwitchPrevious,
    SwitchRecent,
    SwitchIndex(u8),
    ShowList,
    ShowInfo,
    ShowScrollback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states (default)
    Cell,
    /// Raw PTY byte passthrough — no libghostty on the client render path
    Raw,
    /// Raw passthrough within a DECSTBM scroll region; rewrites conflicting sequences
    Region,
}

pub(crate) struct RunContext {
    pub resp_rx:       tonic::Streaming<termd::proto::TerminalResponse>,
    pub cmd_tx:        tokio::sync::mpsc::Sender<TerminalCommand>,
    pub pty_id:        String,
    pub item:          PtyItem,
    pub refresh_gen:   u64,
    pub refresh_bytes: Vec<u8>,
    pub buffered:      Vec<(u64, Vec<u8>)>,
    pub action_rx:     tokio::sync::mpsc::Receiver<InputAction>,
}

/// Outcome returned by every render-mode runner.
pub(super) enum RunOutcome {
    /// The server PTY exited or closed.
    ServerClosed,
    /// The current render mode can no longer handle the current dimensions, or has detected
    /// an opportunity to switch. The given mode should be started next.
    /// `refresh_bytes` is empty when the caller relies on the server sending a Refresh
    /// following a resize event; populated when the caller has Refresh data to hand off.
    ChangeRenderMode(RenderMode, RunContext),
    /// An input action was received; the render mode returns the context
    /// so the outer session loop can handle PTY switching.
    Action(InputAction, RunContext),
}

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    CreateRequest, DestroyRequest, ListRequest, PtyItem, RefreshRequest, ResizeRequest,
    SubscribeRequest, UnsubscribeRequest,
    TerminalCommand, StreamMetadataReason,
    terminal_service_client::TerminalServiceClient,
};

mod cell;
mod raw;
mod region;
mod scrollback;

type AuthedClient = TerminalServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        fn(Request<()>) -> Result<Request<()>, tonic::Status>,
    >,
>;

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

struct TerminalGuard {
    original: nix::sys::termios::Termios,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        use nix::sys::termios::{tcsetattr, SetArg};
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(libc::STDIN_FILENO) };
        let _ = tcsetattr(fd, SetArg::TCSAFLUSH, &self.original);
    }
}

fn setup_raw_mode() -> Result<TerminalGuard> {
    use nix::sys::termios::{tcgetattr, tcsetattr, SetArg, LocalFlags, InputFlags};
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(libc::STDIN_FILENO) };
    let original = tcgetattr(fd)?;
    let mut raw = original.clone();
    raw.local_flags.remove(
        LocalFlags::ICANON | LocalFlags::ECHO | LocalFlags::ISIG | LocalFlags::IEXTEN,
    );
    raw.input_flags.remove(
        InputFlags::IXON | InputFlags::ICRNL | InputFlags::BRKINT
            | InputFlags::INPCK | InputFlags::ISTRIP,
    );
    raw.control_chars[libc::VMIN as usize] = 1;
    raw.control_chars[libc::VTIME as usize] = 0;
    tcsetattr(fd, SetArg::TCSAFLUSH, &raw)?;
    Ok(TerminalGuard { original })
}


pub(super) async fn show_error(msg: &str) {
    clear_screen();
    use std::io::Write;
    let _ = std::io::stderr().write_all(format!("\r\n[Error: {msg}]\r\n").as_bytes());
    let _ = std::io::stderr().flush();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

pub(super) fn get_terminal_size() -> (u32, u32) {
    let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws); }
    (ws.ws_col as u32, ws.ws_row as u32)
}

pub(super) fn server_fits_client(server_cols: u32, server_rows: u32, client_cols: u32, client_rows: u32) -> bool {
    client_cols >= server_cols && client_rows >= server_rows
}

// Disable any PTY-set terminal modes so client-side UI and new-PTY refreshes start clean.
// Called on every renderer exit (before ShowList, PTY switch, etc.) and also at session exit
// (where the caller appends the cursor-to-last-row tail).
fn reset_terminal_modes() {
    // Consider using a full "RIS" reset to initial state,
    // keeping it specific like this does help us understand the missing gaps.
    use std::io::Write;
    let _ = std::io::stdout().write_all(concat!(
        "\x1b[?1049l",  // leave alternate screen mode
        "\x1b[?1000l",  // disable X10 mouse reporting
        "\x1b[?1002l",  // disable button-event mouse tracking
        "\x1b[?1003l",  // disable all-motion mouse tracking
        "\x1b[?1006l",  // disable SGR mouse extension
        "\x1b[?1016l",  // disable SGR-pixels mouse extension
        "\x1b[?1015l",  // disable urxvt mouse extension
        "\x1b[?2004l",  // disable bracketed paste
        "\x1b[r",       // reset DECSTBM scroll region to full screen
        "\x1b[?69l",    // disable DECLRMM (horizontal margins)
        "\x1b[?7h",     // re-enable auto-wrap (DECAWM)
        "\x1b[0m",      // reset SGR (colors, attributes)
        "\x1b[<1u",     // pop kitty keyboard protocol
        "\x1b[0q",      // reset cursor style to default
        "\x1b[?25h",    // show cursor
        "\x1b[?1l",     // DECCKM - normal cursor keys
        "\x1b>",        // DECNKM - normal keypad mode
        "\x1b(B",       // reset G0 character set to ASCII
    ).as_bytes());
    let _ = std::io::stdout().flush();
}

fn move_terminal_end() {
    use std::io::Write;
    let (_, rows) = get_terminal_size();
    let _ = std::io::stdout().write_all(
        format!("\x1b[{rows};1H\r\n").as_bytes() // move cursor to last row
    );
    let _ = std::io::stdout().flush();
}

async fn subscribe(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  &str,
) -> anyhow::Result<bool> {
    let (cols, rows) = get_terminal_size();
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest {
            pty_id:   pty_id.to_owned(),
            hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
            cols,
            rows,
        })),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during subscribe"),
            Some(r) => match r.response {
                Some(Response::Subscribe(c)) if c.success => return Ok(true),
                Some(Response::Subscribe(c)) => {
                    show_error("[subscribe failed: {}]", c.error.unwrap_or_default()).await;
                    return Ok(false);
                }
                _ => {}
            }
        }
    }
}

// Returns Ok(None) if the server returned an error for this PTY (e.g. reader thread dead).
// Returns Ok(Some(...)) on success. Returns Err on transport failure.
async fn request_refresh(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  &str,
) -> anyhow::Result<Option<(u64, Vec<u8>, Vec<(u64, Vec<u8>)>)>> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.to_owned() })),
    }).await?;
    let mut buffered: Vec<(u64, Vec<u8>)> = Vec::new();
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during refresh"),
            Some(r) => match r.response {
                Some(Response::Refresh(rf)) => return Ok(Some((rf.generation, rf.data, buffered))),
                Some(Response::Stream(s))   => buffered.push((s.generation, s.data)),
                Some(Response::Command(c)) if !c.success => return Ok(None),
                _ => {}
            }
        }
    }
}

async fn fetch_list(
    cmd_tx:   &mpsc::Sender<TerminalCommand>,
    resp_rx:  &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list: &mut Vec<PtyItem>,
) -> anyhow::Result<()> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::List(ListRequest {})),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during list"),
            Some(r) => match r.response {
                Some(Response::List(lr)) => { *pty_list = lr.items; return Ok(()); }
                _ => {}
            }
        }
    }
}

// Ensures pty_list is populated, fetching if needed. Returns false and shows
// an error message if the fetch fails; the caller should continue 'session.
async fn ensure_list(
    cmd_tx:   &mpsc::Sender<TerminalCommand>,
    resp_rx:  &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list: &mut Vec<PtyItem>,
) -> bool {
    if !pty_list.is_empty() { return true; }
    if let Err(e) = fetch_list(cmd_tx, resp_rx, pty_list).await {
        show_error(&e.to_string()).await;
        return false;
    }
    true
}

fn next_pty<'a>(list: &'a [PtyItem], current_id: &str) -> Option<&'a PtyItem> {
    if list.is_empty() { return None; }
    let pos = list.iter().position(|p| p.pty_id == current_id).unwrap_or(0);
    Some(&list[(pos + 1) % list.len()])
}

fn prev_pty<'a>(list: &'a [PtyItem], current_id: &str) -> Option<&'a PtyItem> {
    if list.is_empty() { return None; }
    let pos = list.iter().position(|p| p.pty_id == current_id).unwrap_or(0);
    Some(&list[(pos + list.len() - 1) % list.len()])
}

// Unsubscribes from the current PTY, updates session state, and subscribes to new_item.
async fn switch_pty(
    cmd_tx:          &mpsc::Sender<TerminalCommand>,
    current_pty_id:  &mut String,
    current_item:    &mut PtyItem,
    previous_pty_id: &mut Option<String>,
    new_item:        PtyItem,
) {
    let _ = cmd_tx.send(TerminalCommand {
        command: Some(Command::Unsubscribe(UnsubscribeRequest {
            pty_id: current_pty_id.clone(),
        })),
    }).await;
    *previous_pty_id = Some(std::mem::replace(current_pty_id, new_item.pty_id.clone()));
    *current_item = new_item;
}

fn recent_pty<'a>(list: &'a [PtyItem], previous_pty_id: &Option<String>) -> Option<&'a PtyItem> {
    let prev_id = previous_pty_id.as_deref()?;
    if let Some(item) = list.iter().find(|p| p.pty_id == prev_id) {
        Some(item)
    } else {
        list.first()
    }
}

fn clear_screen() {
    use std::io::Write;
    let _ = std::io::stdout().write_all(b"\x1b[2J\x1b[H");
    let _ = std::io::stdout().flush();
}

fn draw_list(items: &[PtyItem], selected: usize) {
    use std::io::Write;
    let mut out = Vec::new();
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    for (i, item) in items.iter().enumerate() {
        if i == selected { out.extend_from_slice(b"\x1b[7m"); }
        let title = if item.title.is_empty() { &item.pts_name } else { &item.title };
        let pty_id_trunc: String = item.pty_id.chars().take(16).collect();
        let title_trunc: String = title.chars().take(32).collect();
        let line = format!(
            " {:<16}  {:<32}  {}x{}\r\n",
            pty_id_trunc,
            title_trunc,
            item.cols, item.rows,
        );
        out.extend_from_slice(line.as_bytes());
        if i == selected { out.extend_from_slice(b"\x1b[0m"); }
    }
    let _ = std::io::stdout().write_all(&out);
    let _ = std::io::stdout().flush();
}

async fn show_list(
    cmd_tx:          &mpsc::Sender<TerminalCommand>,
    resp_rx:         &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_list:        &mut Vec<PtyItem>,
    current_pty_id:  &str,
) -> anyhow::Result<Option<String>> {
    // Returns Some(new_pty_id) on selection, None on cancel.
    use tokio::io::AsyncReadExt;

    if let Err(e) = fetch_list(cmd_tx, resp_rx, pty_list).await {
        show_error(&e.to_string()).await;
        return Ok(None);
    }
    if pty_list.is_empty() {
        show_error("[No PTYs available]").await;
        return Ok(None);
    }

    let mut selected = pty_list
        .iter()
        .position(|p| p.pty_id == current_pty_id)
        .unwrap_or(0);

    draw_list(pty_list, selected);

    let mut stdin = tokio::io::stdin();
    let mut buf = [0u8; 8];

    loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => return Ok(None),
            Ok(n) => n,
        };

        match &buf[..n] {
            // Enter — select
            [b'\r'] | [b'\n'] => {
                clear_screen();
                return Ok(Some(pty_list[selected].pty_id.clone()));
            }
            // Arrow keys arrive as 3-byte ESC sequences; match the whole read
            [0x1b, b'[', b'A', ..] => {
                if selected > 0 { selected -= 1; }
                draw_list(pty_list, selected);
            }
            [0x1b, b'[', b'B', ..] => {
                if selected + 1 < pty_list.len() { selected += 1; }
                draw_list(pty_list, selected);
            }
            // Bare ESC: try to read 2 more bytes within 50 ms to rule out
            // a split arrow-key sequence. Timeout means it really was bare ESC.
            [0x1b] => {
                let mut rest = [0u8; 2];
                let is_arrow = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    stdin.read(&mut rest),
                ).await
                .ok()
                .and_then(|r| r.ok())
                .map(|n2| &rest[..n2] == b"[A" || &rest[..n2] == b"[B")
                .unwrap_or(false);

                if is_arrow {
                    if rest[1] == b'A' {
                        if selected > 0 { selected -= 1; }
                    } else {
                        if selected + 1 < pty_list.len() { selected += 1; }
                    }
                    draw_list(pty_list, selected);
                } else {
                    // Bare escape — cancel
                    clear_screen();
                    return Ok(None);
                }
            }
            _ => {}
        }
    }
}

pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
    mode: RenderMode,
) -> Result<()> {
    if debug {
        return run_debug(client, item.pty_id).await;
    }

    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    let _guard = setup_raw_mode()?;

    let mut current_pty_id = item.pty_id.clone();
    let mut current_item = item;
    let mut pty_list: Vec<PtyItem> = Vec::new();
    let mut previous_pty_id: Option<String> = None;
    let mut subscribed_pty_id: Option<String> = None;

    // Only allow cell→region upgrades if the user originally chose region mode.
    // --render-mode=cell stays in cell.
    let allow_upgrade = mode == RenderMode::Region;
    let mut dispatch_mode = mode;

    'session: loop {
        let subscribe_ok = if subscribed_pty_id.as_deref() != Some(current_pty_id.as_str()) {
            let ok = subscribe(&cmd_tx, &mut resp_rx, &current_pty_id).await?;
            if ok { subscribed_pty_id = Some(current_pty_id.clone()); }
            ok
        } else {
            true
        };

        let refresh_result = if subscribe_ok {
            request_refresh(&cmd_tx, &mut resp_rx, &current_pty_id).await?
        } else {
            None
        };

        let (refresh_gen, refresh_bytes, buffered) = match refresh_result {
            Some(triple) => triple,
            None => {
                // PTY reader is dead (closed PTY) — enter with empty state so
                // the user can still use ^A commands to switch or create.
                clear_screen();
                eprint!("\r\n[PTY closed]\r\n");
                (0, vec![], vec![])
            }
        };

        let (action_tx, action_rx) = mpsc::channel::<InputAction>(4);
        let input_task = tokio::spawn(input::run_stdin(
            cmd_tx.clone(),
            action_tx,
            current_pty_id.clone(),
        ));

        let ctx = RunContext {
            resp_rx,
            cmd_tx: cmd_tx.clone(),
            pty_id: current_pty_id.clone(),
            item: current_item.clone(),
            refresh_gen,
            refresh_bytes,
            buffered,
            action_rx,
        };

        let mut dispatch_ctx = ctx;
        let outcome = loop {
            let result = match dispatch_mode {
                RenderMode::Cell   => cell::run(dispatch_ctx, allow_upgrade).await?,
                RenderMode::Raw    => raw::run(dispatch_ctx).await?,
                RenderMode::Region => region::run(dispatch_ctx).await?,
            };
            match result {
                RunOutcome::ChangeRenderMode(new_mode, new_ctx) => {
                    // TODO: We really should request a refresh when switching RenderModes (ideally we could keep stdin open and not have to redo the input loop)
                    dispatch_mode = new_mode;
                    dispatch_ctx = new_ctx;
                }
                other => break other,
            }
        };

        input_task.abort();
        let _ = input_task.await; // synchronize: ensure run_stdin has stopped reading stdin

        match outcome {
            RunOutcome::ServerClosed => {
                reset_terminal_modes();
                move_terminal_end();
                eprintln!("[Connection closed]");
                break 'session;
            }
            RunOutcome::ChangeRenderMode(_, _) => unreachable!(),
            RunOutcome::Action(action, ctx) => {
                resp_rx = ctx.resp_rx;
                reset_terminal_modes();
                match action {
                    InputAction::Detach => break 'session,

                    InputAction::Destroy => {
                        cmd_tx.send(TerminalCommand {
                            command: Some(Command::Destroy(DestroyRequest {
                                pty_id: current_pty_id.clone(),
                            })),
                        }).await?;
                        loop {
                            match resp_rx.message().await? {
                                None => { eprintln!("[server disconnected]"); break 'session; }
                                Some(r) => if let Some(Response::Command(c)) = r.response {
                                    if !c.success {
                                        show_error(&format!("[Failed to destroy PTY: {}]", c.error.unwrap_or_default())).await;
                                        continue 'session;
                                    }
                                    break;
                                }
                            }
                        }
                        pty_list.clear();
                        if ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            if let Some(target) = recent_pty(&pty_list, &previous_pty_id).cloned() {
                                if target.pty_id != current_pty_id {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                }
                            }
                        }
                    }

                    InputAction::ForceResize => {
                        let (cols, rows) = get_terminal_size();
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Resize(ResizeRequest {
                                pty_id: current_pty_id.clone(), cols, rows,
                            })),
                        }).await;
                    }

                    InputAction::ForceRefresh => {
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Refresh(RefreshRequest {
                                pty_id: current_pty_id.clone(),
                            })),
                        }).await;
                    }

                    InputAction::Create => {
                        let (cols, rows) = get_terminal_size();
                        cmd_tx.send(TerminalCommand {
                            command: Some(Command::Create(CreateRequest {
                                cols, rows, command: None,
                            })),
                        }).await?;
                        'create: loop {
                            match resp_rx.message().await? {
                                None => { move_terminal_end(); eprintln!("[server disconnected]"); break 'session; }
                                Some(r) => if let Some(Response::Create(cr)) = r.response {
                                    match cr.item {
                                        Some(new_item) => {
                                            switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, new_item).await;
                                            break 'create;
                                        }
                                        None => {
                                            show_error("[Failed to create new PTY]").await;
                                            pty_list.clear();
                                            continue 'session;
                                        }
                                    }
                                }
                            }
                        }
                        pty_list.clear();
                    }

                    InputAction::SwitchNext => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = next_pty(&pty_list, &current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::SwitchPrevious => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = prev_pty(&pty_list, &current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::SwitchRecent => {
                        if ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            if let Some(target) = recent_pty(&pty_list, &previous_pty_id).cloned() {
                                if target.pty_id != current_pty_id {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                }
                            }
                        }
                    }

                    InputAction::SwitchIndex(n) => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = pty_list.get(n as usize).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::ShowList => {
                        match show_list(&cmd_tx, &mut resp_rx, &mut pty_list, &current_pty_id).await? {
                            Some(new_id) if new_id != current_pty_id => {
                                // Look up item from list so current_item has correct cols/rows.
                                if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                    pty_list.clear();
                                }
                            }
                            _ => {}
                        }
                    }

                    InputAction::ShowInfo => {
                        show_error(&format!(
                            "requested={mode:?} actual={dispatch_mode:?} pty={current_pty_id}"
                        )).await;
                    }

                    InputAction::ShowScrollback => {
                        scrollback::show_scrollback(
                            &cmd_tx,
                            &mut resp_rx,
                            &current_pty_id,
                            current_item.rows,
                        ).await?;
                    }
                }
            }
        }
    }

    // reset_terminal_modes() was already called when the last renderer exited above.
    // Append the last-row move so the shell prompt lands below the PTY content.
    move_terminal_end();

    drop(_guard);
    Ok(())
}

async fn run_debug(client: &mut AuthedClient, pty_id: String) -> Result<()> {
    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    // Subscribe
    let (cols, rows) = get_terminal_size();
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest {
            pty_id:   pty_id.clone(),
            hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
            cols,
            rows,
        })),
    }).await?;

    loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during subscribe"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Subscribe(s)) => {
                    if s.success {
                        eprintln!("[Subscribe pty_id={} subscriber_id={}]", s.pty_id, s.subscriber_id);
                    } else {
                        eprintln!("subscribe failed: {}", s.error.unwrap_or_default());
                        return Ok(());
                    }
                    break;
                }
                _ => {}
            }
        }
    }

    // Request refresh
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
    }).await?;

    // Main debug receive loop — print events to stderr, no rendering
    loop {
        match resp_rx.message().await {
            Ok(Some(r)) => match r.response {
                Some(Response::Stream(s)) => {
                    eprintln!("[Stream gen={} len={}]", s.generation, s.data.len());
                }
                Some(Response::Refresh(rf)) => {
                    eprintln!("[Refresh gen={} len={}]", rf.generation, rf.data.len());
                }
                Some(Response::Metadata(m)) => {
                    eprintln!("[Metadata reason={} gen={} pty_id={}]", m.reason, m.generation, m.pty_id);
                    if m.reason == StreamMetadataReason::Closed as i32 {
                        eprintln!("[Connection closed]");
                        break;
                    }
                }
                _ => {}
            },
            _ => { eprintln!("[Connection closed]"); break; }
        }
    }

    Ok(())
}
