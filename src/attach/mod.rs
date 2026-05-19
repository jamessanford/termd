use anyhow::Result;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{RowIterator, CellIterator};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

mod input;

pub(super) enum InputAction {
    Detach,
    Create,
    SwitchNext,
    SwitchIndex(u8),
    ShowList,
    ShowScrollback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    /// Cell-by-cell render state for all dirty states (default)
    Cell,
    /// VT formatter for full repaints, cell-by-cell for partial repaints
    Formatter,
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
    /// Region mode detected it can no longer handle current dimensions.
    /// `refresh_bytes` is empty — this relies on the server sending a
    /// refresh following a resize event. That holds for resize-triggered
    /// fallbacks but would not hold for arbitrary render-mode changes.
    FallbackToCell(RunContext),
    /// An input action was received; the render mode returns the context
    /// so the outer session loop can handle PTY switching.
    Action(InputAction, RunContext),
}

use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    CreateRequest, ListRequest, PtyItem, RefreshRequest,
    SubscribeRequest, UnsubscribeRequest,
    TerminalCommand, StreamMetadataReason,
    terminal_service_client::TerminalServiceClient,
};

mod cell;
mod formatter;
mod raw;
mod region;

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


pub(super) fn get_terminal_size() -> (u32, u32) {
    let mut ws = libc::winsize { ws_row: 0, ws_col: 0, ws_xpixel: 0, ws_ypixel: 0 };
    unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws); }
    (ws.ws_col as u32, ws.ws_row as u32)
}

async fn subscribe(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  &str,
) -> anyhow::Result<()> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.to_owned() })),
    }).await?;
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during subscribe"),
            Some(r) => match r.response {
                Some(Response::Command(c)) if c.success => return Ok(()),
                Some(Response::Command(c)) => {
                    anyhow::bail!("subscribe failed: {}", c.error.unwrap_or_default())
                }
                _ => {}
            }
        }
    }
}

async fn request_refresh(
    cmd_tx:  &mpsc::Sender<TerminalCommand>,
    resp_rx: &mut tonic::Streaming<termd::proto::TerminalResponse>,
    pty_id:  &str,
) -> anyhow::Result<(u64, Vec<u8>, Vec<(u64, Vec<u8>)>)> {
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.to_owned() })),
    }).await?;
    let mut buffered: Vec<(u64, Vec<u8>)> = Vec::new();
    loop {
        match resp_rx.message().await? {
            None => anyhow::bail!("server disconnected during refresh"),
            Some(r) => match r.response {
                Some(Response::Refresh(rf)) => return Ok((rf.generation, rf.data, buffered)),
                Some(Response::Stream(s))   => buffered.push((s.generation, s.data)),
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

fn next_pty<'a>(list: &'a [PtyItem], current_id: &str) -> Option<&'a PtyItem> {
    if list.is_empty() { return None; }
    let pos = list.iter().position(|p| p.pty_id == current_id).unwrap_or(0);
    Some(&list[(pos + 1) % list.len()])
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

    fetch_list(cmd_tx, resp_rx, pty_list).await?;
    if pty_list.is_empty() {
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
    let mut should_subscribe = true;

    'session: loop {
        if should_subscribe {
            subscribe(&cmd_tx, &mut resp_rx, &current_pty_id).await?;
        }
        should_subscribe = true;

        let (refresh_gen, refresh_bytes, buffered) =
            request_refresh(&cmd_tx, &mut resp_rx, &current_pty_id).await?;

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

        let mut dispatch_mode = mode;
        let mut dispatch_ctx = ctx;
        let outcome = loop {
            let result = match dispatch_mode {
                RenderMode::Cell      => cell::run(dispatch_ctx).await?,
                RenderMode::Formatter => formatter::run(dispatch_ctx).await?,
                RenderMode::Raw       => raw::run(dispatch_ctx).await?,
                RenderMode::Region    => region::run(dispatch_ctx).await?,
            };
            match result {
                RunOutcome::FallbackToCell(fallback_ctx) => {
                    dispatch_mode = RenderMode::Cell;
                    dispatch_ctx = fallback_ctx;
                }
                other => break other,
            }
        };

        input_task.abort();
        let _ = input_task.await; // synchronize: ensure run_stdin has stopped reading stdin

        match outcome {
            RunOutcome::ServerClosed => {
                eprintln!("[Connection closed]");
                break 'session;
            }
            RunOutcome::FallbackToCell(_) => unreachable!(),
            RunOutcome::Action(action, ctx) => {
                resp_rx = ctx.resp_rx;
                match action {
                    InputAction::Detach => break 'session,

                    InputAction::Create => {
                        let (cols, rows) = get_terminal_size();
                        cmd_tx.send(TerminalCommand {
                            command: Some(Command::Create(CreateRequest {
                                cols, rows, command: None,
                            })),
                        }).await?;
                        'create: loop {
                            match resp_rx.message().await? {
                                None => { eprintln!("[server disconnected]"); break 'session; }
                                Some(r) => if let Some(Response::Create(cr)) = r.response {
                                    match cr.item {
                                        Some(new_item) => {
                                            let _ = cmd_tx.send(TerminalCommand {
                                                command: Some(Command::Unsubscribe(
                                                    UnsubscribeRequest { pty_id: current_pty_id.clone() }
                                                )),
                                            }).await;
                                            current_pty_id = new_item.pty_id.clone();
                                            current_item = new_item;
                                            break 'create;
                                        }
                                        None => {
                                            eprintln!("[server returned Create with no item]");
                                            break 'session;
                                        }
                                    }
                                }
                            }
                        }
                        pty_list.clear();
                    }

                    InputAction::SwitchNext => {
                        if pty_list.is_empty() {
                            fetch_list(&cmd_tx, &mut resp_rx, &mut pty_list).await?;
                        }
                        if let Some(target) = next_pty(&pty_list, &current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                let _ = cmd_tx.send(TerminalCommand {
                                    command: Some(Command::Unsubscribe(
                                        UnsubscribeRequest { pty_id: current_pty_id.clone() }
                                    )),
                                }).await;
                                current_pty_id = target.pty_id.clone();
                                current_item = target;
                            }
                        }
                    }

                    InputAction::SwitchIndex(n) => {
                        if pty_list.is_empty() {
                            fetch_list(&cmd_tx, &mut resp_rx, &mut pty_list).await?;
                        }
                        if let Some(target) = pty_list.get(n as usize).cloned() {
                            if target.pty_id != current_pty_id {
                                let _ = cmd_tx.send(TerminalCommand {
                                    command: Some(Command::Unsubscribe(
                                        UnsubscribeRequest { pty_id: current_pty_id.clone() }
                                    )),
                                }).await;
                                current_pty_id = target.pty_id.clone();
                                current_item = target;
                            }
                        }
                    }

                    InputAction::ShowList => {
                        match show_list(&cmd_tx, &mut resp_rx, &mut pty_list, &current_pty_id).await? {
                            Some(new_id) if new_id != current_pty_id => {
                                let _ = cmd_tx.send(TerminalCommand {
                                    command: Some(Command::Unsubscribe(
                                        UnsubscribeRequest { pty_id: current_pty_id.clone() }
                                    )),
                                }).await;
                                // Look up item from list so current_item has correct cols/rows.
                                if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                                    current_item = target;
                                    current_pty_id = new_id;
                                    pty_list.clear();
                                }
                            }
                            _ => {
                                // Cancel or selected same PTY — skip resubscribe, just refresh.
                                should_subscribe = false;
                            }
                        }
                    }

                    InputAction::ShowScrollback => {}
                }
            }
        }
    }

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
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.clone() })),
    }).await?;

    loop {
        match resp_rx.message().await? {
            None => { eprintln!("server disconnected during subscribe"); return Ok(()); }
            Some(r) => match r.response {
                Some(Response::Command(c)) => {
                    if !c.success {
                        eprintln!("subscribe failed: {}", c.error.unwrap_or_default());
                        return Ok(());
                    }
                    break;
                }
                _ => {}
            }
        }
    }

    // NOTE: no resize sent — server PTY owns its dimensions

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

