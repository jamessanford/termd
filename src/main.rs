use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tonic::Request;

use termd::{
    proto::{
        terminal_command::Command, terminal_response::Response,
        CreateRequest, DestroyRequest, ListRequest, RefreshRequest,
        ResizeRequest, SubscribeRequest, TerminalCommand, WriteRequest,
        terminal_service_client::TerminalServiceClient,
    },
    pty::PtyRegistry,
    server,
};

fn default_socket() -> PathBuf {
    let base = std::env::var("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/run/termd"));
    base.join("termd.sock")
}

#[derive(Parser)]
#[command(name = "termd", about = "PTY daemon with gRPC streaming API")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the daemon in the foreground
    Start {
        #[arg(long)]
        log_grpc: bool,
        #[arg(long, default_value = "127.0.0.1:7777")]
        tcp_addr: SocketAddr,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// List active PTYs
    List {
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Create a new PTY
    Create {
        #[arg(long, default_value = "80")]
        cols: u32,
        #[arg(long, default_value = "24")]
        rows: u32,
        #[arg(long)]
        cmd: Option<String>,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Destroy a PTY
    Destroy {
        pty_id: String,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Write text to a PTY (appends newline)
    Send {
        pty_id: String,
        text: String,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Attach to a running PTY, streaming output to stdout and forwarding stdin
    Attach {
        /// PTY ID to attach to (from `termd list`)
        pty_id: String,
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Print message metadata to stderr instead of writing data to stdout
        #[arg(long)]
        debug: bool,
    },
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

fn get_terminal_size() -> Result<(u32, u32)> {
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) };
    if ret < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((ws.ws_col as u32, ws.ws_row as u32))
}

#[derive(Clone, Copy)]
enum EscapeState {
    Normal,
    AfterNewline,
    AfterTilde,
}

async fn run_stdin(
    cmd_tx: tokio::sync::mpsc::Sender<TerminalCommand>,
    pty_id: String,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
) {
    use tokio::io::AsyncReadExt;

    let mut stdin = tokio::io::stdin();
    let mut state = EscapeState::AfterNewline;
    let mut buf = [0u8; 256];
    let mut shutdown_tx = Some(shutdown_tx);

    'outer: loop {
        let n = match stdin.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };

        let mut to_send: Vec<u8> = Vec::new();

        for &byte in &buf[..n] {
            match state {
                EscapeState::Normal => {
                    to_send.push(byte);
                    if byte == b'\n' {
                        state = EscapeState::AfterNewline;
                    }
                }
                EscapeState::AfterNewline => {
                    if byte == b'~' {
                        state = EscapeState::AfterTilde;
                    } else if byte == b'\n' {
                        to_send.push(byte);
                    } else {
                        to_send.push(byte);
                        state = EscapeState::Normal;
                    }
                }
                EscapeState::AfterTilde => {
                    if byte == b'.' {
                        if !to_send.is_empty() {
                            let _ = cmd_tx.send(TerminalCommand {
                                command: Some(Command::Write(WriteRequest {
                                    pty_id: pty_id.clone(),
                                    data: to_send,
                                })),
                            }).await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break 'outer;
                    } else if byte == b'\n' {
                        to_send.push(b'~');
                        to_send.push(byte);
                        state = EscapeState::AfterNewline;
                    } else {
                        to_send.push(b'~');
                        to_send.push(byte);
                        state = EscapeState::Normal;
                    }
                }
            }
        }

        if !to_send.is_empty() {
            if cmd_tx.send(TerminalCommand {
                command: Some(Command::Write(WriteRequest {
                    pty_id: pty_id.clone(),
                    data: to_send,
                })),
            }).await.is_err() {
                break;
            }
        }
    }
}

async fn run_sigwinch(
    cmd_tx: tokio::sync::mpsc::Sender<TerminalCommand>,
    pty_id: String,
) {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sig = match signal(SignalKind::window_change()) {
        Ok(s) => s,
        Err(_) => return,
    };
    while sig.recv().await.is_some() {
        if let Ok((cols, rows)) = get_terminal_size() {
            if cmd_tx.send(TerminalCommand {
                command: Some(Command::Resize(ResizeRequest {
                    pty_id: pty_id.clone(),
                    cols,
                    rows,
                })),
            }).await.is_err() {
                break;
            }
        }
    }
}

fn auth_interceptor(mut req: Request<()>) -> Result<Request<()>, tonic::Status> {
    req.metadata_mut()
        .insert("x-auth-token", server::AUTH_TOKEN.parse().unwrap());
    Ok(req)
}

type AuthedClient = TerminalServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        fn(Request<()>) -> Result<Request<()>, tonic::Status>,
    >,
>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Cmd::Start { log_grpc, tcp_addr, socket } => {
            let level = if log_grpc { "debug" } else { "info" };
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| level.into()),
                )
                .init();

            let socket_path = socket.unwrap_or_else(default_socket);
            if let Some(parent) = socket_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let registry = Arc::new(PtyRegistry::new());
            server::serve(registry, &socket_path, tcp_addr, log_grpc).await?;
        }

        Cmd::List { socket } => {
            let mut client = connect_client(socket).await?;
            let resp = send_recv(&mut client, Command::List(ListRequest {})).await?;
            match resp.response {
                Some(Response::List(l)) => {
                    if l.items.is_empty() {
                        println!("No active PTYs.");
                    } else {
                        println!("{:<38} {:>5} {:>5}  {}", "ID", "COLS", "ROWS", "TITLE");
                        for item in l.items {
                            println!(
                                "{:<38} {:>5} {:>5}  {}",
                                item.pty_id, item.cols, item.rows, item.title
                            );
                        }
                    }
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Create { cols, rows, cmd, socket } => {
            let mut client = connect_client(socket).await?;
            let resp = send_recv(
                &mut client,
                Command::Create(CreateRequest { cols, rows, command: cmd }),
            )
            .await?;
            match resp.response {
                Some(Response::Create(c)) => {
                    println!("{}", c.item.map(|i| i.pty_id).unwrap_or_default());
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Destroy { pty_id, socket } => {
            let mut client = connect_client(socket).await?;
            let resp = send_recv(
                &mut client,
                Command::Destroy(DestroyRequest { pty_id: pty_id.clone() }),
            )
            .await?;
            match resp.response {
                Some(Response::Command(c)) => {
                    if c.success {
                        println!("destroyed {pty_id}");
                    } else {
                        eprintln!("error: {}", c.error.unwrap_or_default());
                        std::process::exit(1);
                    }
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Send { pty_id, text, socket } => {
            let mut client = connect_client(socket).await?;
            let mut data = text.into_bytes();
            data.push(b'\n');
            let resp = send_recv(&mut client, Command::Write(WriteRequest { pty_id, data })).await?;
            match resp.response {
                Some(Response::Command(c)) if !c.success => {
                    eprintln!("error: {}", c.error.unwrap_or_default());
                    std::process::exit(1);
                }
                _ => {}
            }
        }

        Cmd::Attach { pty_id, socket, debug } => {
            use tokio::io::AsyncWriteExt;
            use tokio::sync::{mpsc, oneshot};
            use tokio_stream::wrappers::ReceiverStream;

            let mut client = connect_client(socket).await?;

            // Open a long-lived bidi stream
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

            // Sync PTY size to local terminal
            if let Ok((cols, rows)) = get_terminal_size() {
                let _ = cmd_tx.send(TerminalCommand {
                    command: Some(Command::Resize(ResizeRequest {
                        pty_id: pty_id.clone(), cols, rows,
                    })),
                }).await;
            }

            // Request refresh
            cmd_tx.send(TerminalCommand {
                command: Some(Command::Refresh(RefreshRequest { pty_id: pty_id.clone() })),
            }).await?;

            // Buffer StreamData while waiting for RefreshResponse
            let mut buffered: Vec<(u64, Vec<u8>)> = Vec::new();
            let (refresh_gen, refresh_bytes) = loop {
                match resp_rx.message().await? {
                    None => { eprintln!("server disconnected during refresh"); return Ok(()); }
                    Some(r) => match r.response {
                        Some(Response::Refresh(rf)) => break (rf.generation, rf.data),
                        Some(Response::Stream(s)) => buffered.push((s.generation, s.data)),
                        _ => {}
                    }
                }
            };

            // Paint refresh and replay buffered chunks that post-date it
            let mut stdout = tokio::io::stdout();
            if debug {
                eprintln!("[Refresh gen={} len={}]", refresh_gen, refresh_bytes.len());
            } else {
                stdout.write_all(&refresh_bytes).await?;
            }
            for (gen, data) in &buffered {
                if *gen > refresh_gen {
                    if debug {
                        eprintln!("[Buffered gen={} len={}]", *gen, data.len());
                    } else {
                        stdout.write_all(data).await?;
                    }
                }
            }
            stdout.flush().await?;

            // Enter raw mode — drop guard restores terminal on any exit path
            let _guard = setup_raw_mode()?;

            // Spawn concurrent actors
            let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
            let stdin_task = tokio::spawn(run_stdin(
                cmd_tx.clone(), pty_id.clone(), shutdown_tx,
            ));
            let sigwinch_task = tokio::spawn(run_sigwinch(
                cmd_tx.clone(), pty_id.clone(),
            ));
            drop(cmd_tx);

            // Main receive loop
            loop {
                tokio::select! {
                    msg = resp_rx.message() => {
                        match msg {
                            Ok(Some(r)) => {
                                if let Some(Response::Stream(s)) = r.response {
                                    if s.generation > refresh_gen {
                                        if debug {
                                            eprintln!("[Stream gen={} len={}]", s.generation, s.data.len());
                                        } else {
                                            if stdout.write_all(&s.data).await.is_err() { break; }
                                            let _ = stdout.flush().await;
                                        }
                                    }
                                }
                            }
                            _ => break,
                        }
                    }
                    _ = &mut shutdown_rx => break,
                }
            }

            stdin_task.abort();
            sigwinch_task.abort();
            // _guard drops here, restoring terminal
        }
    }

    Ok(())
}

async fn connect_client(socket: Option<PathBuf>) -> Result<AuthedClient> {
    use hyper_util::rt::TokioIo;
    use tonic::transport::Endpoint;
    use tower::service_fn;

    let path = socket.unwrap_or_else(default_socket);
    let channel = Endpoint::try_from("http://[::]:1")?
        .connect_with_connector(service_fn(move |_| {
            let path = path.clone();
            async move { tokio::net::UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await?;

    Ok(TerminalServiceClient::with_interceptor(
        channel,
        auth_interceptor as fn(Request<()>) -> Result<Request<()>, tonic::Status>,
    ))
}

async fn send_recv(
    client: &mut AuthedClient,
    cmd: Command,
) -> Result<termd::proto::TerminalResponse> {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let (tx, rx) = mpsc::channel(1);
    tx.send(TerminalCommand { command: Some(cmd) }).await?;
    drop(tx);

    let mut stream = client.stream(ReceiverStream::new(rx)).await?.into_inner();
    stream
        .message()
        .await?
        .ok_or_else(|| anyhow::anyhow!("empty response from server"))
}
