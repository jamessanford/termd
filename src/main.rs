use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tonic::Request;

use termd::{
    proto::{
        terminal_command::Command, terminal_response::Response,
        CreateRequest, DestroyRequest, ListRequest, PtyItem, ResizeRequest,
        TerminalCommand, WriteRequest,
        terminal_service_client::TerminalServiceClient,
    },
    pty::PtyRegistry,
    server,
};

mod attach;

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
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
    },
    /// List active PTYs
    List {
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
        #[arg(long, help = "Show subscribers for each PTY")]
        verbose: bool,
    },
    /// Create a new PTY
    Create {
        #[arg(long, default_value = "80")]
        cols: u32,
        #[arg(long, default_value = "24")]
        rows: u32,
        #[arg(long)]
        cmd: Option<String>,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
    },
    /// Destroy a PTY
    Destroy {
        pty_id: String,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
    },
    /// Write text to a PTY (appends newline)
    Send {
        pty_id: String,
        text: String,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
    },
    /// Resize a PTY's columns and rows on the server
    Resize {
        pty_id: String,
        cols: u32,
        rows: u32,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
    },
    /// Attach to a running PTY, streaming output to stdout and forwarding stdin
    Attach {
        /// PTY ID to attach to (from `termd list`)
        pty_id: String,
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock or /run/termd/termd.sock]")]
        socket: Option<PathBuf>,
        /// Print message metadata to stderr instead of writing data to stdout
        #[arg(long)]
        debug: bool,
        /// Rendering strategy for terminal output
        #[arg(long, value_enum, default_value_t = attach::RenderMode::Cell)]
        render_mode: attach::RenderMode,
    },
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

        Cmd::List { socket, verbose } => {
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
                            if verbose {
                                println!("TODO");
                            }
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
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
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
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
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

        Cmd::Resize { pty_id, cols, rows, socket } => {
            let mut client = connect_client(socket).await?;
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
            let resp = send_recv(
                &mut client,
                Command::Resize(ResizeRequest { pty_id: pty_id.clone(), cols, rows }),
            ).await?;
            match resp.response {
                Some(Response::Command(c)) => {
                    if c.success {
                        println!("resized {} to {}x{}", pty_id, cols, rows);
                    } else {
                        eprintln!("error: {}", c.error.unwrap_or_default());
                        std::process::exit(1);
                    }
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Attach { pty_id, socket, debug, render_mode } => {
            let mut client = connect_client(socket).await?;
            let item = resolve_pty_item(&mut client, &pty_id).await?;
            attach::run(&mut client, item, debug, render_mode).await?;
        }
    }

    Ok(())
}

async fn resolve_pty_id(client: &mut AuthedClient, prefix: &str) -> Result<String> {
    use termd::proto::terminal_response::Response;

    let resp = send_recv(client, Command::List(ListRequest {})).await?;
    let ids = match resp.response {
        Some(Response::List(l)) => l.items.into_iter().map(|i| i.pty_id).collect::<Vec<_>>(),
        other => return Err(anyhow::anyhow!("unexpected list response: {other:?}")),
    };
    let matches: Vec<_> = ids.iter().filter(|id| id.starts_with(prefix)).collect();
    match matches.len() {
        1 => Ok(matches[0].clone()),
        0 => Err(anyhow::anyhow!("no PTY matches prefix {:?}", prefix)),
        _ => Err(anyhow::anyhow!(
            "ambiguous prefix {:?} matches: {}",
            prefix,
            matches.iter().map(|s| &s[..8]).collect::<Vec<_>>().join(", ")
        )),
    }
}

async fn resolve_pty_item(client: &mut AuthedClient, prefix: &str) -> Result<PtyItem> {
    use termd::proto::terminal_response::Response;

    let resp = send_recv(client, Command::List(ListRequest {})).await?;
    let items = match resp.response {
        Some(Response::List(l)) => l.items,
        other => return Err(anyhow::anyhow!("unexpected list response: {other:?}")),
    };
    let matches: Vec<_> = items.iter().filter(|i| i.pty_id.starts_with(prefix)).collect();
    match matches.len() {
        1 => Ok(matches[0].clone()),
        0 => Err(anyhow::anyhow!("no PTY matches prefix {:?}", prefix)),
        _ => Err(anyhow::anyhow!(
            "ambiguous prefix {:?} matches: {}",
            prefix,
            matches.iter().map(|i| &i.pty_id[..8]).collect::<Vec<_>>().join(", ")
        )),
    }
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
