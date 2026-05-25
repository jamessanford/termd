use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
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
        .or_else(|_| std::env::var("TMPDIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"));
    base.join("termd.sock")
}

enum Destination {
    Socket(PathBuf),
    Tcp(String),
}

#[derive(Args)]
struct ConnectionArgs {
    #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock]")]
    socket: Option<PathBuf>,
    #[arg(long, help = "Endpoint URI to connect to (e.g. http://127.0.0.1:7777 or https://host:7777)")]
    endpoint: Option<String>,
}

impl ConnectionArgs {
    fn destination(self) -> Destination {
        if let Some(ep) = self.endpoint {
            Destination::Tcp(ep)
        } else {
            Destination::Socket(self.socket.unwrap_or_else(default_socket))
        }
    }
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
        #[arg(long, help = "Unix socket path [default: $XDG_RUNTIME_DIR/termd.sock]")]
        socket: Option<PathBuf>,
    },
    /// Attach to a PTY and act as multiplexer
    Attach {
        /// PTY ID to attach to (from `termd list`)
        pty_id: Option<String>,
        #[command(flatten)]
        conn: ConnectionArgs,
        /// Print message metadata to stderr instead of writing data to stdout
        #[arg(long)]
        debug: bool,
        /// Rendering strategy for terminal output
        #[arg(long, value_enum, default_value_t = attach::RenderMode::Region)]
        render_mode: attach::RenderMode,
    },
    /// List active PTYs
    List {
        #[command(flatten)]
        conn: ConnectionArgs,
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
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    /// Destroy a PTY
    Destroy {
        pty_id: String,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    /// Resize a PTY's columns and rows on the server
    Resize {
        pty_id: String,
        cols: u32,
        rows: u32,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
    /// Inject text to a PTY
    Send {
        pty_id: String,
        text: String,
        #[command(flatten)]
        conn: ConnectionArgs,
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

        Cmd::List { conn, verbose } => {
            let mut client = connect_client(conn.destination()).await?;
            let resp = send_recv(&mut client, Command::List(ListRequest {})).await?;
            match resp.response {
                Some(Response::List(l)) => {
                    if l.items.is_empty() {
                        println!("No active PTYs.");
                    } else {
                        println!("{:<16} {:>5} {:>5}  {}", "ID", "COLS", "ROWS", "TITLE");
                        for item in l.items {
                            println!(
                                "{:016x} {:>5} {:>5}  {}",
                                item.pty_id, item.cols, item.rows, item.title
                            );
                            if verbose {
                                for sub in &item.subscribers {
                                    println!(
                                        "  ( {:<36} {} {}x{} )",
                                        sub.subscriber_id, sub.hostname, sub.cols, sub.rows
                                    );
                                }
                            }
                        }
                    }
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Create { cols, rows, cmd, conn } => {
            let mut client = connect_client(conn.destination()).await?;
            let resp = send_recv(
                &mut client,
                Command::Create(CreateRequest { cols, rows, command: cmd }),
            )
            .await?;
            match resp.response {
                Some(Response::Create(c)) => {
                    println!("{}", c.item.map(|i| format!("{:016x}", i.pty_id)).unwrap_or_default());
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Destroy { pty_id, conn } => {
            let mut client = connect_client(conn.destination()).await?;
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
            let resp = send_recv(
                &mut client,
                Command::Destroy(DestroyRequest { pty_id }),
            )
            .await?;
            match resp.response {
                Some(Response::Command(c)) => {
                    if c.success {
                        println!("destroyed {:016x}", pty_id);
                    } else {
                        eprintln!("error: {}", c.error.unwrap_or_default());
                        std::process::exit(1);
                    }
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Send { pty_id, text, conn } => {
            let mut client = connect_client(conn.destination()).await?;
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
            let data = text.into_bytes();
            let resp = send_recv(&mut client, Command::Write(WriteRequest { pty_id, data })).await?;
            match resp.response {
                Some(Response::Command(c)) if !c.success => {
                    eprintln!("error: {}", c.error.unwrap_or_default());
                    std::process::exit(1);
                }
                _ => {}
            }
        }

        Cmd::Resize { pty_id, cols, rows, conn } => {
            let mut client = connect_client(conn.destination()).await?;
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
            let resp = send_recv(
                &mut client,
                Command::Resize(ResizeRequest { pty_id, cols, rows }),
            ).await?;
            match resp.response {
                Some(Response::Command(c)) => {
                    if c.success {
                        println!("resized {:016x} to {}x{}", pty_id, cols, rows);
                    } else {
                        eprintln!("error: {}", c.error.unwrap_or_default());
                        std::process::exit(1);
                    }
                }
                other => eprintln!("unexpected response: {other:?}"),
            }
        }

        Cmd::Attach { pty_id, conn, debug, render_mode } => {
            let mut client = connect_client(conn.destination()).await?;
            let item = match pty_id {
                Some(prefix) => resolve_pty_item(&mut client, &prefix).await?,
                None => auto_select_or_create(&mut client).await?,
            };
            attach::run(&mut client, item, debug, render_mode).await?;
        }
    }

    Ok(())
}

async fn resolve_pty_id(client: &mut AuthedClient, prefix: &str) -> Result<u64> {
    use termd::proto::terminal_response::Response;

    let resp = send_recv(client, Command::List(ListRequest {})).await?;
    let ids = match resp.response {
        Some(Response::List(l)) => l.items.into_iter().map(|i| i.pty_id).collect::<Vec<u64>>(),
        other => return Err(anyhow::anyhow!("unexpected list response: {other:?}")),
    };
    let prefix_lower = prefix.to_ascii_lowercase();
    let matches: Vec<_> = ids.iter().filter(|&&id| format!("{:016x}", id).starts_with(&prefix_lower)).collect();
    match matches.len() {
        1 => Ok(*matches[0]),
        0 => Err(anyhow::anyhow!("no PTY matches prefix {:?}", prefix)),
        _ => Err(anyhow::anyhow!(
            "ambiguous prefix {:?} matches: {}",
            prefix,
            matches.iter().map(|&&id| format!("{:016x}", id)[..8].to_string()).collect::<Vec<_>>().join(", ")
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
    let prefix_lower = prefix.to_ascii_lowercase();
    let matches: Vec<_> = items.iter().filter(|i| format!("{:016x}", i.pty_id).starts_with(&prefix_lower)).collect();
    match matches.len() {
        1 => Ok(matches[0].clone()),
        0 => Err(anyhow::anyhow!("no PTY matches prefix {:?}", prefix)),
        _ => Err(anyhow::anyhow!(
            "ambiguous prefix {:?} matches: {}",
            prefix,
            matches.iter().map(|i| format!("{:08x}", i.pty_id)).collect::<Vec<_>>().join(", ")
        )),
    }
}

fn pick_best_pty(items: &[PtyItem]) -> Option<&PtyItem> {
    items.iter().max_by_key(|p| {
        let ts = p.last_subscribed_at.as_ref().or(p.created_at.as_ref());
        ts.map(|t| (t.seconds, t.nanos)).unwrap_or((0, 0))
    })
}

async fn auto_select_or_create(client: &mut AuthedClient) -> Result<PtyItem> {
    let resp = send_recv(client, Command::List(ListRequest {})).await?;
    let items = match resp.response {
        Some(Response::List(l)) => l.items,
        other => return Err(anyhow::anyhow!("unexpected list response: {other:?}")),
    };
    if let Some(best) = pick_best_pty(&items) {
        return Ok(best.clone());
    }
    let (cols, rows) = attach::get_terminal_size();
    let resp = send_recv(
        client,
        Command::Create(CreateRequest { cols, rows, command: None }),
    ).await?;
    match resp.response {
        Some(Response::Create(c)) => c.item.ok_or_else(|| anyhow::anyhow!("server returned empty CreateResponse")),
        other => Err(anyhow::anyhow!("unexpected create response: {other:?}")),
    }
}

async fn connect_client(dest: Destination) -> Result<AuthedClient> {
    let channel = match dest {
        Destination::Socket(path) => {
            use hyper_util::rt::TokioIo;
            use tonic::transport::Endpoint;
            use tower::service_fn;
            Endpoint::try_from("http://[::]:1")?
                .connect_with_connector(service_fn(move |_| {
                    let path = path.clone();
                    async move { tokio::net::UnixStream::connect(path).await.map(TokioIo::new) }
                }))
                .await?
        }
        Destination::Tcp(uri) => {
            tonic::transport::Channel::from_shared(uri)?
                .connect()
                .await?
        }
    };
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
