use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tonic::Request;

use termd::{
    proto::{
        CreateRequest, DestroyRequest, ListRequest, PtyItem, ResizeRequest,
        Size,
        terminal_service_client::TerminalServiceClient,
    },
    auth,
    client,
    pty::PtyRegistry,
    server,
};

/// A `std::io::Write` wrapper that attempts the underlying write but never
/// reports an error. Used for the daemon's tracing output so that a stdout/stderr
/// gone bad (EIO after the controlling terminal disappears) degrades to dropped
/// log lines instead of a panic that would tear down the logging task. See the
/// `with_writer` call in `Cmd::Start` for the full rationale.
struct IgnoreWriteErrors<W>(W);

impl<W: std::io::Write> std::io::Write for IgnoreWriteErrors<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let _ = self.0.write_all(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let _ = self.0.flush();
        Ok(())
    }
}

mod attach;

fn default_socket() -> PathBuf {
    let base = std::env::var("XDG_RUNTIME_DIR")
        .or_else(|_| std::env::var("TMPDIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"));
    base.join("termd.sock")
}

#[derive(Args)]
struct ConnectionArgs {
    #[arg(long, default_value_os_t = default_socket(), conflicts_with = "endpoint")]
    socket: PathBuf,
    /// Endpoint URI to connect to (e.g. https://192.0.2.7:7777#sha256:<hex>,
    /// or http://127.0.0.1:7777 for a --tls=off daemon)
    #[arg(long, env = "TERMD_ENDPOINT", conflicts_with = "socket")]
    endpoint: Option<String>,
    /// Auth token for TCP connections (see `termd token`)
    #[arg(long, env = "TERMD_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Pin the server's self-signed certificate by fingerprint (sha256:<hex>,
    /// see `termd token`); may also be given as the endpoint's #fragment.
    /// Without it, https endpoints are verified against the system roots.
    #[arg(long, env = "TERMD_FINGERPRINT")]
    fingerprint: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum TlsMode {
    /// Serve TLS with a persistent self-signed certificate
    On,
    /// Plaintext h2c, for running behind a TLS-terminating proxy like caddy
    Off,
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
        #[arg(long, default_value = "0.0.0.0:7777")]
        listen: SocketAddr,
        #[arg(long, default_value_os_t = default_socket())]
        socket: PathBuf,
        /// TLS on the TCP listener
        #[arg(long, value_enum, default_value_t = TlsMode::On)]
        tls: TlsMode,
    },
    /// Print ready-to-run `termd attach` command lines carrying the TCP auth
    /// token and certificate fingerprint (via the socket only)
    Token {
        #[arg(long, default_value_os_t = default_socket())]
        socket: PathBuf,
        /// Replace the token with a new one; the old one stops working
        #[arg(long)]
        refresh: bool,
        /// Print TERMD_TOKEN= / TERMD_FINGERPRINT= lines instead of commands
        #[arg(long)]
        raw: bool,
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
        #[arg(long, value_enum, default_value_t = attach::RenderMode::Autowrap)]
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
    /// Print a PTY's screen contents
    Dump {
        pty_id: String,
        /// Rows to print, counting back from the bottom of the screen
        /// (default: the PTY's height; more reaches into scrollback)
        #[arg(long)]
        rows: Option<u32>,
        #[command(flatten)]
        conn: ConnectionArgs,
    },
}

pub(crate) type ClientInterceptor =
    Box<dyn FnMut(Request<()>) -> Result<Request<()>, tonic::Status> + Send>;

pub(crate) type AuthedClient = TerminalServiceClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, ClientInterceptor>,
>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Cmd::Start { log_grpc, listen, socket, tls } => {
            let level = if log_grpc { "debug" } else { "info" };
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| level.into()),
                )
                // Write logs through a wrapper that never surfaces write errors.
                // A backgrounded daemon can outlive its controlling terminal
                // (started in a shell that later closes); its stdout/stderr then
                // fail every write with EIO. tracing-subscriber reacts to a writer
                // error by falling back to eprintln!, which itself panics when
                // stderr is also broken ("failed printing to stderr"). That panic
                // unwinds whatever task happened to be logging — including a
                // connection's disconnect-cleanup path, which leaks that client's
                // subscriber entries. Swallowing the error makes logging a silent
                // no-op instead of a process-corrupting panic.
                .with_writer(|| IgnoreWriteErrors(std::io::stdout()))
                .init();

            if let Some(parent) = socket.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let state = auth::state_dir();
            let tokens = auth::TokenStore::load_or_create(&state)?;
            let cert = match tls {
                TlsMode::On => Some(auth::load_or_create_cert(&state)?),
                TlsMode::Off => {
                    if !listen.ip().is_loopback() {
                        tracing::warn!(%listen, "--tls=off on a non-loopback address: token and traffic are sent in the clear");
                    }
                    None
                }
            };

            let registry = Arc::new(PtyRegistry::new());
            server::serve(registry, &socket, listen, tokens, cert, log_grpc).await?;
        }

        Cmd::Token { socket, refresh, raw } => {
            use termd::proto::{admin_service_client::AdminServiceClient, TokenRequest};
            let channel = client::socket_channel(socket).await?;
            let resp = AdminServiceClient::new(channel)
                .token(TokenRequest { refresh })
                .await?
                .into_inner();
            if raw {
                println!("TERMD_TOKEN={}", resp.token);
                if !resp.fingerprint.is_empty() {
                    println!("TERMD_FINGERPRINT={}", resp.fingerprint);
                }
                return Ok(());
            }
            anyhow::ensure!(
                !resp.listen.is_empty(),
                "daemon didn't report its listen address; it predates this client, restart it"
            );
            let listen: SocketAddr = resp.listen.parse()
                .with_context(|| format!("daemon reported bad listen address {:?}", resp.listen))?;
            let (scheme, fragment) = match resp.fingerprint.as_str() {
                "" => ("http", String::new()),
                fp => ("https", format!("#{fp}")),
            };
            // Single-quoted: zsh's extendedglob treats `#` as a glob operator.
            for host in advertised_hosts(listen) {
                println!(
                    "TERMD_TOKEN={} termd attach --endpoint '{scheme}://{}{fragment}'",
                    resp.token,
                    SocketAddr::new(host, listen.port()),
                );
            }
        }

        Cmd::List { conn, verbose } => {
            let mut client = connect_client(conn).await?;
            let mut items = client.list(ListRequest {}).await?.into_inner().items;
            if items.is_empty() {
                println!("No active PTYs.");
            } else {
                items.sort_by_key(|p| p.sort_order);
                println!("{:>3} {:<16} {:>5} {:>5}  TITLE", "#", "ID", "COLS", "ROWS");
                for item in items {
                    let (cols, rows) = attach::item_size(&item);
                    println!(
                        "{:>3} {:016x} {:>5} {:>5}  {}",
                        item.sort_order, item.pty_id, cols, rows, item.title
                    );
                    if verbose {
                        for sub in &item.subscribers {
                            let s = sub.size.unwrap_or_default();
                            println!(
                                "    ( {:<36} {} {}x{} )",
                                sub.subscriber_id, sub.hostname, s.cols, s.rows
                            );
                        }
                    }
                }
            }
        }

        Cmd::Create { cols, rows, cmd, conn } => {
            let mut client = connect_client(conn).await?;
            let item = client.create(CreateRequest {
                size: Some(Size { cols, rows }),
                command: cmd,
            }).await?.into_inner();
            println!("{:016x}", item.pty_id);
        }

        Cmd::Destroy { pty_id, conn } => {
            let mut client = connect_client(conn).await?;
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
            match client.destroy(DestroyRequest { pty_id }).await {
                Ok(_) => println!("destroyed {:016x}", pty_id),
                Err(status) => {
                    eprintln!("error: {}", status.message());
                    std::process::exit(1);
                }
            }
        }

        Cmd::Send { pty_id, text, conn } => {
            let mut client = connect_client(conn).await?;
            let item = resolve_pty_item(&mut client, &pty_id).await?;
            let (cols, rows) = attach::item_size(&item);
            // No standalone write RPC remains: open a Subscribe stream, send the
            // bytes as a Write frame, then close it.
            use tokio::sync::mpsc;
            use tokio_stream::wrappers::ReceiverStream;
            use termd::proto::{subscribe_frame::Frame, SubscribeFrame, SubscribeStart, WriteData};
            let (tx, rx) = mpsc::channel::<SubscribeFrame>(2);
            tx.send(SubscribeFrame {
                frame: Some(Frame::Start(SubscribeStart {
                    pty_id: item.pty_id,
                    hostname: hostname::get().unwrap_or_default().to_string_lossy().into_owned(),
                    size: Some(Size { cols, rows }),
                    keep_on_exit: None,
                })),
            }).await?;
            tx.send(SubscribeFrame {
                frame: Some(Frame::Write(WriteData { data: text.into_bytes() })),
            }).await?;
            let mut events = client.subscribe(ReceiverStream::new(rx)).await?.into_inner();
            // Wait for Ready so the server has accepted the stream.
            let _ = events.message().await?;
            // Half-close the up-stream and drain events until the server ends
            // the response stream. The server reads inbound frames in order and
            // closes on half-close, so stream end confirms the Write reached
            // the PTY. A bare drop would RST the stream, and h2 >= 0.4.15
            // discards buffered DATA when a reset is scheduled — losing the
            // Write. Timeout-capped so a wedged server can't hang us; that's
            // an error, since delivery is then unconfirmed.
            drop(tx);
            let drain = async {
                while events.message().await?.is_some() {}
                Ok::<(), tonic::Status>(())
            };
            match tokio::time::timeout(std::time::Duration::from_secs(2), drain).await {
                Ok(r) => r?,
                Err(_) => anyhow::bail!("timed out waiting for the server to confirm delivery"),
            }
        }

        Cmd::Dump { pty_id, rows, conn } => {
            use termd::proto::{ScrollbackOpKind, ScrollbackRequest};
            let mut client = connect_client(conn).await?;
            let item = resolve_pty_item(&mut client, &pty_id).await?;
            let (_, pty_rows) = attach::item_size(&item);
            let row_count = rows.unwrap_or(pty_rows).max(1);
            // One-shot snapshot via the scrollback viewport: OPEN pins at the
            // live tail and returns the bottom `row_count` rows; CLOSE removes
            // the pin. The subscriber_id is only a pin key server-side, so no
            // Subscribe stream is needed.
            let pin = format!("dump-{}", std::process::id());
            let sr = client.scrollback(ScrollbackRequest {
                pty_id: item.pty_id,
                subscriber_id: pin.clone(),
                kind: ScrollbackOpKind::ScrollbackOpen as i32,
                amount: 0,
                row_count,
            }).await?.into_inner();
            let _ = client.scrollback(ScrollbackRequest {
                pty_id: item.pty_id,
                subscriber_id: pin,
                kind: ScrollbackOpKind::ScrollbackClose as i32,
                amount: 0,
                row_count: 0,
            }).await;
            use std::io::Write;
            let mut out = std::io::stdout().lock();
            out.write_all(&sr.data)?;
            if !sr.data.ends_with(b"\n") {
                out.write_all(b"\n")?;
            }
        }

        Cmd::Resize { pty_id, cols, rows, conn } => {
            let mut client = connect_client(conn).await?;
            let pty_id = resolve_pty_id(&mut client, &pty_id).await?;
            match client.resize(ResizeRequest {
                pty_id,
                size: Some(Size { cols, rows }),
            }).await {
                Ok(_) => println!("resized {:016x} to {}x{}", pty_id, cols, rows),
                Err(status) => {
                    eprintln!("error: {}", status.message());
                    std::process::exit(1);
                }
            }
        }

        Cmd::Attach { pty_id, conn, debug, render_mode } => {
            let mut client = connect_client(conn).await?;
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
    resolve_pty_item(client, prefix).await.map(|i| i.pty_id)
}

async fn resolve_pty_item(client: &mut AuthedClient, prefix: &str) -> Result<PtyItem> {
    let items = client.list(ListRequest {}).await?.into_inner().items;
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
    let items = client.list(ListRequest {}).await?.into_inner().items;
    if let Some(best) = pick_best_pty(&items) {
        return Ok(best.clone());
    }
    let (cols, rows) = attach::get_terminal_size();
    let item = client.create(CreateRequest {
        size: Some(Size { cols, rows }),
        command: None,
    }).await?.into_inner();
    Ok(item)
}

/// Addresses to suggest for reaching a listener: the bound IP itself, or for
/// a wildcard bind, every non-loopback, non-link-local interface address.
fn advertised_hosts(listen: SocketAddr) -> Vec<std::net::IpAddr> {
    use std::net::IpAddr;
    if !listen.ip().is_unspecified() {
        return vec![listen.ip()];
    }
    let want_v6 = listen.is_ipv6();
    let mut hosts: Vec<IpAddr> = nix::ifaddrs::getifaddrs()
        .into_iter()
        .flatten()
        .filter_map(|ifa| {
            let addr = ifa.address?;
            if let Some(v4) = addr.as_sockaddr_in() {
                Some(IpAddr::V4(v4.ip()))
            } else {
                addr.as_sockaddr_in6().map(|v6| IpAddr::V6(v6.ip()))
            }
        })
        .filter(|ip| !ip.is_loopback())
        .filter(|ip| match ip {
            // A [::] bind is dual-stack on Linux; a 0.0.0.0 bind is v4 only.
            IpAddr::V4(v4) => !v4.is_link_local(),
            IpAddr::V6(v6) => want_v6 && !v6.is_unicast_link_local(),
        })
        .collect();
    hosts.dedup();
    if hosts.is_empty() {
        hosts.push(if want_v6 { std::net::Ipv6Addr::LOCALHOST.into() } else { std::net::Ipv4Addr::LOCALHOST.into() });
    }
    hosts
}

async fn connect_client(conn: ConnectionArgs) -> Result<AuthedClient> {
    let channel = match &conn.endpoint {
        None => client::socket_channel(conn.socket).await?,
        Some(uri) => client::tcp_channel(uri, conn.fingerprint.as_deref()).await?,
    };
    let interceptor: ClientInterceptor = match conn.token {
        Some(token) => {
            let value: tonic::metadata::MetadataValue<_> =
                token.trim().parse().context("token is not a valid header value")?;
            Box::new(move |mut req: Request<()>| {
                req.metadata_mut().insert("x-auth-token", value.clone());
                Ok(req)
            })
        }
        None => Box::new(|req: Request<()>| Ok(req)),
    };
    Ok(TerminalServiceClient::with_interceptor(channel, interceptor))
}
