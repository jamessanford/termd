// Black-box, end-to-end tests that drive termd over a real gRPC connection on a
// unix socket. Unlike integration.rs (which exercises pieces in-process), these
// spin up the full server and talk to it as an external client would.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Request;
use tower::service_fn;
use hyper_util::rt::TokioIo;
use termd::pty::PtyRegistry;
use termd::server::make_service;
use termd::proto::terminal_service_client::TerminalServiceClient;
use termd::proto::terminal_command::Command;
use termd::proto::terminal_response::Response;
use termd::proto::{TerminalCommand, ListRequest, CreateRequest, DestroyRequest};
use termd::proto::{PtyItem, SubscribeRequest, UnsubscribeRequest};

const TEST_TOKEN: &str = "test-token";

// ---------------------------------------------------------------------------
// Black-box multi-client subscription lifecycle test.
//
// This is intentionally built as a small, parameterizable harness so new
// clients / terminals / subscribers can be added by appending calls to the
// scenario body — no new boilerplate required.
//
//   World::connect(name)            -> opens a new long-lived client connection
//   World::create(client, term)     -> `client` creates a terminal, remembered as `term`
//   World::destroy(client, term)    -> `client` destroys a terminal, remembered as `term`
//   World::subscribe(client, term)  -> `client` subscribes to `term`
//   World::unsubscribe(client, term)-> `client` unsubscribes from `term`
//   World::drop_connection(client)  -> abrupt loss of `client`'s connection
//   World::close_connection(client) -> graceful close of `client`'s connection
//   World::verify(&[(term, n), ...])-> spin up a *fresh* gRPC client, ListRequest,
//                                       assert terminal count + per-terminal subscriber
//                                       counts, then drop the fresh client.
//
// Client model: one client == one long-lived bidi `stream` RPC. All of that
// client's subscribe/unsubscribe/create commands are multiplexed over its single
// stream; dropping the stream removes every subscription it held.
//
// `verify` polls until the expected state is reached (or a timeout fires) because
// subscriber removal on disconnect is eventual on the server side.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AuthInterceptor;
impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, tonic::Status> {
        req.metadata_mut().insert("x-auth-token", TEST_TOKEN.parse().unwrap());
        Ok(req)
    }
}

type AuthedClient =
    TerminalServiceClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>;

// Connects a brand-new authenticated gRPC client to the server socket.
async fn connect_grpc(socket: &Path) -> AuthedClient {
    let socket_path = socket.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:1")
        .unwrap()
        .connect_with_connector(service_fn(move |_| {
            let path = socket_path.clone();
            async move { tokio::net::UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await
        .unwrap();
    TerminalServiceClient::with_interceptor(channel, AuthInterceptor)
}

// A running server bound to a unix socket. Holds the TempDir for the test's
// lifetime — dropping it removes the socket and kills the server.
struct TestServer {
    _dir: tempfile::TempDir,
    socket: PathBuf,
}

impl TestServer {
    async fn start() -> TestServer {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("termd.sock");
        let registry = Arc::new(PtyRegistry::new());
        let svc = make_service(registry, false, TEST_TOKEN.to_string());
        let socket_path = socket.clone();
        tokio::spawn(async move {
            let listener = UnixListener::bind(&socket_path).unwrap();
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        TestServer { _dir: dir, socket }
    }
}

// A single long-lived client connection: one bidi `stream` RPC over which all of
// this client's commands are sent. `grpc` is retained to keep the channel alive.
struct Client {
    name: String,
    _grpc: AuthedClient,
    cmd_tx: mpsc::Sender<TerminalCommand>,
    resp: tonic::Streaming<termd::proto::TerminalResponse>,
}

impl Client {
    async fn connect(socket: &Path, name: &str) -> Client {
        let mut grpc = connect_grpc(socket).await;
        let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
        let resp = grpc
            .stream(ReceiverStream::new(cmd_rx))
            .await
            .unwrap()
            .into_inner();
        Client { name: name.to_string(), _grpc: grpc, cmd_tx, resp }
    }

    async fn send(&self, cmd: Command) {
        self.cmd_tx
            .send(TerminalCommand { command: Some(cmd) })
            .await
            .expect("client command stream closed unexpectedly");
    }

    // Reads responses off this client's stream until `f` yields a value, skipping
    // interleaved broadcast traffic (stream data, metadata, etc.).
    async fn drain_until<R>(&mut self, mut f: impl FnMut(Response) -> Option<R>) -> R {
        loop {
            let msg = self.resp.message().await.unwrap().unwrap();
            if let Some(r) = f(msg.response.unwrap()) {
                return r;
            }
        }
    }

    async fn create_terminal(&mut self) -> u64 {
        self.send(Command::Create(CreateRequest { cols: 80, rows: 24, command: None }))
            .await;
        tokio::time::timeout(
            Duration::from_secs(5),
            self.drain_until(|r| match r {
                Response::Create(c) => Some(c.item.unwrap().pty_id),
                _ => None,
            }),
        )
        .await
        .expect("timed out waiting for CreateResponse")
    }

    async fn destroy_terminal(&mut self, pty_id: u64) {
        self.send(Command::Destroy(DestroyRequest { pty_id }))
            .await;
        tokio::time::timeout(
            Duration::from_secs(5),
            self.drain_until(|r| match r {
                Response::Command(c) if c.success && c.pty_id == pty_id => Some(()),
                _ => None,
            }),
        )
        .await
        .expect("timed out waiting for DestroyResponse")
    }

    async fn subscribe(&mut self, pty_id: u64) {
        let hostname = self.name.clone();
        self.send(Command::Subscribe(SubscribeRequest { pty_id, hostname, cols: 80, rows: 24 }))
            .await;
        tokio::time::timeout(
            Duration::from_secs(5),
            self.drain_until(|r| match r {
                Response::Subscribe(s) if s.success && s.pty_id == pty_id => Some(()),
                _ => None,
            }),
        )
        .await
        .expect("timed out waiting for SubscribeResponse");
    }

    // Fire-and-forget: removal is observed by the next `verify` poll.
    async fn unsubscribe(&self, pty_id: u64) {
        self.send(Command::Unsubscribe(UnsubscribeRequest { pty_id })).await;
    }
}

// One-shot: open a fresh gRPC client, send a single ListRequest, return the items,
// then drop the client (the whole connection is torn down when this returns).
async fn fresh_list(socket: &Path) -> Vec<PtyItem> {
    let mut grpc = connect_grpc(socket).await;
    let (tx, rx) = mpsc::channel::<TerminalCommand>(1);
    tx.send(TerminalCommand { command: Some(Command::List(ListRequest {})) })
        .await
        .unwrap();
    drop(tx);
    let mut stream = grpc.stream(ReceiverStream::new(rx)).await.unwrap().into_inner();
    loop {
        match stream.message().await.unwrap().unwrap().response.unwrap() {
            Response::List(l) => return l.items,
            _ => continue,
        }
    }
}

struct World {
    server: TestServer,
    clients: HashMap<String, Client>,
    terminals: HashMap<String, u64>, // logical name -> pty_id
}

impl World {
    async fn new() -> World {
        World { server: TestServer::start().await, clients: HashMap::new(), terminals: HashMap::new() }
    }

    async fn connect(&mut self, name: &str) {
        let client = Client::connect(&self.server.socket, name).await;
        self.clients.insert(name.to_string(), client);
    }

    async fn create(&mut self, client: &str, term: &str) {
        let c = self.clients.get_mut(client).expect("unknown client");
        let pty_id = c.create_terminal().await;
        self.terminals.insert(term.to_string(), pty_id);
    }

    async fn destroy(&mut self, client: &str, term: &str) {
        let pty_id = *self.terminals.get(term).expect("unknown terminal");
        self.clients.get_mut(client).expect("unknown client").destroy_terminal(pty_id).await;
        self.terminals.remove(term);
    }

    async fn subscribe(&mut self, client: &str, term: &str) {
        let pty_id = *self.terminals.get(term).expect("unknown terminal");
        self.clients.get_mut(client).expect("unknown client").subscribe(pty_id).await;
    }

    async fn unsubscribe(&self, client: &str, term: &str) {
        let pty_id = *self.terminals.get(term).expect("unknown terminal");
        self.clients.get(client).expect("unknown client").unsubscribe(pty_id).await;
    }

    // Abrupt connection loss: drop everything immediately.
    fn drop_connection(&mut self, client: &str) {
        self.clients.remove(client).expect("unknown client");
    }

    // Graceful close: half-close the command stream first, then drop.
    async fn close_connection(&mut self, client: &str) {
        let c = self.clients.remove(client).expect("unknown client");
        drop(c.cmd_tx); // signal end-of-commands to the server
        // `c.resp` / `c._grpc` drop here, tearing down the connection.
    }

    // Spin up a fresh gRPC client, ListRequest, and assert the terminal count and
    // each terminal's subscriber count. `expected` is &[(terminal_name, sub_count)].
    // Polls to absorb eventual-consistency on disconnect-driven removals.
    async fn verify(&self, expected: &[(&str, usize)]) {
        let want: Vec<(u64, usize)> = expected
            .iter()
            .map(|(name, cnt)| (*self.terminals.get(*name).expect("unknown terminal"), *cnt))
            .collect();

        let converged = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let items = fresh_list(&self.server.socket).await;
                if Self::matches(&items, &want) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;

        if converged.is_err() {
            let items = fresh_list(&self.server.socket).await;
            let actual: BTreeMap<u64, usize> =
                items.iter().map(|i| (i.pty_id, i.subscribers.len())).collect();
            let want_map: BTreeMap<u64, usize> = want.iter().cloned().collect();
            panic!(
                "VERIFY failed: wanted {} terminal(s) {:?}, got {} terminal(s) {:?}",
                want.len(),
                want_map,
                items.len(),
                actual,
            );
        }
    }

    fn matches(items: &[PtyItem], want: &[(u64, usize)]) -> bool {
        if items.len() != want.len() {
            return false;
        }
        want.iter().all(|(pid, cnt)| {
            items.iter().any(|i| i.pty_id == *pid && i.subscribers.len() == *cnt)
        })
    }
}

#[tokio::test]
async fn test_multi_client_subscription_lifecycle() {
    let mut w = World::new().await;

    // A, B, C connect.
    w.connect("A").await;
    w.connect("B").await;
    w.connect("C").await;

    // A: Create terminal X
    w.create("A", "X").await;
    w.verify(&[("X", 0)]).await;

    // A: Subscribe to terminal X
    w.subscribe("A", "X").await;
    w.verify(&[("X", 1)]).await;

    // A: Unsubscribe from terminal X
    w.unsubscribe("A", "X").await;
    w.verify(&[("X", 0)]).await;

    // A: Create terminal Y
    w.create("A", "Y").await;
    w.verify(&[("X", 0), ("Y", 0)]).await;

    // A: Subscribe to terminal Y
    w.subscribe("A", "Y").await;
    w.verify(&[("X", 0), ("Y", 1)]).await;

    // A: Unsubscribe from terminal Y
    w.unsubscribe("A", "Y").await;
    w.verify(&[("X", 0), ("Y", 0)]).await;

    // A: Subscribe to terminal X  (A is left open, subscribed to X)
    w.subscribe("A", "X").await;
    w.verify(&[("X", 1), ("Y", 0)]).await;

    w.connect("D").await;
    w.create("D", "Z").await;
    w.subscribe("D", "Z").await;
    w.connect("E").await;
    w.subscribe("E", "X").await;
    w.unsubscribe("E", "X").await;
    w.subscribe("E", "Z").await;
    w.destroy("E", "Z").await;
    w.close_connection("D").await;
    w.close_connection("E").await;
    w.connect("D").await;
    w.subscribe("D", "X").await;
    w.unsubscribe("D", "X").await;
    w.subscribe("D", "Y").await;
    w.unsubscribe("D", "Y").await;

    w.verify(&[("X", 1), ("Y", 0)]).await;

    // B: Subscribe to terminal X
    w.subscribe("B", "X").await;
    w.verify(&[("X", 2), ("Y", 0)]).await;

    // B: Unsubscribe from terminal X, then B: Subscribe to terminal Y
    // (NOTE: no VERIFY between these two steps)
    w.unsubscribe("B", "X").await;
    w.subscribe("B", "Y").await;
    w.verify(&[("X", 1), ("Y", 1)]).await;

    // C: Create terminal Z, then C: Subscribe terminal Z
    w.create("C", "Z").await;
    w.subscribe("C", "Z").await;
    w.verify(&[("X", 1), ("Y", 1), ("Z", 1)]).await;

    // C: Unsubscribe terminal Z, then C: Subscribe terminal X
    w.unsubscribe("C", "Z").await;
    w.subscribe("C", "X").await;
    w.verify(&[("X", 2), ("Y", 1), ("Z", 0)]).await;

    // A: Suddenly drop connection A  (A was subscribed to X)
    w.drop_connection("A");
    // B: Unsubscribe terminal Y, then B: Close connection
    w.unsubscribe("B", "Y").await;
    w.close_connection("B").await;
    w.verify(&[("X", 1), ("Y", 0), ("Z", 0)]).await;

    // C: Unsubscribe terminal X, then C: Close connection
    w.unsubscribe("C", "X").await;
    w.close_connection("C").await;
    w.verify(&[("X", 0), ("Y", 0), ("Z", 0)]).await;
}
