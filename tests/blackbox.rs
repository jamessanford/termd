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
use termd::proto::{
    ListRequest, CreateRequest, DestroyRequest, Size,
    PtyItem, SubscribeFrame, SubscribeEvent, SubscribeStart,
    subscribe_frame, subscribe_event,
};

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
// Client model: one client == one authed gRPC channel. Under the new protocol
// each subscription is its OWN per-PTY Subscribe stream; a client holds one such
// stream per terminal it is subscribed to. Unsubscribe = drop that stream.
// Control ops (create/destroy/list) are unary calls on the channel. Dropping the
// whole client tears down every Subscribe stream it held (i.e. all its subs).
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

// One held subscription: the per-PTY Subscribe stream's up-channel and its event
// stream. Dropping this drops the stream, which unsubscribes on the server.
struct Subscription {
    _frame_tx: mpsc::Sender<SubscribeFrame>,
    _events: tonic::Streaming<SubscribeEvent>,
}

// A single client: an authed gRPC channel plus the set of Subscribe streams it
// currently holds, keyed by pty_id. Control ops are unary; each subscription is
// its own stream.
struct Client {
    name: String,
    grpc: AuthedClient,
    subs: HashMap<u64, Subscription>,
}

impl Client {
    async fn connect(socket: &Path, name: &str) -> Client {
        let grpc = connect_grpc(socket).await;
        Client { name: name.to_string(), grpc, subs: HashMap::new() }
    }

    async fn create_terminal(&mut self) -> u64 {
        let item = tokio::time::timeout(
            Duration::from_secs(5),
            self.grpc.create(CreateRequest { size: Some(Size { cols: 80, rows: 24 }), command: None }),
        )
        .await
        .expect("timed out waiting for Create")
        .expect("create failed")
        .into_inner();
        item.pty_id
    }

    async fn destroy_terminal(&mut self, pty_id: u64) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.grpc.destroy(DestroyRequest { pty_id }),
        )
        .await
        .expect("timed out waiting for Destroy")
        .expect("destroy failed");
    }

    // Open a per-PTY Subscribe stream, send Start, await Ready, and retain the
    // stream so the subscription stays live until `unsubscribe`/client drop.
    async fn subscribe(&mut self, pty_id: u64) {
        let (frame_tx, frame_rx) = mpsc::channel::<SubscribeFrame>(16);
        frame_tx.send(SubscribeFrame {
            frame: Some(subscribe_frame::Frame::Start(SubscribeStart {
                pty_id,
                hostname: self.name.clone(),
                size: Some(Size { cols: 80, rows: 24 }),
            })),
        }).await.unwrap();

        let mut events = tokio::time::timeout(
            Duration::from_secs(5),
            self.grpc.subscribe(ReceiverStream::new(frame_rx)),
        )
        .await
        .expect("timed out opening Subscribe stream")
        .expect("subscribe failed")
        .into_inner();

        match tokio::time::timeout(Duration::from_secs(5), events.message()).await {
            Ok(Ok(Some(SubscribeEvent { event: Some(subscribe_event::Event::Ready(_)) }))) => {}
            other => panic!("expected Ready as first subscribe event, got {other:?}"),
        }

        self.subs.insert(pty_id, Subscription { _frame_tx: frame_tx, _events: events });
    }

    // Drop the per-PTY Subscribe stream; removal is observed by the next `verify`.
    fn unsubscribe(&mut self, pty_id: u64) {
        self.subs.remove(&pty_id);
    }
}

// One-shot: open a fresh gRPC client, unary List, return the items, then drop it.
async fn fresh_list(socket: &Path) -> Vec<PtyItem> {
    let mut grpc = connect_grpc(socket).await;
    grpc.list(ListRequest {}).await.unwrap().into_inner().items
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

    fn unsubscribe(&mut self, client: &str, term: &str) {
        let pty_id = *self.terminals.get(term).expect("unknown terminal");
        self.clients.get_mut(client).expect("unknown client").unsubscribe(pty_id);
    }

    // Abrupt connection loss: drop the client (and all its Subscribe streams).
    fn drop_connection(&mut self, client: &str) {
        self.clients.remove(client).expect("unknown client");
    }

    // Graceful close: under the per-PTY-stream model there is no single command
    // stream to half-close; dropping the client tears down every Subscribe stream
    // it held and closes the channel.
    fn close_connection(&mut self, client: &str) {
        self.clients.remove(client).expect("unknown client");
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
    w.unsubscribe("A", "X");
    w.verify(&[("X", 0)]).await;

    // A: Create terminal Y
    w.create("A", "Y").await;
    w.verify(&[("X", 0), ("Y", 0)]).await;

    // A: Subscribe to terminal Y
    w.subscribe("A", "Y").await;
    w.verify(&[("X", 0), ("Y", 1)]).await;

    // A: Unsubscribe from terminal Y
    w.unsubscribe("A", "Y");
    w.verify(&[("X", 0), ("Y", 0)]).await;

    // A: Subscribe to terminal X  (A is left open, subscribed to X)
    w.subscribe("A", "X").await;
    w.verify(&[("X", 1), ("Y", 0)]).await;

    w.connect("D").await;
    w.create("D", "Z").await;
    w.subscribe("D", "Z").await;
    w.connect("E").await;
    w.subscribe("E", "X").await;
    w.unsubscribe("E", "X");
    w.subscribe("E", "Z").await;
    w.destroy("E", "Z").await;
    w.close_connection("D");
    w.close_connection("E");
    w.connect("D").await;
    w.subscribe("D", "X").await;
    w.unsubscribe("D", "X");
    w.subscribe("D", "Y").await;
    w.unsubscribe("D", "Y");

    w.verify(&[("X", 1), ("Y", 0)]).await;

    // B: Subscribe to terminal X
    w.subscribe("B", "X").await;
    w.verify(&[("X", 2), ("Y", 0)]).await;

    // B: Unsubscribe from terminal X, then B: Subscribe to terminal Y
    // (NOTE: no VERIFY between these two steps)
    w.unsubscribe("B", "X");
    w.subscribe("B", "Y").await;
    w.verify(&[("X", 1), ("Y", 1)]).await;

    // C: Create terminal Z, then C: Subscribe terminal Z
    w.create("C", "Z").await;
    w.subscribe("C", "Z").await;
    w.verify(&[("X", 1), ("Y", 1), ("Z", 1)]).await;

    // C: Unsubscribe terminal Z, then C: Subscribe terminal X
    w.unsubscribe("C", "Z");
    w.subscribe("C", "X").await;
    w.verify(&[("X", 2), ("Y", 1), ("Z", 0)]).await;

    // A: Suddenly drop connection A  (A was subscribed to X)
    w.drop_connection("A");
    // B: Unsubscribe terminal Y, then B: Close connection
    w.unsubscribe("B", "Y");
    w.close_connection("B");
    w.verify(&[("X", 1), ("Y", 0), ("Z", 0)]).await;

    // C: Unsubscribe terminal X, then C: Close connection
    w.unsubscribe("C", "X");
    w.close_connection("C");
    w.verify(&[("X", 0), ("Y", 0), ("Z", 0)]).await;
}
