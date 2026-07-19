use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Request;
use tower::service_fn;
use hyper_util::rt::TokioIo;
use termd::pty::PtyRegistry;
use termd::pty::{MetadataReason, PtyEvent};
use termd::server::make_service;
use termd::proto::terminal_service_client::TerminalServiceClient;
use termd::proto::{
    ListRequest, CreateRequest, DestroyRequest, ResizeRequest, RefreshRequest,
    ScrollbackRequest, ScrollbackOpKind, Size,
    SubscribeFrame, SubscribeEvent, SubscribeStart, WriteData,
    subscribe_frame, subscribe_event, stream_metadata,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const TEST_TOKEN: &str = "test-token";

// A live Subscribe stream: the up-channel (frame_tx) and the down-channel
// (event stream), plus the server-assigned subscriber_id learned from the
// first Ready event. Dropping `frame_tx` (and the event stream) unsubscribes.
struct Sub {
    frame_tx: mpsc::Sender<SubscribeFrame>,
    events: tonic::Streaming<SubscribeEvent>,
    subscriber_id: String,
}

// Open a Subscribe stream for `pty_id` at the given size, send the mandatory
// Start frame, and read the first event, asserting it is Ready. Returns the
// live stream handles + subscriber_id.
async fn subscribe<T>(
    client: &mut TerminalServiceClient<T>,
    pty_id: u64,
    cols: u32,
    rows: u32,
) -> Sub
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    let (frame_tx, frame_rx) = mpsc::channel::<SubscribeFrame>(16);
    frame_tx.send(SubscribeFrame {
        frame: Some(subscribe_frame::Frame::Start(SubscribeStart {
            pty_id,
            hostname: "tester".into(),
            size: Some(Size { cols, rows }),
        })),
    }).await.unwrap();

    let mut events = client.subscribe(ReceiverStream::new(frame_rx)).await.unwrap().into_inner();
    let subscriber_id = match events.message().await.unwrap() {
        Some(SubscribeEvent { event: Some(subscribe_event::Event::Ready(r)) }) => {
            assert!(!r.subscriber_id.is_empty(), "subscriber_id must not be empty");
            r.subscriber_id
        }
        other => panic!("expected Ready as first event, got {other:?}"),
    };
    Sub { frame_tx, events, subscriber_id }
}

// Read events off `sub` until `f` yields Some, with a bounded timeout. Skips
// interleaved traffic (data/metadata/etc.). Panics on timeout or stream end.
async fn read_until<R>(
    sub: &mut Sub,
    secs: u64,
    mut f: impl FnMut(subscribe_event::Event) -> Option<R>,
) -> R {
    tokio::time::timeout(Duration::from_secs(secs), async {
        loop {
            match sub.events.message().await {
                Ok(Some(SubscribeEvent { event: Some(ev) })) => {
                    if let Some(r) = f(ev) {
                        return r;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) => panic!("subscribe stream ended before expected event"),
                Err(e) => panic!("subscribe stream error: {e}"),
            }
        }
    })
    .await
    .expect("timed out waiting for expected subscribe event")
}

// Build an authed client over its OWN independent channel/connection to the
// given socket. Each call is a separate H2 connection — dropping the returned
// client tears that connection down (used to replicate `termd send`'s process
// exit, where there is no shared connection kept alive afterward).
async fn connect_client(
    socket: std::path::PathBuf,
) -> TerminalServiceClient<tonic::service::interceptor::InterceptedService<Channel, impl tonic::service::Interceptor>> {
    let socket_path = socket.clone();
    let channel = Endpoint::try_from("http://[::]:1").unwrap()
        .connect_with_connector(service_fn(move |_| {
            let path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(path).await.map(TokioIo::new)
            }
        }))
        .await
        .unwrap();

    TerminalServiceClient::with_interceptor(channel, |mut req: Request<()>| {
        req.metadata_mut().insert(
            "x-auth-token",
            TEST_TOKEN.parse().unwrap(),
        );
        Ok(req)
    })
}

// Returns (TempDir, socket_path, client). Caller must hold TempDir for the test
// duration — dropping it removes the socket file and kills the server. The
// socket_path lets a test open additional independent connections via
// `connect_client`.
#[allow(dead_code)]
async fn test_server() -> (tempfile::TempDir, std::path::PathBuf, TerminalServiceClient<tonic::service::interceptor::InterceptedService<Channel, impl tonic::service::Interceptor>>) {
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

    let client = connect_client(socket.clone()).await;
    (dir, socket, client)
}

#[tokio::test]
async fn test_auth_rejects_missing_token() {
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

    let socket_path = socket.clone();
    let channel = Endpoint::try_from("http://[::]:1").unwrap()
        .connect_with_connector(service_fn(move |_| {
            let path = socket_path.clone();
            async move {
                tokio::net::UnixStream::connect(path).await.map(TokioIo::new)
            }
        }))
        .await
        .unwrap();

    let mut client = TerminalServiceClient::new(channel);
    let result = client.list(ListRequest {}).await;
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn test_create_lists_one_pty() {
    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let list = registry.list();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].info().id, handle.info().id);
}

#[tokio::test]
async fn test_destroy_removes_pty() {
    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let id = handle.info().id;
    registry.destroy(id).unwrap();
    assert!(registry.get(id).is_none());
}

#[tokio::test]
async fn test_destroy_closes_broadcast() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let mut rx = {
        let handle = registry.create(80, 24, None).unwrap();
        let id = handle.info().id;
        let rx = handle.subscribe();
        registry.destroy(id).unwrap();
        // handle drops here; destroy has already removed it from the registry
        rx
    };

    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Err(RecvError::Closed) => return true,
                Err(RecvError::Lagged(_)) | Ok(_) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(closed, "broadcast should close within 5s of destroy");
}

#[tokio::test]
async fn test_exit_notification_broadcast() {
    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.subscribe();

    handle.write(b"exit\n").unwrap();

    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(PtyEvent::Data(chunk)) => {
                    // Exit notification format: "\r\n[Command <title> exited with code <N>]\r\n"
                    // Match the stable prefix regardless of title content or exit code.
                    if chunk.data.windows(9).any(|w| w == b"[Command ") {
                        return true;
                    }
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return false,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "should receive exit notification after shell exits");
}

#[tokio::test]
async fn test_write_produces_broadcast_output() {
    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.subscribe();
    handle.write(b"echo __termd_test__\n").unwrap();

    let chunk = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match rx.recv().await.expect("broadcast recv failed") {
                PtyEvent::Data(chunk) => {
                    let text = String::from_utf8_lossy(&chunk.data);
                    if text.contains("__termd_test__") {
                        return chunk;
                    }
                }
                _ => continue,
            }
        }
    })
    .await
    .expect("timed out waiting for echo output");

    assert!(chunk.generation > 0);
}

#[tokio::test]
async fn test_refresh_returns_screen_data() {
    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    // Write something and wait for it to appear
    handle.write(b"echo __refresh_test__\n").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let data = handle.refresh().await.unwrap();
    assert!(data.generation > 0);
    // Smoke test: verifies the refresh pipeline works end-to-end.
    // We don't assert on specific content here because terminal
    // rendering of the echo output may vary by shell startup timing.
    assert!(!data.data.is_empty(), "refresh data should not be empty");
    assert!(
        data.data.starts_with(b"\x1b["),
        "refresh data should start with an ANSI escape sequence"
    );
}

// The unary-refresh snapshot must be emitted INLINE on the data broadcast,
// addressed to the requesting subscriber (PtyEvent::RefreshFor) — not on a side
// channel. Riding the same ordered channel as the PTY data is what keeps the
// snapshot correctly sequenced relative to that data. A subscriber listening on
// the broadcast must therefore observe a RefreshFor for its own id.
#[tokio::test]
async fn test_unary_refresh_emits_inline_on_broadcast() {
    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.subscribe();

    handle.deliver_refresh("sub-inline").await.unwrap();

    let mut found = false;
    while let Ok(ev) = rx.try_recv() {
        if let PtyEvent::RefreshFor { subscriber_id, data } = ev {
            if subscriber_id == "sub-inline" {
                assert!(!data.data.is_empty(), "inline snapshot data should not be empty");
                found = true;
                break;
            }
        }
    }
    assert!(found, "unary refresh must emit RefreshFor inline on the data broadcast");
}

#[tokio::test]
async fn test_list_empty() {
    let (_dir, _socket, mut client) = test_server().await;
    let resp = client.list(ListRequest {}).await.unwrap().into_inner();
    assert!(resp.items.is_empty());
}

#[tokio::test]
async fn test_create_and_list() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;
    assert!(pty_id != 0);

    let resp = client.list(ListRequest {}).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].pty_id, pty_id);
}

#[tokio::test]
async fn test_destroy() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    client.destroy(DestroyRequest { pty_id }).await.expect("destroy failed");

    let resp = client.list(ListRequest {}).await.unwrap().into_inner();
    assert!(resp.items.is_empty(), "expected empty list after destroy");
}

#[tokio::test]
async fn test_tcp_transport_accepts_list() {
    let registry = Arc::new(PtyRegistry::new());
    let svc = termd::server::make_service(registry, false, TEST_TOKEN.to_string());

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = {
        let mut ch = None;
        for _ in 0..10 {
            match Channel::from_shared(format!("http://127.0.0.1:{port}"))
                .unwrap()
                .connect()
                .await
            {
                Ok(c) => { ch = Some(c); break; }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        ch.expect("TCP server did not become ready in time")
    };
    let mut client = TerminalServiceClient::with_interceptor(channel, |mut req: Request<()>| {
        req.metadata_mut().insert(
            "x-auth-token",
            TEST_TOKEN.parse().unwrap(),
        );
        Ok(req)
    });

    let resp = client.list(ListRequest {}).await.unwrap().into_inner();
    assert!(resp.items.is_empty());
}

#[tokio::test]
async fn test_resize_broadcasts_metadata() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.meta_subscribe();

    handle.resize(120, 40).await.unwrap();

    let found = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match rx.recv().await {
                Ok(meta) => {
                    if matches!(meta.reason, MetadataReason::Resize) {
                        return meta.info.cols == 120 && meta.info.rows == 40;
                    }
                }
                Err(RecvError::Closed) => return false,
                Err(RecvError::Lagged(_)) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "resize should broadcast a Resize metadata event");
}

#[tokio::test]
async fn test_resize_broadcasts_refresh_event() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.subscribe();

    handle.resize(120, 40).await.unwrap();

    let found = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match rx.recv().await {
                Ok(event) => match event {
                    PtyEvent::Refresh(rd) => {
                        return rd.cols == 120 && rd.rows == 40;
                    }
                    _ => continue,
                },
                Err(RecvError::Closed) => return false,
                Err(RecvError::Lagged(_)) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "resize should broadcast a PtyEvent::Refresh with updated cols/rows");
}

#[tokio::test]
async fn test_title_change_broadcasts_metadata() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.meta_subscribe();

    // OSC 0 sets window title; \x07 is BEL terminator
    handle.write(b"printf '\\033]0;TestTitle\\007'\n").unwrap();

    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(meta) => {
                    if matches!(meta.reason, MetadataReason::TitleChanged) {
                        return meta.info.title == "TestTitle";
                    }
                }
                Err(RecvError::Closed) => return false,
                Err(RecvError::Lagged(_)) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "title change should broadcast a TitleChanged metadata event with updated title");
}

#[tokio::test]
async fn test_closed_broadcasts_metadata() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.meta_subscribe();

    handle.write(b"exit\n").unwrap();

    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(meta) => {
                    if matches!(meta.reason, MetadataReason::Closed) {
                        return true;
                    }
                }
                Err(RecvError::Closed) => return false,
                Err(RecvError::Lagged(_)) => continue,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "PTY exit should broadcast a Closed metadata event");
}

#[tokio::test]
async fn test_subscribe_receives_closed_metadata() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Open a Subscribe stream (the subscription's lifetime is the stream).
    let mut sub = subscribe(&mut client, pty_id, 80, 24).await;

    // Trigger PTY exit via a Write frame on the stream.
    sub.frame_tx.send(SubscribeFrame {
        frame: Some(subscribe_frame::Frame::Write(WriteData { data: b"exit\n".to_vec() })),
    }).await.unwrap();

    // Exit now arrives as Metadata::Exited on the subscribe stream.
    read_until(&mut sub, 5, |ev| match ev {
        subscribe_event::Event::Metadata(m) => match m.event {
            Some(stream_metadata::Event::Exited(_)) => Some(()),
            _ => None,
        },
        _ => None,
    }).await;
}

#[tokio::test]
async fn test_resize_via_grpc_delivers_metadata() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Subscribe at the PTY's own size so the unary resize below is the only
    // size change (a differently-sized subscriber would itself refit the PTY).
    let mut sub = subscribe(&mut client, pty_id, 80, 24).await;

    // Unary resize.
    client.resize(ResizeRequest {
        pty_id,
        size: Some(Size { cols: 120, rows: 40 }),
    }).await.unwrap();

    // Resize now arrives as Metadata::Resized on the subscribe stream.
    read_until(&mut sub, 2, |ev| match ev {
        subscribe_event::Event::Metadata(m) => match m.event {
            Some(stream_metadata::Event::Resized(r)) => {
                let s = r.size.unwrap_or_default();
                assert_eq!((s.cols, s.rows), (120, 40));
                Some(())
            }
            _ => None,
        },
        _ => None,
    }).await;
}

#[tokio::test]
async fn test_subscribe_returns_subscriber_id() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // The subscribe ack is now the first Event::Ready(SubscribeReady{subscriber_id}).
    // `subscribe` reads it and asserts the id is non-empty; capture it here too.
    let sub = subscribe(&mut client, pty_id, 80, 24).await;
    assert!(!sub.subscriber_id.is_empty(), "subscriber_id must not be empty");
}

#[tokio::test]
async fn test_list_shows_subscribers() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Subscribe at the PTY's size (so the subscriber size is reported as 80x24
    // and no refit perturbs it). `sub` stays alive so the subscriber persists.
    let sub = subscribe(&mut client, pty_id, 80, 24).await;
    let subscriber_id = sub.subscriber_id.clone();

    // Poll list until the subscriber appears (registration is async w.r.t. Ready).
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let resp = client.list(ListRequest {}).await.unwrap().into_inner();
            let item = resp.items.iter().find(|i| i.pty_id == pty_id)
                .expect("PTY not found in list");
            if item.subscribers.len() == 1 {
                let s = &item.subscribers[0];
                assert_eq!(s.hostname, "tester");
                let size = s.size.unwrap_or_default();
                assert_eq!((size.cols, size.rows), (80, 24));
                assert_eq!(s.subscriber_id, subscriber_id,
                    "subscriber_id in list should match the one from Subscribe Ready");
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("subscriber did not appear in list within 5s");
}

#[tokio::test]
async fn test_disconnect_removes_subscriber() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Subscribe, wait for Ready, then drop the stream — which unsubscribes
    // (there is no Unsubscribe RPC; the stream's lifetime IS the subscription).
    {
        let sub = subscribe(&mut client, pty_id, 80, 24).await;
        // sub (frame_tx + event stream) drops here — client disconnect.
        drop(sub);
    }

    // Poll until the subscriber list is empty or we time out.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let resp = client.list(ListRequest {}).await.unwrap().into_inner();
            let item = resp.items.iter().find(|i| i.pty_id == pty_id);
            if item.map(|i| i.subscribers.is_empty()).unwrap_or(false) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("subscriber not removed within 5s");
}

#[test]
fn test_destroy_all_empties_registry() {
    let registry = termd::pty::PtyRegistry::new();
    registry.create(80, 24, None).unwrap();
    registry.create(80, 24, None).unwrap();
    assert_eq!(registry.list().len(), 2);
    registry.destroy_all();
    assert_eq!(registry.list().len(), 0);
}

#[tokio::test]
async fn test_scrollback_via_grpc() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Scrollback targets a live subscription by its subscriber_id, so subscribe
    // first and keep the stream alive for the duration of the call.
    let sub = subscribe(&mut client, pty_id, 80, 24).await;

    let sr = client.scrollback(ScrollbackRequest {
        pty_id,
        subscriber_id: sub.subscriber_id.clone(),
        kind: ScrollbackOpKind::ScrollbackOpen as i32,
        amount: 0,
        row_count: 24,
    }).await.unwrap().into_inner();

    // With Point::Screen semantics total includes the active screen rows even
    // when there is no history yet.
    assert_eq!(sr.total_scrollback_rows, 24, "fresh PTY total should equal screen rows");
    assert_eq!(sr.row_offset, 0, "OPEN on a fresh PTY sits at the tail");
    assert!(sr.data.is_empty(), "blank active screen produces no VT output");
}

#[tokio::test]
async fn test_subscribe_grows_pty_to_larger_client() {
    let (_dir, _socket, mut client) = test_server().await;

    // Create a PTY at 80x24.
    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Subscribe with a larger terminal than the PTY. The refit should grow the
    // PTY to the subscriber's size; assert via list (the grown PTY size).
    let _sub = subscribe(&mut client, pty_id, 100, 40).await;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let resp = client.list(ListRequest {}).await.unwrap().into_inner();
            let item = resp.items.iter().find(|i| i.pty_id == pty_id).expect("PTY in list");
            let size = item.size.unwrap_or_default();
            if (size.cols, size.rows) == (100, 40) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("subscribing with a larger client should grow the PTY to 100x40");
}

#[tokio::test]
async fn test_subscribe_shrinks_pty_for_single_smaller_client() {
    let (_dir, _socket, mut client) = test_server().await;

    // Create a PTY at 80x24.
    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Subscribe with a smaller terminal. With a single subscriber there are no
    // other clients to clip, so the PTY tracks it exactly and shrinks to 70x20.
    // `sub` stays alive so the subscriber is still attached.
    let sub = subscribe(&mut client, pty_id, 70, 20).await;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let resp = client.list(ListRequest {}).await.unwrap().into_inner();
            let item = resp.items.iter().find(|i| i.pty_id == pty_id).expect("PTY in list");
            let size = item.size.unwrap_or_default();
            if (size.cols, size.rows) == (70, 20) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("a single smaller client should shrink the PTY to 70x20");
    drop(sub);
}

#[tokio::test]
async fn test_subscribe_does_not_shrink_for_multiple_subscribers() {
    let (_dir, _socket, mut client) = test_server().await;

    // Create a PTY at 80x24.
    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Subscriber A (100x40). As the sole subscriber the PTY grows to 100x40.
    let sub_a = subscribe(&mut client, pty_id, 100, 40).await;

    // Wait for the grow to land before B joins.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let resp = client.list(ListRequest {}).await.unwrap().into_inner();
            let item = resp.items.iter().find(|i| i.pty_id == pty_id).expect("PTY in list");
            let size = item.size.unwrap_or_default();
            if (size.cols, size.rows) == (100, 40) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("sole subscriber A should grow the PTY to 100x40");

    // Subscriber B (70x20) joins. With two subscribers the grow-only policy
    // applies: the PTY must NOT shrink to the smaller client.
    let sub_b = subscribe(&mut client, pty_id, 70, 20).await;

    // Poll list until B is registered (2 subscribers), then assert the PTY size.
    let size = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let resp = client.list(ListRequest {}).await.unwrap().into_inner();
            let item = resp.items.iter().find(|i| i.pty_id == pty_id).expect("PTY in list");
            if item.subscribers.len() == 2 {
                return item.size.unwrap_or_default();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("both subscribers should be registered");

    assert_eq!((size.cols, size.rows), (100, 40),
        "with multiple subscribers the PTY must not shrink to a smaller client");
    drop(sub_a);
    drop(sub_b);
}

// Refresh is now a unary ack returning (); the snapshot arrives on the matching
// Subscribe stream as Event::Refresh, ordered against live output. Port of the
// in-process refresh smoke test to the gRPC data plane.
#[tokio::test]
async fn test_refresh_delivers_snapshot_on_stream() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    let mut sub = subscribe(&mut client, pty_id, 80, 24).await;

    // Unary refresh ack (returns ()).
    client.refresh(RefreshRequest {
        pty_id,
        subscriber_id: sub.subscriber_id.clone(),
    }).await.expect("refresh ack");

    // The snapshot arrives as Event::Refresh on the subscribe stream.
    let (size, data) = read_until(&mut sub, 5, |ev| match ev {
        subscribe_event::Event::Refresh(rf) => Some((rf.size.unwrap_or_default(), rf.data)),
        _ => None,
    }).await;

    assert_eq!((size.cols, size.rows), (80, 24), "refresh snapshot should carry the PTY size");
    assert!(!data.is_empty(), "refresh data should not be empty");
    assert!(
        data.starts_with(b"\x1b["),
        "refresh data should start with an ANSI escape sequence"
    );
}

// Half-close-and-drain teardown for a send-style subscriber: drop the
// up-channel (graceful END_STREAM, not the RST_STREAM a full drop causes),
// then read events until the server ends the response stream. The server
// processes inbound frames in order and closes the stream on half-close, so
// stream end is positive confirmation every queued Write reached the PTY.
async fn close_and_drain(sub: Sub) {
    let Sub { frame_tx, mut events, .. } = sub;
    drop(frame_tx);
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(_) = events.message().await.expect("drain error") {}
    })
    .await
    .expect("timed out draining subscribe stream after half-close");
}

// Live concern #1: the `termd send` pattern. A client opens a Subscribe stream,
// sends Start + Write, waits for Ready, half-closes the up-stream, and drains
// events to end-of-stream before exiting. The written bytes MUST reach the PTY
// (a bare drop would RST the stream and can discard the buffered Write — h2
// >= 0.4.15 enforces this). We observe delivery via a SECOND, long-lived
// subscriber's Event::Data.
//
// This is the WEAK form: the sender shares the long-lived `client` (and its H2
// connection), so the server has unbounded time to drain the buffered Write even
// after the stream is dropped. It proves the server reads a queued Write, but not
// that it survives connection teardown. See the strong form below.
#[tokio::test]
async fn test_send_pattern_delivers_bytes_to_pty() {
    let (_dir, _socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Observer subscriber stays attached for the whole test.
    let mut observer = subscribe(&mut client, pty_id, 80, 24).await;

    // The "send" client: subscribe, write, wait for Ready (already done by
    // `subscribe`), then half-close and drain — exactly the termd-send shape.
    {
        let sender = subscribe(&mut client, pty_id, 80, 24).await;
        sender.frame_tx.send(SubscribeFrame {
            frame: Some(subscribe_frame::Frame::Write(WriteData {
                data: b"echo __termd_send__\n".to_vec(),
            })),
        }).await.unwrap();
        close_and_drain(sender).await;
    }

    // The bytes must reach the PTY: the observer sees the echoed text in output.
    read_until(&mut observer, 5, |ev| match ev {
        subscribe_event::Event::Data(d) => {
            if String::from_utf8_lossy(&d.data).contains("__termd_send__") {
                Some(())
            } else {
                None
            }
        }
        _ => None,
    }).await;
}

// Live concern #1 — STRONG form. This reproduces the actual `termd send` teardown:
// the sender runs on its OWN independent connection (its own `Channel`/H2
// connection, like a separate process), queues a Write right after Ready,
// half-closes and drains to end-of-stream, and only THEN is the entire sender
// client — channel and all — dropped, tearing the connection down (the process
// exiting). Draining to stream end before teardown is what guarantees the Write
// reached the PTY; delivery is observed on a SEPARATELY-connected, long-lived
// observer so nothing of the sender's survives to help it along.
#[tokio::test]
async fn test_send_pattern_survives_connection_teardown() {
    let (_dir, socket, mut client) = test_server().await;

    let item = client.create(CreateRequest {
        size: Some(Size { cols: 80, rows: 24 }), command: None,
    }).await.unwrap().into_inner();
    let pty_id = item.pty_id;

    // Observer on the original (long-lived) connection.
    let mut observer = subscribe(&mut client, pty_id, 80, 24).await;

    // The sender gets a brand-new, independent connection. After queueing the
    // Write it half-closes and drains to stream end, then EVERYTHING the sender
    // owns — event stream, client, channel — is dropped, replicating `termd
    // send` exiting.
    {
        let mut sender_client = connect_client(socket.clone()).await;
        let sender = subscribe(&mut sender_client, pty_id, 80, 24).await;
        sender.frame_tx.send(SubscribeFrame {
            frame: Some(subscribe_frame::Frame::Write(WriteData {
                data: b"echo __termd_teardown__\n".to_vec(),
            })),
        }).await.unwrap();
        close_and_drain(sender).await;
        drop(sender_client);
    }

    // Despite the sender's connection being gone, the bytes must have reached the
    // PTY: the independently-connected observer sees the echoed text.
    read_until(&mut observer, 5, |ev| match ev {
        subscribe_event::Event::Data(d) => {
            if String::from_utf8_lossy(&d.data).contains("__termd_teardown__") {
                Some(())
            } else {
                None
            }
        }
        _ => None,
    }).await;
}
