use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Request;
use tower::service_fn;
use hyper_util::rt::TokioIo;
use termd::pty::PtyRegistry;
use termd::pty::{MetadataReason, PtyMetadata};
use termd::server::make_service;
use termd::proto::terminal_service_client::TerminalServiceClient;
use termd::proto::{TerminalCommand, terminal_command};
use termd::proto::{ListRequest, CreateRequest, DestroyRequest};

async fn send_recv<T>(
    client: &mut TerminalServiceClient<T>,
    cmd: terminal_command::Command,
) -> termd::proto::TerminalResponse
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let (tx, rx) = mpsc::channel(1);
    tx.send(TerminalCommand { command: Some(cmd) }).await.unwrap();
    drop(tx);

    let mut stream = client.stream(ReceiverStream::new(rx)).await.unwrap().into_inner();
    stream.message().await.unwrap().unwrap()
}

// Returns (TempDir, client). Caller must hold TempDir for the test duration —
// dropping it removes the socket file and kills the server.
#[allow(dead_code)]
async fn test_server() -> (tempfile::TempDir, TerminalServiceClient<tonic::service::interceptor::InterceptedService<Channel, impl tonic::service::Interceptor>>) {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("termd.sock");
    let registry = Arc::new(PtyRegistry::new());
    let svc = make_service(registry, false);
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

    let client = TerminalServiceClient::with_interceptor(channel, |mut req: Request<()>| {
        req.metadata_mut().insert(
            "x-auth-token",
            termd::server::AUTH_TOKEN.parse().unwrap(),
        );
        Ok(req)
    });
    (dir, client)
}

#[tokio::test]
async fn test_auth_rejects_missing_token() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("termd.sock");
    let registry = Arc::new(PtyRegistry::new());
    let svc = make_service(registry, false);
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
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let _ = tx.send(TerminalCommand {
        command: Some(terminal_command::Command::List(ListRequest {})),
    }).await;
    drop(tx);
    let result = client.stream(tokio_stream::wrappers::ReceiverStream::new(rx)).await;
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
    let id = handle.info().id.clone();
    registry.destroy(&id).unwrap();
    assert!(registry.get(&id).is_none());
}

#[tokio::test]
async fn test_destroy_closes_broadcast() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let mut rx = {
        let handle = registry.create(80, 24, None).unwrap();
        let id = handle.info().id.clone();
        let rx = handle.subscribe();
        registry.destroy(&id).unwrap();
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
                Ok(chunk) => {
                    // Exit notification format: "\r\n[Command <title> exited with code <N>]\r\n"
                    // Match the stable prefix regardless of title content or exit code.
                    if chunk.data.windows(9).any(|w| w == b"[Command ") {
                        return true;
                    }
                }
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
            let chunk = rx.recv().await.expect("broadcast recv failed");
            let text = String::from_utf8_lossy(&chunk.data);
            if text.contains("__termd_test__") {
                return chunk;
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

#[tokio::test]
async fn test_list_empty() {
    let (_dir, mut client) = test_server().await;
    let resp = send_recv(&mut client, terminal_command::Command::List(ListRequest {})).await;
    match resp.response.unwrap() {
        termd::proto::terminal_response::Response::List(l) => assert!(l.items.is_empty()),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn test_create_and_list() {
    let (_dir, mut client) = test_server().await;

    let resp = send_recv(&mut client, terminal_command::Command::Create(CreateRequest {
        cols: 80, rows: 24, command: None,
    })).await;
    let pty_id = match resp.response.unwrap() {
        termd::proto::terminal_response::Response::Create(c) => c.item.unwrap().pty_id,
        other => panic!("unexpected: {other:?}"),
    };
    assert!(!pty_id.is_empty());

    let resp = send_recv(&mut client, terminal_command::Command::List(ListRequest {})).await;
    match resp.response.unwrap() {
        termd::proto::terminal_response::Response::List(l) => {
            assert_eq!(l.items.len(), 1);
            assert_eq!(l.items[0].pty_id, pty_id);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn test_destroy() {
    let (_dir, mut client) = test_server().await;

    let resp = send_recv(&mut client, terminal_command::Command::Create(CreateRequest {
        cols: 80, rows: 24, command: None,
    })).await;
    let pty_id = match resp.response.unwrap() {
        termd::proto::terminal_response::Response::Create(c) => c.item.unwrap().pty_id,
        other => panic!("unexpected: {other:?}"),
    };

    let resp = send_recv(&mut client, terminal_command::Command::Destroy(
        DestroyRequest { pty_id: pty_id.clone() },
    )).await;
    match resp.response.unwrap() {
        termd::proto::terminal_response::Response::Command(c) => {
            assert!(c.success, "destroy failed: {:?}", c.error);
        }
        other => panic!("unexpected: {other:?}"),
    }

    let resp = send_recv(&mut client, terminal_command::Command::List(ListRequest {})).await;
    match resp.response.unwrap() {
        termd::proto::terminal_response::Response::List(l) => {
            assert!(l.items.is_empty(), "expected empty list after destroy");
        }
        other => panic!("unexpected after destroy list: {other:?}"),
    }
}

#[tokio::test]
async fn test_tcp_transport_accepts_list() {
    let registry = Arc::new(PtyRegistry::new());
    let svc = termd::server::make_service(registry, false);

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
            termd::server::AUTH_TOKEN.parse().unwrap(),
        );
        Ok(req)
    });

    let resp = send_recv(&mut client, terminal_command::Command::List(ListRequest {})).await;
    match resp.response.unwrap() {
        termd::proto::terminal_response::Response::List(l) => assert!(l.items.is_empty()),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn test_resize_broadcasts_metadata() {
    use tokio::sync::broadcast::error::RecvError;

    let registry = PtyRegistry::new();
    let handle = registry.create(80, 24, None).unwrap();
    let mut rx = handle.meta_subscribe();

    handle.resize(120, 40).unwrap();

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
    use termd::proto::{
        terminal_command::Command, terminal_response::Response,
        TerminalCommand, SubscribeRequest, WriteRequest, StreamMetadataReason,
    };

    let (_dir, mut client) = test_server().await;

    // Create a PTY
    let resp = send_recv(&mut client, Command::Create(CreateRequest {
        cols: 80, rows: 24, command: None,
    })).await;
    let pty_id = match resp.response.unwrap() {
        Response::Create(c) => c.item.unwrap().pty_id,
        other => panic!("expected Create, got {other:?}"),
    };

    // Open a long-lived bidi stream and subscribe
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<TerminalCommand>(16);
    let mut resp_stream = client
        .stream(tokio_stream::wrappers::ReceiverStream::new(cmd_rx))
        .await
        .unwrap()
        .into_inner();

    cmd_tx.send(TerminalCommand {
        command: Some(Command::Subscribe(SubscribeRequest { pty_id: pty_id.clone() })),
    }).await.unwrap();

    // Wait for subscribe ack
    loop {
        match resp_stream.message().await.unwrap().unwrap().response.unwrap() {
            Response::Command(c) if c.success => break,
            _ => {}
        }
    }

    // Trigger PTY exit
    cmd_tx.send(TerminalCommand {
        command: Some(Command::Write(WriteRequest {
            pty_id: pty_id.clone(),
            data: b"exit\n".to_vec(),
        })),
    }).await.unwrap();

    // Wait for CLOSED metadata
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match resp_stream.message().await {
                Ok(Some(resp)) => match resp.response.unwrap() {
                    Response::Metadata(m)
                        if m.reason == StreamMetadataReason::Closed as i32 => return true,
                    _ => continue,
                },
                _ => return false,
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(found, "subscribe stream should deliver Closed metadata after PTY exits");
}
