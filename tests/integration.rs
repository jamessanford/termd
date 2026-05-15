use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::Request;
use tower::service_fn;
use hyper_util::rt::TokioIo;
use termd::pty::PtyRegistry;
use termd::server::make_service;
use termd::proto::terminal_service_client::TerminalServiceClient;
use termd::proto::{TerminalCommand, terminal_command};
use termd::proto::ListRequest;

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
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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
}
