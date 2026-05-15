use std::time::Duration;
use termd::pty::PtyRegistry;

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
