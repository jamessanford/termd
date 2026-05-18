# SIGTERM/SIGINT Graceful Shutdown

**Date:** 2026-05-18
**Status:** Approved

## Problem

`termd start` has no signal handling. SIGTERM and SIGINT kill the process immediately,
leaving utmp entries written by libutempter stranded in `/var/run/utmp` — sessions
remain visible in `who`/`w` after the daemon exits. Active attach clients are also
torn off without any end-of-stream notification.

## Goals

- Clean up all utmp entries on SIGTERM and SIGINT
- Give active gRPC streaming handlers a short window to drain before exit
- No new files; three existing files change

## Design

### Shutdown sequence

```
SIGTERM or SIGINT fires
    │
    ├─ registry.destroy_all()
    │     SIGHUP all child shells → reader threads broadcast Closed
    │     → gRPC streaming handlers see end-of-stream and return
    │
    └─ shutdown signal resolves
          tonic stops accepting new connections
          existing handlers drain (up to 5 s timeout)
               │
               └─ utmp::remove_all_records()   ← belt-and-suspenders cleanup
                    │
                    └─ process exits
```

Calling `destroy_all()` *as part of* the shutdown signal (not after draining) ensures
the drain is fast: children receive SIGHUP, die, reader threads broadcast `Closed`, and
gRPC handlers return on their own within a few hundred milliseconds.

### `src/utmp.rs` — add `remove_all_records()`

New extern declaration and public function:

```rust
#[cfg(has_utempter)]
extern "C" {
    fn utempter_remove_added_record() -> libc::c_int;
}

/// Remove all utmp entries added by this process.
/// Called at graceful shutdown as a belt-and-suspenders cleanup.
/// No-op if termd was built without libutempter.
pub fn remove_all_records() {
    #[cfg(has_utempter)]
    unsafe { utempter_remove_added_record(); }
}
```

`remove_all_records()` is idempotent with the reader threads' individual
`remove_record()` calls — whichever runs first wins, the other is a silent no-op.

### `src/pty.rs` — add `PtyRegistry::destroy_all()`

```rust
pub fn destroy_all(&self) {
    let ids: Vec<String> = self.ptys.read().unwrap().keys().cloned().collect();
    for id in ids {
        let _ = self.destroy(&id);
    }
}
```

Collects IDs under the read lock, then destroys each. Individual `destroy()` failures
(e.g. process already gone) are silently ignored — the loop continues.

### `src/server.rs` — update `serve()`

Replace `serve_with_incoming` with `serve_with_incoming_shutdown` on both listeners.
A spawned task watches for SIGTERM/SIGINT; when either fires it calls
`registry.destroy_all()` and broadcasts on a `tokio::sync::broadcast` channel. Each
server receives its own channel receiver as the shutdown future.

After `tokio::try_join!` (wrapped in a 5 s `tokio::time::timeout`), call
`utmp::remove_all_records()` and return. Whether the timeout fires or the servers
drain cleanly, cleanup always runs.

```rust
pub async fn serve(
    registry: Arc<PtyRegistry>,
    unix_path: &std::path::Path,
    tcp_addr: std::net::SocketAddr,
    log_grpc: bool,
) -> anyhow::Result<()> {
    // ... socket setup unchanged ...

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let mut shutdown_rx1 = shutdown_tx.subscribe();
    let mut shutdown_rx2 = shutdown_tx.subscribe();

    let registry_for_shutdown = registry.clone();
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint  = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
            _ = sigint.recv()  => tracing::info!("SIGINT received, shutting down"),
        }
        registry_for_shutdown.destroy_all();
        let _ = shutdown_tx.send(());
    });

    let svc_unix = make_service(registry.clone(), log_grpc);
    let svc_tcp  = make_service(registry.clone(), log_grpc);

    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::try_join!(
            Server::builder()
                .add_service(svc_unix)
                .serve_with_incoming_shutdown(
                    UnixListenerStream::new(unix_listener),
                    async move { let _ = shutdown_rx1.recv().await; },
                ),
            Server::builder()
                .add_service(svc_tcp)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(tcp_listener),
                    async move { let _ = shutdown_rx2.recv().await; },
                ),
        ),
    ).await;

    crate::utmp::remove_all_records();
    Ok(())
}
```

Note: `registry` is passed to `make_service` by clone now that `registry_for_shutdown`
also holds a clone. The existing call already clones for each service, so this is a
minimal change.

## Edge cases

- **No PTYs running:** `destroy_all()` and `remove_all_records()` are both no-ops.
- **5 s drain timeout exceeded:** Log a warning (from `timeout` returning `Err`),
  proceed to `remove_all_records()` and exit. Active streams are force-closed by
  process exit.
- **Double-remove race:** Reader threads may call `remove_record()` concurrently with
  `remove_all_records()`. Both call into libutempter's helper process serially;
  removing an already-removed entry is a no-op.
- **Signal during startup:** The signal task is spawned before the servers bind, so
  SIGTERM during slow startup is handled correctly.

## What is not in scope

- Waiting for reader threads to fully complete before exit (children are reaped by
  init if orphaned; `remove_all_records()` handles utmp cleanup independently)
- Configurable drain timeout (5 s is hardcoded; can be made a flag later)
- SIGHUP reload semantics
