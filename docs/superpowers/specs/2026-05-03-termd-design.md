---
name: termd design
description: Design spec for termd — a Rust gRPC bidi-stream daemon managing PTYs with libghostty-rs
type: project
---

# termd Design Spec

## Overview

`termd` is a Rust daemon that manages PTYs (pseudo-terminals) backed by `libghostty-rs` terminal
emulator instances. Clients connect via gRPC and interact over a single bidirectional stream,
sending commands (create, destroy, subscribe, write, resize, etc.) and receiving responses and
streamed PTY output.

This is a proof-of-concept. Skeleton implementations are acceptable where noted.

---

## Project Structure

```
src/
  main.rs        — CLI entry point (clap): start, list, create, destroy subcommands
  server.rs      — tonic service impl, auth interceptor, grpc-log layer, dual-transport listener
  commands.rs    — dispatch logic per TerminalCommand variant
  pty.rs         — PtyRegistry, PtyHandle, spawn/destroy, blocking reader thread
proto/
  terminal.proto
build.rs         — tonic_build::compile_protos
Cargo.toml
```

---

## Protobuf Spec

```protobuf
syntax = "proto3";
import "google/protobuf/timestamp.proto";

package terminal;

service TerminalService {
  rpc Stream(stream TerminalCommand) returns (stream TerminalResponse);
}

// --- Commands (client → server) ---

message TerminalCommand {
  oneof command {
    ListRequest        list        = 1;
    CreateRequest      create      = 2;
    DestroyRequest     destroy     = 3;
    SubscribeRequest   subscribe   = 4;
    UnsubscribeRequest unsubscribe = 5;
    WriteRequest       write       = 6;
    ResizeRequest      resize      = 7;
    SetTitleRequest    set_title   = 8;
    RefreshRequest     refresh     = 9;
  }
}

message ListRequest        {}
message CreateRequest      { uint32 cols = 1; uint32 rows = 2; optional string command = 3; }
message DestroyRequest     { string pty_id = 1; }
message SubscribeRequest   { string pty_id = 1; }
message UnsubscribeRequest { string pty_id = 1; }
message WriteRequest       { string pty_id = 1; bytes data = 2; }
message ResizeRequest      { string pty_id = 1; uint32 cols = 2; uint32 rows = 3; }
message SetTitleRequest    { string pty_id = 1; string title = 2; }
message RefreshRequest     { string pty_id = 1; }

// --- Responses (server → client) ---

message TerminalResponse {
  oneof response {
    ListResponse    list    = 1;
    CreateResponse  create  = 2;
    CommandResponse command = 3;  // destroy / subscribe / unsubscribe / write / resize / set_title
    StreamData      stream  = 4;
    RefreshResponse refresh = 5;
  }
}

message PtyItem {
  string pty_id                        = 1;
  string hostname                      = 2;
  string pts_name                      = 3;  // e.g. /dev/pts/30
  uint32 cols                          = 4;
  uint32 rows                          = 5;
  string title                         = 6;
  bool   subscribed                    = 7;  // true if this client is subscribed
  google.protobuf.Timestamp created_at = 8;
}

message ListResponse    { repeated PtyItem items = 1; }
message CreateResponse  { PtyItem item = 1; }
message CommandResponse { string pty_id = 1; bool success = 2; optional string error = 3; }
message StreamData      { string pty_id = 1; uint64 generation = 2; bytes data = 3; }
message RefreshResponse {
  string pty_id   = 1;
  uint64 generation = 2;
  bytes  data     = 3;  // raw screen data from libghostty render API
  uint32 cursor_x = 4;
  uint32 cursor_y = 5;
}
```

---

## PTY Management (`pty.rs`)

### Core types

All fields of `PtyHandle` are private. External code (`commands.rs`, `server.rs`) interacts only
through `PtyRegistry` and the `PtyHandle` method surface below — never touching fields directly.
This keeps the broadcast/thread internals easy to swap without touching call sites.

```rust
// All fields private
struct PtyHandle { /* ... */ }

impl PtyHandle {
    fn info(&self) -> PtyInfo              // hostname, pts_name, cols, rows, title, created_at
    fn subscribe(&self) -> broadcast::Receiver<Arc<PtyChunk>>
    fn write(&self, data: &[u8]) -> Result<()>
    fn resize(&self, cols: u32, rows: u32) -> Result<()>
    fn set_title(&self, title: &str)
    async fn refresh(&self) -> Result<RefreshData>
}

struct PtyRegistry {
    ptys: RwLock<HashMap<String, Arc<PtyHandle>>>,
}

impl PtyRegistry {
    fn create(&self, cols: u32, rows: u32, command: Option<&str>) -> Result<Arc<PtyHandle>>
    fn destroy(&self, id: &str) -> Result<()>
    fn get(&self, id: &str) -> Option<Arc<PtyHandle>>
    fn list(&self) -> Vec<Arc<PtyHandle>>
}
```

Internal field layout (inside `pty.rs` only):

```rust
struct PtyHandle {
    id:          String,
    pts_name:    String,
    created_at:  SystemTime,
    cols:        AtomicU32,
    rows:        AtomicU32,
    title:       Arc<Mutex<String>>,   // shared with on_title_changed closure on reader thread
    generation:  AtomicU64,
    tx:          broadcast::Sender<Arc<PtyChunk>>,
    writer:      Mutex<File>,
    refresh_tx:  std::sync::mpsc::SyncSender<oneshot::Sender<RefreshData>>,
    child_pid:   u32,
}

struct PtyChunk { generation: u64, data: Bytes }

// Sent back from the reader thread in response to a refresh
struct RefreshData { generation: u64, data: Bytes, cursor_x: u32, cursor_y: u32 }
```

### Spawn flow (`PtyRegistry::create`)

1. Allocate PTY master/slave pair via `posix_openpt` + `grantpt` + `unlockpt` (using `nix` crate).
2. `fork()` — child process:
   - `setsid()` to start a new session
   - Open slave fd as the controlling terminal (`TIOCSCTTY`)
   - Set environment: `TERM=xterm-ghostty` (named constant; wire up from libghostty if exposed in
     a future version), `USER`, `HOME`, `SHELL` inherited from daemon environment
   - `exec` `$SHELL` (or the override command from `CreateRequest`)
   - Placeholder for systemd-logind session registration (Linux) / launchd (future macOS)
3. Create `broadcast::channel` for output and `std::sync::mpsc::sync_channel` for refresh requests.
4. Spawn a **dedicated `std::thread::spawn`** (not a tokio task) that exclusively owns the
   `libghostty_vt::Terminal` and all render objects (`RenderState`, `RowIterator`, `CellIterator`).
   This is required because libghostty-rs objects are `!Send + !Sync`.

   The reader thread loop:
   - Blocking-reads from master fd
   - Feeds bytes into `terminal.vt_write()`, increments `generation`, broadcasts `Arc<PtyChunk>`
   - Listens on refresh request receiver; on receipt, renders screen via libghostty render API and
     sends `RefreshData` back via the oneshot sender
   - On EOF (shell exit): stops reading and broadcasting, but stays alive waiting for refresh
     requests — **PTY remains in the registry until an explicit `DestroyRequest`**
   - Exits naturally when `refresh_tx` (the `SyncSender`) is dropped, which happens when the
     `Arc<PtyHandle>` is dropped during destroy
5. Register `on_title_changed` effect on the Terminal to update the shared `Arc<Mutex<String>>`
   title (the same Arc stored in `PtyHandle::title`).
6. Wrap in `Arc<PtyHandle>` and insert into registry.

### Destroy flow

Send `SIGHUP` to `child_pid`, remove from registry, drop `Arc<PtyHandle>`. When the reader thread's
sender handle is dropped, the broadcast channel closes and any subscribers see a disconnect.

---

## gRPC Server (`server.rs`)

### Authentication

A tonic `interceptor` checks the `x-auth-token` gRPC metadata header against a hardcoded
`AUTH_TOKEN` constant. Returns `UNAUTHENTICATED` if missing or wrong.

### gRPC request/response logging

When `--log-grpc` is passed, a `tower::Layer` (or second interceptor) logs the full serialized
request and response at `DEBUG` level via `tracing`. Applied only when the flag is set.

### Dual transport

`server::serve(registry, unix_path, tcp_addr, log_grpc)` starts two tonic servers in parallel —
one over `tokio::net::UnixListener` and one over `TcpListener` — sharing the same
`Arc<PtyRegistry>` via `tokio::join!`.

### Stream handler

The `Stream` RPC handler maintains per-connection state:

```rust
let mut subscriptions: HashMap<String, broadcast::Receiver<Arc<PtyChunk>>> = HashMap::new();
```

Main loop uses `tokio::select!` over:
- **(a)** Next inbound `TerminalCommand` — dispatched to `commands.rs`
- **(b)** Next chunk from any subscribed PTY receiver — sent as `StreamData` response

`tokio::select!` doesn't natively handle a dynamic set of futures, so subscribed receivers are
polled via `futures::stream::SelectAll` (or an equivalent poll-each-in-turn approach), merged into
a single stream that the select arm drives.

Subscribing adds a `broadcast::Receiver` to the map; unsubscribing drops it.

---

## CLI (`main.rs`)

```
termd start [--log-grpc] [--tcp-addr ADDR]
    Run the server in the foreground. --log-grpc enables per-request/response debug logging.
    --tcp-addr sets the TCP listen address (default: 127.0.0.1:7777).

termd list
    Connect to Unix socket → open Stream → send ListRequest → print PTY table.

termd create [--cols N] [--rows N] [--cmd CMD]
    Connect → send CreateRequest → print new PTY ID.

termd destroy <pty-id>
    Connect → send DestroyRequest → print success/error.
```

Client subcommands share a helper that:
1. Connects to the Unix socket (path: `$XDG_RUNTIME_DIR/termd.sock` or `/run/termd/termd.sock`)
2. Opens a `Stream` with `x-auth-token` in gRPC metadata
3. Sends one command, reads one response, prints result, exits

Uses `clap` derive macros. Uses `tracing` + `tracing-subscriber` for logging throughout.

---

## libghostty-rs Integration

Each PTY's reader thread owns:
- `Terminal` — receives all PTY output via `vt_write()`
- `RenderState`, `RowIterator`, `CellIterator` — used only for `RefreshRequest` handling

On refresh: render the current screen snapshot, serialize cell data as raw bytes (exact wire format
is PoC-quality for now), include cursor position and current `generation`.

The `on_title_changed` effect callback updates `PtyHandle::title` via an `Arc<Mutex<String>>`
shared between the reader thread and the handle.

---

## Key Dependencies

| Crate | Purpose |
|---|---|
| `tonic` | gRPC server + client |
| `prost` / `prost-types` | Protobuf codegen + `Timestamp` |
| `tokio` | Async runtime |
| `tower` | Middleware layers (gRPC logging) |
| `clap` | CLI |
| `nix` | PTY allocation, `fork`, `setsid` |
| `tracing` / `tracing-subscriber` | Structured logging |
| `bytes` | Zero-copy byte buffers |
| `uuid` | PTY ID generation |
| `libghostty-vt` | Terminal emulation per PTY |

---

## Out of Scope (PoC)

- systemd-logind D-Bus session registration (placeholder only)
- macOS launchd support
- PTY state persistence
- OSC protocol direct handling
- Client-side libghostty integration
- Cursor desync prevention in `StreamData`
