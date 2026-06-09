# Reader-thread restructure and stream-lag recovery

Date: 2026-06-09
Status: approved

Two changes to termd, designed together because the first creates the structure
the second's server half plugs into:

1. **Reader-thread restructure.** PTY writes move into the reader thread, and
   the thread's three request channels (`refresh_tx`, `scrollback_tx`,
   `resize_tx`) plus the new write path consolidate into a single
   `ReaderRequest` enum channel. The reader becomes the sole owner of the
   master fd. State mirrored between `PtyHandle` and the thread consolidates
   into one `Arc<PtyShared>`.
2. **Stream-lag recovery.** A subscriber that lags the broadcast channel and
   loses data is told so explicitly (`DATA_LOST` stream metadata); the attach
   client recovers by requesting a refresh.

## Motivation

- `PtyHandle::write` writes the master fd from the tokio thread. The fd is
  `O_NONBLOCK` (the `dup` shares the open file description with the reader's
  fd, so the `F_SETFL` in `create()` applies to both), so a full PTY input
  queue makes `write_all` fail with `EAGAIN` and the bytes are silently
  dropped — the attach client never reads the error `CommandResponse`. The
  proper fix (buffer the unwritten tail, poll `POLLOUT` until drained) has to
  live in the reader's poll loop.
- The daemon intentionally runs tokio in `current_thread` flavor during
  development; nothing on the runtime thread may block.
- `resize_tx.try_send` is best-effort: a full channel silently desyncs the
  libghostty terminal's dimensions from the real PTY size, permanently.
- The reader thread takes 15 parameters, is fed by three channels plus a
  wakeup pipe, and its post-loop cleanup is three drain loops plus a mirror
  struct (`ClosedNotifier`).
- On `BroadcastStreamRecvError::Lagged` the forwarding task logs a warning and
  continues; in raw/region render modes the client's screen is then
  permanently corrupt with no recovery path.

## Out of scope (deliberately)

- Protocol correlation / request ids (`CommandResponse` semantics unchanged).
- Merging the data and metadata broadcast channels.
- Generation-numbering changes (on-demand refreshes still consume generations
  invisible to other subscribers — which is why lag is signaled explicitly
  instead of inferred from generation gaps).
- Event loss inside modal client helpers (`fetch_list`, `request_refresh`,
  scrollback pager): a `DATA_LOST` arriving in those windows is discarded like
  all other metadata there. Known limitation of the current stream-draining
  pattern; deferred with it.
- Backpressure on the PTY read side (the daemon never pauses reading the
  master; lag recovery is the chosen stance).

## Part 1: Reader-thread restructure

### Module layout

```
src/pty/mod.rs       PtyRegistry, PtyHandle, PtyShared, public types
                     (PtyInfo, PtyEvent, PtyChunk, PtyMetadata, RefreshData,
                      ScrollbackData, ScrollbackOp, SubscriberInfo,
                      MetadataReason). External API stays `crate::pty::*`.
src/pty/reader.rs    ReaderRequest, Reader struct + poll loop, write
                     buffering, cleanup. boundary/process_read tests move here.
src/pty/snapshot.rs  do_refresh, do_scrollback, formatter plumbing.
                     refresh/scrollback tests move here.
```

No import changes outside the module. Test suites move with their subjects
and must pass unchanged (the "logic moved, not rewritten" check).

### ReaderRequest

```rust
pub(crate) enum ReaderRequest {
    Write(Bytes),                                   // fire-and-forget
    Resize { cols: u32, rows: u32, reply: oneshot::Sender<Result<()>> },
    Refresh { reply: oneshot::Sender<Result<RefreshData>> },
    Scrollback { subscriber_id: String, op: ScrollbackOp, amount: i32,
                 row_count: u32, reply: oneshot::Sender<Result<ScrollbackData>> },
}
```

- Carried on an **unbounded** `std::sync::mpsc::channel` so sends from the
  tokio thread never block. Each send is followed by the existing one-byte
  wakeup-pipe write. Input volume is bounded upstream by gRPC flow control.
- Requests are handled in channel order: a client's Write → Resize → Write
  sequencing is preserved.
- `Resize` carries a reply so `handle_resize` returns an honest
  `CommandResponse`; the refit call in `handle_subscribe` drops the receiver
  (fire-and-forget). `PtyHandle::resize` becomes `async fn` and
  `handle_resize` follows.
- `close_scrollback` keeps its fire-and-forget character (send + ignore).

### PtyShared

All state currently mirrored or shared piecemeal between `PtyHandle` and the
reader thread consolidates into one struct held by both sides:

```rust
pub(crate) struct PtyShared {
    // immutable
    id: u64, hostname: String, pts_name: String, created_at: SystemTime,
    // shared mutable
    cols: AtomicU32, rows: AtomicU32,
    title: Mutex<String>,
    generation: AtomicU64,
    subscribers: RwLock<HashMap<String, SubscriberInfo>>,
    last_subscribed_at: Mutex<Option<SystemTime>>,
    sort_order: AtomicU32,
}
impl PtyShared { pub fn info(&self) -> PtyInfo { ... } }  // moves from PtyHandle
```

`PtyHandle` shrinks to: `shared: Arc<PtyShared>`, `req_tx`, `wakeup_write`,
`tx` (data broadcast), `meta_tx` (metadata broadcast), `child_pid`. Its
methods become thin sends plus delegation to `shared`.

**Invariant:** the reader holds `Arc<PtyShared>`, never `Arc<PtyHandle>`.
`wakeup_write` lives only on the handle, preserving destroy-by-drop → reader
sees `POLLHUP` → exits. The same rule applies to anything long-lived spawned
elsewhere (see Part 2's forwarding task).

Because the reader can call `shared.info()`, metadata it emits carries
complete `PtyInfo`: the `TitleChanged` path stops fabricating
`subscribers: None, sort_order: 0`, and `Resize` metadata is emitted by the
thread that performed the resize.

### Reader struct and loop

```rust
struct Reader {
    terminal: Terminal<'static, 'static>,
    master: File,            // sole master fd, O_NONBLOCK; the dup is gone
    wakeup_read: OwnedFd,
    req_rx: std::sync::mpsc::Receiver<ReaderRequest>,
    tx: broadcast::Sender<PtyEvent>,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    shared: Arc<PtyShared>,
    child: std::process::Child,
    pending_out: Vec<u8>,    // unwritten PTY-input tail
    pins: HashMap<String, TrackedGridRef>,
    pending_replies: Vec<oneshot::Sender<Result<RefreshData>>>,
    pending_broadcast_refresh: bool,
    prev_title: String,
    prev_screen: Screen,
    exit_code: Option<i32>,  // read by Drop
}
```

Loop iteration: drain wakeup pipe → drain `req_rx` (`try_recv`) → boundary-
gated `flush_refreshes` (unchanged) → opportunistic flush of `pending_out` →
`poll()` → handle revents (wakeup `POLLHUP` = shutdown; master `POLLOUT` =
flush `pending_out`; master `POLLIN` = read + `process_read`) → title/screen
change detection (now via `shared`).

Per request:

- **Write**: if `pending_out` is empty, attempt the write immediately and
  buffer only the unwritten tail on `EAGAIN`; otherwise append. While
  `pending_out` is non-empty, the master's poll entry adds `POLLOUT`.
  Capped by `MAX_PENDING_INPUT` (1 MiB): beyond the cap the incoming write is
  dropped with a `warn!` — the same failure mode as today's silent drop, but
  at ~16x the kernel queue depth and visible in logs.
- **Resize**: `TIOCSWINSZ` on `master` → `terminal.resize` →
  `shared.cols/rows.store` → `pending_broadcast_refresh = true` → broadcast
  `Resize` metadata with `shared.info()` → reply `Ok`. On `ioctl` failure:
  reply `Err`, no state update, no metadata (parity with today's early
  return). Kernel resize, VT resize, and metadata now happen on one thread in
  one order; the `resize_tx` desync window is gone.
- **Refresh** / **Scrollback**: today's behavior in the new envelope.

`process_read`, the VT-boundary splitting, generation stamping, and the
stall-timeout path are untouched — they move files, not logic.

### Cleanup and shutdown

- One `cleanup()` method replaces the post-loop straight-line code and three
  drain loops: reap child → set `self.exit_code` → utmp remove → exit-message
  broadcast → a single final `match` drain of `req_rx` (`Refresh`/`Scrollback`
  served against the final terminal without waiting for a boundary, `Resize`
  replies `Err("PTY closed")`, `Write` dropped).
- `impl Drop for Reader` emits the `Closed` metadata, replacing
  `ClosedNotifier` — same panic-safety (fires on unwind with
  `exit_code: None`), one less mirror struct. `Drop` does not touch
  `terminal`, utmp, or `pending_out`.
- If `Terminal::new` (or callback registration) fails at startup, no `Reader`
  exists yet; the spawn closure emits `Closed` itself in that branch. (Today
  that failure leaves a forever-listed PTY with a dead reader; this fixes it.)
- `PtyRegistry::destroy` is unchanged: SIGHUP + drop handle → `wakeup_write`
  closes → `POLLHUP`. A disconnected `req_rx` is equivalent but `POLLHUP`
  remains the wake mechanism.

### Error semantics

- All `PtyHandle` request methods map a closed channel to the existing
  "PTY reader thread is dead" error; with an unbounded channel that is the
  only send failure.
- `write()` keeps its `Result` signature but only fails post-mortem.
- Reader panic: pending oneshot replies drop, waking callers with the
  existing "dropped response" errors; `Drop` emits `Closed`.

## Part 2: Stream-lag recovery

### Proto

```proto
enum StreamMetadataReason {
  RESIZE              = 0;
  CLOSED              = 1;
  TITLE_CHANGED       = 2;
  SUBSCRIBERS_CHANGED = 3;
  DATA_LOST           = 4;   // new, additive
}
```

`MetadataReason::DataLost` is added to the Rust enum and the mapping in
`server.rs`.

### Server

In `handle_subscribe`'s forwarding task, both `Lagged(n)` arms — the data
stream *and* the metadata stream — keep the existing `warn!` and additionally
send to this connection only:

```rust
PtyEvent::Metadata(Arc::new(PtyMetadata {
    reason: MetadataReason::DataLost,
    exit_code: None,
    generation: shared.generation.load(Ordering::Relaxed),
    info: shared.info(),
}))
```

The task captures `Arc<PtyShared>` (per the invariant above — capturing
`Arc<PtyHandle>` would keep `wakeup_write` alive and break destroy-by-drop).
The synthetic event travels through the same `sub_tx` as the data it follows,
so the client sees it in stream order. Covering the metadata stream matters
because a lagged-away `Resize`/`TitleChanged` is also repaired by a refresh
(`RefreshResponse` carries cols/rows).

### Client

In the attach render loop (`attach/mod.rs`):

- New arm: `Metadata` with reason `DataLost` for the current PTY sends
  `Command::Refresh`, guarded by a loop-local `refresh_pending: bool` so a
  persistently slow client doesn't amplify lag into a refresh storm.
- The flag is set on send and cleared by the existing `Response::Refresh` arm
  (any refresh clears it regardless of cause — harmless).
- No render-handler changes: all three modes already recover fully from a
  `Refresh` event, and the existing
  `s.generation > current_refresh_gen` filter drops the pre-refresh gap.
- Loop-local state resets naturally on session restart / PTY switch.

## Testing

1. **Moved suites pass unchanged**: boundary, `process_read`, refresh,
   scrollback, subscriber tests relocate with their code.
2. **Write buffering** (`reader.rs`): unit tests drive the write/flush methods
   against a non-blocking pipe standing in for the master fd — fill to
   `EAGAIN`, assert the tail lands in `pending_out`; drain the read end,
   assert flush completes with byte order preserved; assert writes beyond
   `MAX_PENDING_INPUT` are dropped with the buffer intact.
3. **Resize via request**: `registry.create` → `handle.resize().await` →
   assert `info()` dims, a `Resize` metadata on `meta_subscribe` carrying real
   subscriber data, and a subsequent refresh reflecting the new size.
4. **DATA_LOST emission** (deterministic unit test): the forwarding task body
   (currently an inline closure in `handle_subscribe`) is extracted into a free
   function so a test can spawn it directly. The test runs it against
   hand-made `broadcast::channel(8)` senders, pushes enough events
   before draining to force `Lagged`, assert a `DataLost` metadata arrives on
   `sub_tx`. (Overflowing the real 512-capacity channel end-to-end would be
   flaky by construction.)
5. **Client refresh-on-lag**: lives inline in the `run` loop, which is not
   unit-testable as written (known, deferred concern); verified manually via
   `run-termd`.
