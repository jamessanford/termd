# StreamMetadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `StreamMetadata` server-push response so subscribed clients receive structured out-of-band events (resize, closed, title change, subscriber change) interleaved with raw `StreamData`.

**Architecture:** Two broadcast channels per PTY — existing `tx` (raw data, unchanged) and new `meta_tx` (metadata events). The subscription task in `commands.rs` merges both into a unified `PtyEvent` enum on the existing `sub_tx` mpsc channel. `server.rs` converts `PtyEvent::Metadata` to the new `StreamMetadata` proto response.

**Tech Stack:** Rust, tonic 0.12, prost, tokio broadcast + mpsc channels

---

### Files to Create / Modify

| File | Change |
|---|---|
| `proto/terminal.proto` | Add `StreamMetadataReason` enum, `StreamMetadata` message, field 6 on `TerminalResponse` |
| `src/pty.rs` | Add `MetadataReason`, `PtyMetadata`, `PtyEvent` types; `meta_tx` on `PtyHandle`; emit from `resize()` and `reader_thread` |
| `src/commands.rs` | Add `PtyEvent` to sub_tx type; merge two broadcast channels in subscribe task; emit `SUBSCRIBERS_CHANGED`; pass registry to `handle_unsubscribe` |
| `src/server.rs` | Update sub channel type; add `PtyEvent::Metadata` → `StreamMetadata` arm |
| `src/main.rs` | Handle `Response::Metadata` in attach loop — break on `CLOSED` |
| `tests/integration.rs` | Unit tests for `meta_subscribe()` events; gRPC-level test for `CLOSED` |

---

### Task 1: Update proto/terminal.proto

**Files:**
- Modify: `proto/terminal.proto`

- [ ] **Step 1: Add StreamMetadataReason enum and StreamMetadata message**

  In `proto/terminal.proto`, after `message RefreshResponse { ... }`, add:

  ```proto
  enum StreamMetadataReason {
    RESIZE              = 0;
    CLOSED              = 1;
    TITLE_CHANGED       = 2;
    SUBSCRIBERS_CHANGED = 3;
  }

  // NOTE: item.subscribed is always true — recipient is by definition subscribed.
  // When PtyItem is split into PtyState + per-client fields, switch to PtyState.
  message StreamMetadata {
    string               pty_id    = 1;
    PtyItem              item      = 2;
    StreamMetadataReason reason    = 3;
    optional int32       exit_code = 4;
  }
  ```

- [ ] **Step 2: Add metadata field to TerminalResponse oneof**

  In the `TerminalResponse` oneof block, add field 6:

  ```proto
  message TerminalResponse {
    oneof response {
      ListResponse    list     = 1;
      CreateResponse  create   = 2;
      CommandResponse command  = 3;
      StreamData      stream   = 4;
      RefreshResponse refresh  = 5;
      StreamMetadata  metadata = 6;
    }
  }
  ```

- [ ] **Step 3: Rebuild and verify codegen**

  ```bash
  cargo build 2>&1 | tail -5
  ```
  Expected: `Finished` with no errors. The generated code at `target/` will include `StreamMetadata`, `StreamMetadataReason`, and `terminal_response::Response::Metadata`.

- [ ] **Step 4: Commit**

  ```bash
  git add proto/terminal.proto
  git commit -m "feat(proto): add StreamMetadata response type and StreamMetadataReason enum"
  ```

---

### Task 2: Add Rust Metadata Types to pty.rs

**Files:**
- Modify: `src/pty.rs`

- [ ] **Step 1: Derive Clone on PtyInfo**

  `PtyMetadata` embeds a `PtyInfo` snapshot, so `PtyInfo` must be `Clone`. Change the struct declaration:

  ```rust
  #[derive(Clone)]
  pub struct PtyInfo {
      pub id: String,
      pub hostname: String,
      pub pts_name: String,
      pub cols: u32,
      pub rows: u32,
      pub title: String,
      pub created_at: SystemTime,
  }
  ```

- [ ] **Step 2: Add MetadataReason, PtyMetadata, and PtyEvent**

  After the `PtyChunk` struct definition, add:

  ```rust
  #[derive(Clone, Debug)]
  pub enum MetadataReason {
      Resize,
      Closed,
      TitleChanged,
      SubscribersChanged,
  }

  #[derive(Clone, Debug)]
  pub struct PtyMetadata {
      pub reason: MetadataReason,
      pub exit_code: Option<i32>,
      pub info: PtyInfo,
  }

  pub enum PtyEvent {
      Data(Arc<PtyChunk>),
      Metadata(Arc<PtyMetadata>),
  }
  ```

- [ ] **Step 3: Add meta_tx field to PtyHandle**

  In `PtyHandle`, after `refresh_tx`:

  ```rust
  meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
  ```

- [ ] **Step 4: Add meta_subscribe and broadcast_metadata methods**

  In the `impl PtyHandle` block, after `subscribe()`:

  ```rust
  pub fn meta_subscribe(&self) -> broadcast::Receiver<Arc<PtyMetadata>> {
      self.meta_tx.subscribe()
  }

  pub fn broadcast_metadata(&self, meta: Arc<PtyMetadata>) {
      let _ = self.meta_tx.send(meta);
  }
  ```

- [ ] **Step 5: Create meta channel and wire into PtyHandle in PtyRegistry::create()**

  After the existing `let (tx, _) = broadcast::channel::<Arc<PtyChunk>>(512);` line, add:

  ```rust
  let (meta_tx, _) = broadcast::channel::<Arc<PtyMetadata>>(64);
  ```

  In the `Arc::new(PtyHandle { ... })` block, add after `refresh_tx`:

  ```rust
  meta_tx: meta_tx.clone(),
  ```

  (We'll pass `meta_tx` to `reader_thread` in Task 5, so keep the local binding alive for now.)

- [ ] **Step 6: Verify compilation**

  ```bash
  cargo build 2>&1 | grep -E "^error|Finished"
  ```
  Expected: `Finished` with no errors.

- [ ] **Step 7: Commit**

  ```bash
  git add src/pty.rs
  git commit -m "feat: add MetadataReason, PtyMetadata, PtyEvent types and meta_tx to PtyHandle"
  ```

---

### Task 3: Write Failing Unit Tests for Metadata Emission

**Files:**
- Modify: `tests/integration.rs`

- [ ] **Step 1: Add imports at top of tests/integration.rs**

  After the existing `use termd::pty::PtyRegistry;` import, add:

  ```rust
  use termd::pty::{MetadataReason, PtyMetadata};
  ```

- [ ] **Step 2: Write the RESIZE metadata test**

  ```rust
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
  ```

- [ ] **Step 3: Write the TITLE_CHANGED metadata test**

  ```rust
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
  ```

- [ ] **Step 4: Write the CLOSED metadata test**

  ```rust
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
  ```

- [ ] **Step 5: Run tests to confirm they fail (timeout)**

  ```bash
  cargo test test_resize_broadcasts_metadata test_title_change_broadcasts_metadata test_closed_broadcasts_metadata -- --nocapture 2>&1 | tail -20
  ```
  Expected: all three tests fail (timeout = `unwrap_or(false)` → assertion fails). This confirms the infrastructure compiles but the emission isn't wired yet.

- [ ] **Step 6: Commit failing tests**

  ```bash
  git add tests/integration.rs
  git commit -m "test: add failing unit tests for metadata broadcast events"
  ```

---

### Task 4: Emit RESIZE Metadata from PtyHandle::resize()

**Files:**
- Modify: `src/pty.rs`

- [ ] **Step 1: Add MetadataReason import if needed**

  `MetadataReason` and `PtyMetadata` are defined in this file, so no new import needed.

- [ ] **Step 2: Emit Resize metadata in PtyHandle::resize()**

  After the two `.store(...)` calls and before `Ok(())`, add:

  ```rust
  let _ = self.meta_tx.send(Arc::new(PtyMetadata {
      reason: MetadataReason::Resize,
      exit_code: None,
      info: self.info(),
  }));
  ```

  Full updated `resize()` method for reference:
  ```rust
  pub fn resize(&self, cols: u32, rows: u32) -> Result<()> {
      use nix::pty::Winsize;
      use nix::libc;
      let ws = Winsize { ws_col: cols as u16, ws_row: rows as u16, ws_xpixel: 0, ws_ypixel: 0 };
      let fd = { self.writer.lock().unwrap().as_raw_fd() };
      let ret = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &ws as *const Winsize) };
      if ret < 0 {
          return Err(anyhow!("TIOCSWINSZ failed: {}", std::io::Error::last_os_error()));
      }
      self.cols.store(cols, Ordering::Relaxed);
      self.rows.store(rows, Ordering::Relaxed);
      // Notify libghostty Terminal in reader_thread
      let _ = self.resize_tx.try_send((cols, rows));
      let wfd = self.wakeup_write.as_raw_fd();
      let _ = unsafe { libc::write(wfd, [2u8].as_ptr() as *const libc::c_void, 1) };
      // Broadcast updated state to all subscribers
      let _ = self.meta_tx.send(Arc::new(PtyMetadata {
          reason: MetadataReason::Resize,
          exit_code: None,
          info: self.info(),
      }));
      Ok(())
  }
  ```

- [ ] **Step 3: Run the resize metadata test**

  ```bash
  cargo test test_resize_broadcasts_metadata -- --nocapture 2>&1 | tail -5
  ```
  Expected: `test test_resize_broadcasts_metadata ... ok`

- [ ] **Step 4: Commit**

  ```bash
  git add src/pty.rs
  git commit -m "feat: broadcast Resize metadata from PtyHandle::resize()"
  ```

---

### Task 5: Emit TITLE_CHANGED and CLOSED from reader_thread

**Files:**
- Modify: `src/pty.rs`

- [ ] **Step 1: Capture created_at before building the handle**

  In `PtyRegistry::create()`, before the `Arc::new(PtyHandle { ... })` block:

  ```rust
  let created_at = SystemTime::now();
  ```

  Update the handle construction to use this value:
  ```rust
  let handle = Arc::new(PtyHandle {
      ...
      created_at,
      ...
  });
  ```

- [ ] **Step 2: Pass context to reader_thread in PtyRegistry::create()**

  Before the `std::thread::Builder::new()` call, add:

  ```rust
  let meta_tx_for_thread = meta_tx.clone();
  let id_for_thread = id.clone();
  let hostname_for_thread = hostname.clone();
  let pts_name_for_thread = pts_name.clone();
  ```

  Update the `.spawn(move || reader_thread(...))` call to pass these new arguments at the end:

  ```rust
  .spawn(move || reader_thread(
      master_reader, tx, generation, refresh_rx, resize_rx, wakeup_read,
      child, title_for_thread, cols, rows,
      meta_tx_for_thread, id_for_thread, hostname_for_thread,
      pts_name_for_thread, created_at,
  ))
  ```

- [ ] **Step 3: Update reader_thread signature**

  Add the new parameters to the `fn reader_thread(...)` definition:

  ```rust
  fn reader_thread(
      mut master: File,
      tx: broadcast::Sender<Arc<PtyChunk>>,
      generation: Arc<AtomicU64>,
      refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
      resize_rx: std::sync::mpsc::Receiver<(u32, u32)>,
      wakeup_read: OwnedFd,
      mut child: std::process::Child,
      title: Arc<Mutex<String>>,
      init_cols: u32,
      init_rows: u32,
      meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
      pty_id: String,
      hostname: String,
      pts_name: String,
      created_at: SystemTime,
  ) {
  ```

- [ ] **Step 4: Track current dimensions and previous title in reader_thread**

  After the iterator setup code (after `let mut cell_iter_obj = ...`), add:

  ```rust
  let mut current_cols = init_cols;
  let mut current_rows = init_rows;
  let mut prev_title = String::new();
  ```

- [ ] **Step 5: Update the resize drain loop to track current dimensions**

  Replace the existing resize drain:

  ```rust
  while let Ok((cols, rows)) = resize_rx.try_recv() {
      current_cols = cols;
      current_rows = rows;
      if let Err(e) = terminal.resize(cols as u16, rows as u16, 0, 0) {
          tracing::debug!("PTY reader: terminal resize failed: {e}");
      }
  }
  ```

- [ ] **Step 6: Detect title changes after vt_write and emit TitleChanged**

  After `terminal.vt_write(&batch);`, before the generation increment, add:

  ```rust
  let current_title = title.lock().unwrap().clone();
  if current_title != prev_title {
      prev_title = current_title.clone();
      let _ = meta_tx.send(Arc::new(PtyMetadata {
          reason: MetadataReason::TitleChanged,
          exit_code: None,
          info: PtyInfo {
              id: pty_id.clone(),
              hostname: hostname.clone(),
              pts_name: pts_name.clone(),
              cols: current_cols,
              rows: current_rows,
              title: current_title,
              created_at,
          },
      }));
  }
  ```

- [ ] **Step 7: Emit Closed metadata in the exit section**

  In the post-loop exit section, after the `tx.send(...)` of the exit notification chunk, add:

  ```rust
  let exit_code = status.as_ref().and_then(|s| s.code());
  let _ = meta_tx.send(Arc::new(PtyMetadata {
      reason: MetadataReason::Closed,
      exit_code,
      info: PtyInfo {
          id: pty_id.clone(),
          hostname: hostname.clone(),
          pts_name: pts_name.clone(),
          cols: current_cols,
          rows: current_rows,
          title: title.lock().unwrap().clone(),
          created_at,
      },
  }));
  ```

- [ ] **Step 8: Verify compilation**

  ```bash
  cargo build 2>&1 | grep -E "^error|Finished"
  ```
  Expected: `Finished` with no errors.

- [ ] **Step 9: Run the title and closed metadata tests**

  ```bash
  cargo test test_title_change_broadcasts_metadata test_closed_broadcasts_metadata -- --nocapture 2>&1 | tail -10
  ```
  Expected: both tests pass.

- [ ] **Step 10: Commit**

  ```bash
  git add src/pty.rs
  git commit -m "feat: emit TitleChanged and Closed metadata from reader_thread"
  ```

---

### Task 6: Update commands.rs — Merge Channels and Emit SUBSCRIBERS_CHANGED

**Files:**
- Modify: `src/commands.rs`

The `sub_tx` mpsc channel currently carries `(String, Arc<PtyChunk>)`. It must change to `(String, PtyEvent)` so the server can receive both data and metadata events. The subscription task needs to merge the two broadcast channels into this single mpsc channel.

- [ ] **Step 1: Update imports in commands.rs**

  Add to the existing `use crate::pty::...` import:

  ```rust
  use crate::pty::{PtyEvent, PtyInfo, PtyMetadata, MetadataReason, PtyRegistry};
  ```

  Also add `Arc` to the std import if not already present:
  ```rust
  use std::{collections::HashSet, sync::Arc};
  ```

- [ ] **Step 2: Make pty_info_to_item pub**

  Change:
  ```rust
  fn pty_info_to_item(info: PtyInfo, subscribed: bool) -> PtyItem {
  ```
  to:
  ```rust
  pub fn pty_info_to_item(info: PtyInfo, subscribed: bool) -> PtyItem {
  ```

- [ ] **Step 3: Update handle_subscribe signature**

  Change the `sub_tx` parameter type from `&tokio::sync::mpsc::Sender<(String, Arc<crate::pty::PtyChunk>)>` to:

  ```rust
  sub_tx: &tokio::sync::mpsc::Sender<(String, PtyEvent)>,
  ```

- [ ] **Step 4: Replace the subscription task in handle_subscribe**

  Replace the entire `if !subscribed_ids.contains(&id) { ... }` block with:

  ```rust
  if !subscribed_ids.contains(&id) {
      let data_rx = handle.subscribe();
      let meta_rx = handle.meta_subscribe();
      let tx = sub_tx.clone();
      let pty_id_clone = id.clone();
      let task = tokio::spawn(async move {
          use tokio_stream::{StreamExt, wrappers::{BroadcastStream, errors::BroadcastStreamRecvError}};
          let mut data_stream = BroadcastStream::new(data_rx);
          let mut meta_stream = BroadcastStream::new(meta_rx);
          loop {
              tokio::select! {
                  item = data_stream.next() => match item {
                      Some(Ok(chunk)) => {
                          if tx.send((pty_id_clone.clone(), PtyEvent::Data(chunk))).await.is_err() { break; }
                      }
                      Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                          tracing::warn!(pty_id = %pty_id_clone, skipped = n, "data broadcast lagged");
                      }
                      None => break,
                  },
                  item = meta_stream.next() => match item {
                      Some(Ok(meta)) => {
                          if tx.send((pty_id_clone.clone(), PtyEvent::Metadata(meta))).await.is_err() { break; }
                      }
                      Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                          tracing::warn!(pty_id = %pty_id_clone, skipped = n, "meta broadcast lagged");
                      }
                      None => break,
                  },
              }
          }
      });
      sub_tasks.insert(id.clone(), task);
      subscribed_ids.insert(id.clone());
      // Notify all subscribers (including this new one) that subscriber count changed
      handle.broadcast_metadata(Arc::new(PtyMetadata {
          reason: MetadataReason::SubscribersChanged,
          exit_code: None,
          info: handle.info(),
      }));
  }
  ```

- [ ] **Step 5: Update handle_unsubscribe to accept registry and emit SUBSCRIBERS_CHANGED**

  Replace the function signature and body:

  ```rust
  pub fn handle_unsubscribe(
      registry: &PtyRegistry,
      req: UnsubscribeRequest,
      subscribed_ids: &mut HashSet<String>,
      sub_tasks: &mut std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
  ) -> TerminalResponse {
      let id = req.pty_id.clone();
      if let Some(task) = sub_tasks.remove(&id) {
          task.abort();
      }
      subscribed_ids.remove(&id);
      // Notify remaining subscribers that the count changed
      if let Some(handle) = registry.get(&id) {
          handle.broadcast_metadata(Arc::new(PtyMetadata {
              reason: MetadataReason::SubscribersChanged,
              exit_code: None,
              info: handle.info(),
          }));
      }
      ok_response(id)
  }
  ```

- [ ] **Step 6: Verify compilation (will fail until server.rs is updated)**

  ```bash
  cargo build 2>&1 | grep "^error" | head -10
  ```
  Expected: errors in `server.rs` about mismatched types (sub_tx type changed). That's expected — proceed to Task 7.

---

### Task 7: Update server.rs

**Files:**
- Modify: `src/server.rs`

- [ ] **Step 1: Update imports**

  Add to the top of server.rs:

  ```rust
  use crate::pty::{PtyEvent, PtyMetadata, MetadataReason};
  use crate::commands;
  ```

- [ ] **Step 2: Update sub_tx and sub_rx types**

  In the `stream()` method, change:

  ```rust
  let (sub_tx, mut sub_rx) = mpsc::channel::<(String, Arc<PtyChunk>)>(1024);
  ```
  to:
  ```rust
  let (sub_tx, mut sub_rx) = mpsc::channel::<(String, PtyEvent)>(1024);
  ```

- [ ] **Step 3: Update the Unsubscribe dispatch to pass registry**

  In `dispatch_command`, change:

  ```rust
  Some(Command::Unsubscribe(r)) => commands::handle_unsubscribe(r, subscribed_ids, sub_tasks),
  ```
  to:
  ```rust
  Some(Command::Unsubscribe(r)) => commands::handle_unsubscribe(registry, r, subscribed_ids, sub_tasks),
  ```

- [ ] **Step 4: Update the sub_rx select arm to handle PtyEvent**

  Replace the existing `Some((pty_id, chunk)) = sub_rx.recv() => { ... }` arm with:

  ```rust
  Some((pty_id, event)) = sub_rx.recv() => {
      let resp = match event {
          PtyEvent::Data(chunk) => proto::TerminalResponse {
              response: Some(proto::terminal_response::Response::Stream(
                  proto::StreamData {
                      pty_id,
                      generation: chunk.generation,
                      data: chunk.data.to_vec(),
                  }
              )),
          },
          PtyEvent::Metadata(meta) => {
              use proto::StreamMetadataReason;
              let reason = match meta.reason {
                  MetadataReason::Resize             => StreamMetadataReason::Resize,
                  MetadataReason::Closed             => StreamMetadataReason::Closed,
                  MetadataReason::TitleChanged       => StreamMetadataReason::TitleChanged,
                  MetadataReason::SubscribersChanged => StreamMetadataReason::SubscribersChanged,
              };
              proto::TerminalResponse {
                  response: Some(proto::terminal_response::Response::Metadata(
                      proto::StreamMetadata {
                          pty_id,
                          item: Some(commands::pty_info_to_item(meta.info.clone(), true)),
                          reason: reason as i32,
                          exit_code: meta.exit_code,
                      }
                  )),
              }
          }
      };
      if resp_tx.send(Ok(resp)).await.is_err() { break; }
  }
  ```

- [ ] **Step 5: Verify full compilation**

  ```bash
  cargo build 2>&1 | grep -E "^error|Finished"
  ```
  Expected: `Finished` with no errors.

- [ ] **Step 6: Run all existing tests**

  ```bash
  cargo test 2>&1 | tail -15
  ```
  Expected: all existing tests pass plus the three new metadata tests.

- [ ] **Step 7: Commit**

  ```bash
  git add src/commands.rs src/server.rs
  git commit -m "feat: wire PtyEvent through subscription pipeline; emit SUBSCRIBERS_CHANGED"
  ```

---

### Task 8: Update main.rs Attach Client + gRPC Integration Test

**Files:**
- Modify: `src/main.rs`
- Modify: `tests/integration.rs`

- [ ] **Step 1: Write failing gRPC-level CLOSED test**

  In `tests/integration.rs`, add:

  ```rust
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
  ```

- [ ] **Step 2: Run the gRPC test to confirm it compiles but fails**

  ```bash
  cargo test test_subscribe_receives_closed_metadata -- --nocapture 2>&1 | tail -10
  ```
  Expected: test fails because `main.rs` hasn't been updated and the gRPC client hasn't been updated (but the server side should now be working — the test should actually pass at this point if the server is correct). If it passes, skip to Step 3 as-is.

- [ ] **Step 3: Handle Response::Metadata in the attach receive loop in main.rs**

  In the `Cmd::Attach` branch, find the main receive loop `loop { tokio::select! { ... } }`. In the `msg = resp_rx.message()` arm, update the inner match to add the Metadata variant after the Stream arm:

  ```rust
  Ok(Some(r)) => {
      match r.response {
          Some(Response::Stream(s)) => {
              if s.generation > refresh_gen {
                  if debug {
                      eprintln!("[Stream gen={} len={}]", s.generation, s.data.len());
                  } else {
                      if stdout.write_all(&s.data).await.is_err() { break; }
                      let _ = stdout.flush().await;
                  }
              }
          }
          Some(Response::Metadata(m)) => {
              use termd::proto::StreamMetadataReason;
              if debug {
                  eprintln!(
                      "[Metadata reason={} pty_id={}]",
                      m.reason, m.pty_id
                  );
              }
              if m.reason == StreamMetadataReason::Closed as i32 {
                  break;
              }
          }
          _ => {}
      }
  }
  ```

  Also add `StreamMetadataReason` to the imports at the top of main.rs in the `use termd::proto::{ ... }` block.

- [ ] **Step 4: Compile and run all tests**

  ```bash
  cargo build 2>&1 | grep -E "^error|Finished"
  cargo test 2>&1 | tail -20
  ```
  Expected: `Finished` with no errors; all tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/main.rs tests/integration.rs
  git commit -m "feat: handle StreamMetadata in attach client; add gRPC-level closed test"
  ```
