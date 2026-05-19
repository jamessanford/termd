# Attach: Closed-PTY Freeze-in-Place Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When the subscribed PTY closes, keep the attach render loop alive so the user can still detach or switch PTYs via `^A`.

**Architecture:** Each render mode (`cell`, `formatter`, `raw`, `region`) gets a `pty_closed: bool` flag. On `Metadata::Closed`, set the flag and print a notice instead of breaking. The existing `action_rx` select arm already handles all subsequent input correctly. No changes to `mod.rs`, `input.rs`, or `RunOutcome`/`RunContext`.

**Tech Stack:** Rust, Tokio, tonic gRPC streaming, libghostty-vt

---

### Task 1: cell.rs — freeze on PTY close

**Files:**
- Modify: `src/attach/cell.rs`

The `run()` loop currently breaks with `RunOutcome::ServerClosed` on `Metadata::Closed`. Change it to set a flag and continue.

- [ ] **Step 1: Add `pty_closed` flag and update `Metadata::Closed` arm**

Find this block in `src/attach/cell.rs` inside the `loop { tokio::select! { ... } }`:

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        lt.resize(mi.cols, mi.rows)?;
                                        out.extend_from_slice(b"\x1b[2J");
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                break;
                            }
                        },
```

Add `let mut pty_closed = false;` immediately before the `loop {` line. Then replace the `Metadata::Closed` arm:

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        lt.resize(mi.cols, mi.rows)?;
                                        out.extend_from_slice(b"\x1b[2J");
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                if !pty_closed {
                                    pty_closed = true;
                                    eprintln!("\r\n[PTY closed]");
                                }
                            }
                        },
```

The `_ => { break; }` stream-error arm is unchanged — that covers true gRPC disconnects and still returns `ServerClosed`.

- [ ] **Step 2: Build**

```
cargo build 2>&1 | head -30
```

Expected: no errors. A warning about `pty_closed` being set but not read is acceptable (it guards the duplicate-message case); suppress with `let mut _pty_closed` if needed, but the flag IS read by the `if !pty_closed` guard so there should be no warning.

- [ ] **Step 3: Run existing tests**

```
cargo test --lib attach::cell 2>&1
```

Expected: all existing tests pass (they test `render_dirty`, not the async loop).

- [ ] **Step 4: Commit**

```bash
git add src/attach/cell.rs
git commit -m "fix(attach): freeze in place on PTY close in cell mode"
```

---

### Task 2: formatter.rs — freeze on PTY close

**Files:**
- Modify: `src/attach/formatter.rs`

Identical pattern to Task 1. The `Metadata::Closed` arm in `formatter.rs`'s `run()` loop is the same structure.

- [ ] **Step 1: Add `pty_closed` flag and update `Metadata::Closed` arm**

Find this block in `src/attach/formatter.rs` inside `loop { tokio::select! { ... } }`:

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        lt.resize(mi.cols, mi.rows)?;
                                        out.extend_from_slice(b"\x1b[2J");
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                break;
                            }
                        },
```

Add `let mut pty_closed = false;` immediately before the `loop {` line. Replace the `Metadata::Closed` arm:

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                if let Some(ref mi) = m.item {
                                    if mi.cols > 0 && mi.rows > 0 {
                                        lt.resize(mi.cols, mi.rows)?;
                                        out.extend_from_slice(b"\x1b[2J");
                                    }
                                }
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                if !pty_closed {
                                    pty_closed = true;
                                    eprintln!("\r\n[PTY closed]");
                                }
                            }
                        },
```

- [ ] **Step 2: Build**

```
cargo build 2>&1 | head -30
```

Expected: no errors.

- [ ] **Step 3: Run existing tests**

```
cargo test --lib attach::formatter 2>&1
```

Expected: all existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/attach/formatter.rs
git commit -m "fix(attach): freeze in place on PTY close in formatter mode"
```

---

### Task 3: raw.rs — freeze on PTY close

**Files:**
- Modify: `src/attach/raw.rs`

`raw.rs` has no local terminal model. Its `Metadata::Closed` arm is the same one-liner. SIGWINCH still fires a server `Refresh` request — the server returns the frozen final screen.

- [ ] **Step 1: Add `pty_closed` flag and update `Metadata::Closed` arm**

Find this block in `src/attach/raw.rs` inside `loop { tokio::select! { ... } }`:

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                // Clear stale content; server will broadcast a Refresh next
                                let _ = stdout.write_all(b"\x1b[2J").await;
                                let _ = stdout.flush().await;
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                break;
                            }
                        },
```

Add `let mut pty_closed = false;` immediately before the `loop {` line. Replace the `Metadata::Closed` arm:

```rust
                        Some(Response::Metadata(m)) => {
                            if m.reason == StreamMetadataReason::Resize as i32 {
                                // Clear stale content; server will broadcast a Refresh next
                                let _ = stdout.write_all(b"\x1b[2J").await;
                                let _ = stdout.flush().await;
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                if !pty_closed {
                                    pty_closed = true;
                                    eprintln!("\r\n[PTY closed]");
                                }
                            }
                        },
```

- [ ] **Step 2: Build**

```
cargo build 2>&1 | head -30
```

Expected: no errors.

- [ ] **Step 3: Commit**

(`raw.rs` has no unit tests.)

```bash
git add src/attach/raw.rs
git commit -m "fix(attach): freeze in place on PTY close in raw mode"
```

---

### Task 4: region.rs — freeze on PTY close

**Files:**
- Modify: `src/attach/region.rs`

`region.rs` is more complex: its loop has a `fallback_ctx` mechanism and post-loop margin-cleanup code. The `Metadata::Closed` break currently skips straight to that cleanup. With the freeze, the cleanup is deferred until the user fires an action (the `action_rx` arm already resets margins before returning).

- [ ] **Step 1: Add `pty_closed` flag and update `Metadata::Closed` arm**

Find this block in `src/attach/region.rs` inside the main `loop { tokio::select! { ... } }`:

```rust
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                break;
                            }
```

Add `let mut pty_closed = false;` immediately before the `loop {` line (it already has `let mut fallback_ctx: Option<super::RunContext> = None;` just before the loop — add `pty_closed` on the line after that). Replace the `Metadata::Closed` arm:

```rust
                            } else if m.reason == StreamMetadataReason::Closed as i32 {
                                if !pty_closed {
                                    pty_closed = true;
                                    eprintln!("\r\n[PTY closed]");
                                }
                            }
```

No other changes. The post-loop margin cleanup (`\x1b[r`, `\x1b[?69l`) still runs when the loop breaks due to a true stream close (`_ => { break; }`) or a fallback-triggered break. When the user fires an action while frozen, the `action_rx` arm's existing cleanup (`stdout.write_all(b"\x1b[r")`, etc.) handles margin teardown correctly.

- [ ] **Step 2: Build**

```
cargo build 2>&1 | head -30
```

Expected: no errors.

- [ ] **Step 3: Run existing tests**

```
cargo test --lib attach::region 2>&1
```

Expected: all existing VtFilter tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/attach/region.rs
git commit -m "fix(attach): freeze in place on PTY close in region mode"
```
