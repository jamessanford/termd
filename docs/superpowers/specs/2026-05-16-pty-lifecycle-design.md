---
name: pty lifecycle design
description: Design spec for PTY reader wakeup pipe, destroy cleanup fix, exit notification, and attach --debug flag
type: project
---

# PTY Lifecycle Improvements

## Overview

Four related improvements to PTY lifecycle management and the `attach` CLI:

1. **Pipe-based reader wakeup** — replace the 50 ms `poll()` timeout with an event-driven pipe so the reader wakes immediately for refresh requests and exits cleanly when the handle is dropped.
2. **Destroy cleanup fix** — move child ownership to the reader thread; eliminate the race between `destroy()` and the reader on child reaping.
3. **Exit notification** — broadcast `[Command {title} exited …]` to subscribers when the PTY closes, for any reason.
4. **`attach --debug`** — print message metadata to stderr instead of raw bytes, for diagnostics.

---

## Part 1: Pipe-based wakeup (Tasks 1 & 2)

### Motivation

The current `reader_thread` calls `poll([master_fd], 50ms)` so it can service `refresh_rx` requests even when the PTY is idle. This adds up to 50 ms latency on every refresh and keeps the thread awake polling when nothing is happening. It also doesn't detect when `PtyHandle` is dropped (write side of broadcast stays open until the child exits).

### Design

**`PtyRegistry::create()`**

- Call `nix::unistd::pipe()` → `(wakeup_read: OwnedFd, wakeup_write: OwnedFd)`.
- Set `FD_CLOEXEC` on both ends via `fcntl`.
- Store `wakeup_write` in `PtyHandle`.
- Pass `wakeup_read` and the `Child` struct directly to `reader_thread`.

**`PtyHandle`**

- Add field: `wakeup_write: std::os::fd::OwnedFd`.
- Remove field: `child: Mutex<Option<std::process::Child>>` — reader thread owns child exclusively.
- Keep `child_pid: u32` for the SIGHUP call in `destroy()`.

**`PtyHandle::refresh()`**

After `refresh_tx.send(tx)`, write one byte to `wakeup_write` via `libc::write`. The reader wakes immediately.

**`reader_thread()` new parameters**

```
wakeup_read: OwnedFd
child: std::process::Child
```

(`title: Arc<Mutex<String>>` is already passed; the exit message reads it at close time.)

**Reader poll loop**

Replace `poll([master_fd], 50ms)` with `poll([master_fd, wakeup_read_fd], -1)`:

| fd | event | action |
|---|---|---|
| `wakeup_read_fd` | `POLLIN` | drain bytes (non-blocking read up to 64 bytes); refresh_rx drained at top of next iteration |
| `wakeup_read_fd` | `POLLHUP` | write end closed (PtyHandle dropped) — break main loop |
| `master_fd` | `POLLIN` | read PTY bytes, feed to terminal, broadcast chunk |
| `master_fd` | `POLLHUP` / read returns EIO | break main loop |

**`PtyRegistry::destroy()`**

- Send SIGHUP to child via `child_pid`.
- Drop the handle (removes from registry, closes `wakeup_write` → POLLHUP fires in reader).
- Remove the `std::thread::spawn(|| child.wait())` — reader now handles reaping.

### Cleanup chain

```
destroy() called
  → SIGHUP sent to child
  → handle Arc dropped → wakeup_write closed → reader POLLHUP
  → reader breaks main loop
  → reader calls child.wait() (or try_wait)
  → reader broadcasts exit notification
  → reader returns → broadcast tx dropped
  → subscription tasks see stream closed → exit
```

If SIGHUP kills the child before the POLLHUP fires, the EIO path triggers first — either way the reader exits.

---

## Part 2: Exit notification (Task 3)

After `reader_thread` breaks out of its main loop:

**Get exit status**

1. Call `child.try_wait()` (non-blocking).
2. If `None` (child not yet dead — POLLHUP path, SIGHUP still propagating), call `child.wait()` (blocks briefly; child will die shortly after SIGHUP).
3. If `wait()` errors, treat as unknown.

**Format the message**

Read the current title from `title: Arc<Mutex<String>>`.

| Condition | Message |
|---|---|
| Exited with code N | `\r\n[Command {title} exited with code {N}]\r\n` |
| Killed by signal | `\r\n[Command {title} was killed]\r\n` |
| Wait failed / unknown | `\r\n[Command {title} terminated]\r\n` |

**Broadcast**

Emit as a `PtyChunk` with `generation = prev_gen + 1`. This generation is greater than `refresh_gen`, so it passes the attach client's filter and appears in the terminal.

After broadcasting, the reader returns. The broadcast `tx` drops, closing the channel and signaling all subscription tasks to exit.

**Behavior with active attach client**

If a client is attached when `destroy` is called, it sees the exit notification appear in the terminal, then the stream closes. This is the intended UX.

---

## Part 3: `attach --debug` (Task 4)

### CLI change

Add to `Cmd::Attach`:

```rust
#[arg(long)]
debug: bool,
```

### Receive loop behavior

**Normal mode** (unchanged): write `s.data` bytes to stdout and flush.

**Debug mode**: print metadata to stderr, write nothing to stdout.

- On `RefreshResponse` (during startup): `eprintln!("[Refresh gen={} len={}]", refresh_gen, refresh_bytes.len())`
- On `StreamData` in main loop: `eprintln!("[Stream gen={} len={}]", s.generation, s.data.len())`

Stdin forwarding (`WriteRequest`) is unaffected in both modes.

---

## Testing

- **Existing tests**: `test_refresh_returns_screen_data` covers the refresh pipeline end-to-end; `test_write_produces_broadcast_output` covers broadcast. Both should continue to pass unchanged.
- **Cleanup**: add a test that creates a PTY, subscribes, calls `destroy`, and verifies the broadcast receiver sees the stream close (i.e., `recv()` returns `Closed`).
- **Exit notification**: add a test that creates a PTY, writes `exit\n`, waits for the broadcast to include `exited with code` bytes, then verifies the broadcast closes.
- **`--debug`**: manual smoke test only (requires terminal).

---

## No proto changes

All four improvements work within the existing proto schema.
