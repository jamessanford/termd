# Subscriber reap leak — ROOT CAUSE FOUND & FIXED

**Status:** root-caused and fixed (uncommitted). Reproduced reliably, fix verified.

## Symptom

`termd list --verbose` keeps listing subscribers ("sessions") for `termd attach`
processes that have already exited. Repeating attach → `C-a d` → list accumulates
stale subscriber entries. Only happened on a daemon that had **lost its
controlling terminal** but was left running in the background.

## Root cause

The daemon's `tracing` logs go to **stdout/stderr**. When `termd start` is
backgrounded and its controlling terminal later closes, the process keeps
running but `fd 0/1/2 → /dev/pts/N (deleted)` — every write returns **EIO**.

On an **abrupt** client disconnect (the h2 "broken pipe" error that `termd
attach` produces when it exits), the stream loop logs:

```rust
// src/server.rs, inbound branch
Some(Err(e)) => {
    tracing::warn!("stream read error: {e}");  // <-- panics here when stdout is EIO
    break;                                      // <-- never reached
}
```

That `warn!` write fails (EIO). `tracing-subscriber` reacts to a writer error by
falling back to `eprintln!` to report it — and `eprintln!` **panics** when stderr
is also EIO:

```
thread 'main' panicked at library/std/src/io/stdio.rs:1165:9:
failed printing to stderr: Input/output error (os error 5)
```

The panic unwinds the stream task **at the `warn!`, before `break`**, so the
disconnect cleanup that removes the subscriber never runs → **leak**.

Why it was so slippery:
- A **clean** half-close (`inbound.next() == None`) logs nothing → no panic →
  cleanup runs → reaps fine. That's every fresh-daemon repro and the unit/integration
  tests.
- Only **abrupt** disconnects hit the `warn!`, and `termd attach` always
  disconnects abruptly (h2 error). So on a terminal-less daemon, *every*
  attach/detach leaks; on a healthy daemon, *none* do.

### Evidence (strace of the live leaking daemon, pid 117872)

```
write(1, "<log line>", 3798)                                = -1 EIO
write(2, "[tracing-subscriber] Unable to write an event ...", 88) = -1 EIO
write(2, "...panicked at library/std/src/io/stdio.rs:1165:9:
            failed printing to stderr: Input/output error (os error 5)", 131) = -1 EIO
```
`fd 1/2 → /dev/pts/1 (deleted)`; `lsof` showed no open connections yet the
subscribers persisted; daemon idle in `do_epoll_wait`.

## The fix (uncommitted)

**`src/main.rs`** — wrap the tracing writer so write errors are swallowed instead
of triggering the panic-y `eprintln!` fallback:

```rust
struct IgnoreWriteErrors<W>(W);
impl<W: std::io::Write> std::io::Write for IgnoreWriteErrors<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let _ = self.0.write_all(buf);
        Ok(buf.len())               // claim success even on EIO
    }
    fn flush(&mut self) -> std::io::Result<()> { let _ = self.0.flush(); Ok(()) }
}
```
wired in via `tracing_subscriber::fmt().with_writer(|| IgnoreWriteErrors(std::io::stdout()))`.

Now a dead terminal degrades to dropped log lines instead of a task-killing panic.

### Defense-in-depth: RAII cleanup guards (implemented)

The writer fix removes the *cause* of the panic, but two RAII guards now make the
cleanup itself panic-proof, so any future unwind in these tasks can't strand state:

- **`ConnReaper` (`src/server.rs`)** — owns a connection's `subscribed_ids` /
  `sub_tasks`; its `Drop` reaps the client's subscribers and aborts its forwarding
  tasks. Replaces the old straight-line cleanup after the select loop, so it runs
  whether the task ends cleanly or unwinds. Verified: with the `main.rs` writer fix
  *temporarily disabled* (panic firing on every abrupt disconnect), the repro still
  shows 0 leaks — the panic unwinds through `Drop` and reaps.

- **`ReaderExitGuard` (`src/pty.rs`)** — armed for the life of `reader_thread`'s
  main loop, disarmed once the normal exit path has sent its (richer) `Closed`
  event. If the loop unwinds instead, the guard's `Drop` still broadcasts a minimal
  `Closed` and drops the utmp record, so a panicking reader can't leave clients
  hung on a dead PTY. The drop path deliberately does not touch the libghostty
  `Terminal` (which may be unsafe to use mid-panic).

An earlier `subscribed_ids`-drift safety net (`remove_subscriber -> bool` +
registry-wide orphan scan with a WARN canary) was prototyped and reverted: it
didn't help, because the panic aborts the task *before* any cleanup runs. The
`Drop`-guard approach supersedes it.

## Verification

Controlled repro `tests/repro/dead_pts_subscriber_leak.py`: starts a daemon with stdout/stderr on a
pty, closes the pty master (→ EIO writes, mimicking the lost terminal), then runs
attach/`C-a d` cycles and counts subscribers.

- **Before fix:** subscribers 0 → 1 → 2 (leaks every cycle).
- **After fix:**  subscribers 0 → 0 → 0 (clean).

Full suite: `cargo test` → 48 + 102 + 26 pass.

## Follow-ups to consider

- Add `tracing::warn!` (and friends) defensively elsewhere? Not needed — the
  writer fix covers all log sites globally.
- Consider proper daemonization: redirect std fds to a log file or `/dev/null`
  at startup so the daemon never depends on an inherited terminal at all. The
  writer fix makes this optional, but it's the cleaner long-term posture.
- A regression test for the panic path is awkward in-process (needs a broken fd +
  the global tracing subscriber), which is why the guard lives as an out-of-process
  script: `tests/repro/dead_pts_subscriber_leak.py` (PASS/exit 0 when fixed,
  FAIL/exit 1 when leaking). Run after `cargo build`; honors `TERMD_BIN`.

## Note for the live daemon (pid 117872)

That process is the **old binary** without the fix; its accumulated orphans clear
on restart. Rebuild and restart `termd start` to pick up the fix.
