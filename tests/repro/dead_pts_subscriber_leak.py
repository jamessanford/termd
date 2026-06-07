#!/usr/bin/env python3
"""Regression guard: subscriber leak when the daemon loses its controlling terminal.

Background
----------
A backgrounded ``termd start`` can outlive the terminal it was launched from. Its
stdout/stderr then point at a deleted pts and every write returns EIO. On an
*abrupt* client disconnect (the h2 "broken pipe" that ``termd attach`` produces
when it exits) the stream loop logs ``tracing::warn!("stream read error: ...")``.
That write hits EIO; tracing-subscriber falls back to ``eprintln!``, which itself
panics ("failed printing to stderr"); the panic unwinds the stream task *before*
its disconnect-cleanup runs, so the client's subscriber entry is never reaped.
Repeat attach/detach and the stale "sessions" pile up in ``termd list --verbose``.

The fix routes the daemon's tracing output through a writer that swallows write
errors (``IgnoreWriteErrors`` in ``src/main.rs``), so a dead terminal drops log
lines instead of panicking a task.

What this script does
---------------------
Reproduces the exact condition that Cargo's in-process integration tests cannot:
it starts a real daemon with stdout/stderr on a pty, closes the pty master to make
those fds EIO (mimicking the lost controlling terminal), then runs a few real
attach -> ``C-a d`` detach cycles and checks that no subscribers are left behind.

  * Before the fix: subscribers accumulate (0 -> 1 -> 2 -> ...). EXIT 1.
  * After the fix:  subscribers stay at 0. EXIT 0.

Usage
-----
    cargo build
    python3 tests/repro/dead_pts_subscriber_leak.py

Set ``TERMD_BIN`` to point at a specific binary; otherwise target/debug then
target/release is used.
"""

import os
import pty
import select
import struct
import subprocess
import sys
import tempfile
import termios
import time
import fcntl

CYCLES = 3
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def find_binary() -> str:
    env = os.environ.get("TERMD_BIN")
    if env:
        return env
    for rel in ("target/debug/termd", "target/release/termd"):
        cand = os.path.join(REPO_ROOT, rel)
        if os.path.exists(cand):
            return cand
    sys.exit("termd binary not found; run `cargo build` first (or set TERMD_BIN)")


def pick_shell() -> str:
    for cand in (os.environ.get("SHELL"), "/bin/bash", "/bin/sh"):
        if cand and os.access(cand, os.X_OK):
            return cand
    sys.exit("no usable shell found for the test PTY")


BIN = find_binary()
SHELL = pick_shell()


def list_verbose(sock: str) -> str:
    return subprocess.run(
        [BIN, "list", "--socket", sock, "--verbose"],
        capture_output=True, text=True, env=dict(os.environ, SHELL=SHELL),
    ).stdout


def subscriber_count(sock: str) -> int:
    # Subscriber rows are indented and begin with "(", e.g.
    #   ( <uuid> hn08 80x24 )
    # PTY header rows never start with "(".
    return sum(1 for ln in list_verbose(sock).splitlines() if ln.strip().startswith("("))


def attach_detach_cycle(sock: str, pty_id: str) -> None:
    """Attach to `pty_id` in a child PTY, send C-a d, wait for the child to go."""
    env = dict(os.environ, SHELL=SHELL)
    pid, fd = pty.fork()
    if pid == 0:  # child: become the attach client
        os.execve(BIN, [BIN, "attach", pty_id, "--socket", sock], env)
    # parent: give the client a real window size, let it subscribe, then detach.
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    time.sleep(1.2)
    os.write(fd, b"\x01")   # C-a
    time.sleep(0.15)
    os.write(fd, b"d")      # detach
    deadline = time.time() + 5
    while time.time() < deadline:
        if os.waitpid(pid, os.WNOHANG)[0]:
            break
        r, _, _ = select.select([fd], [], [], 0.1)
        if r:
            try:
                os.read(fd, 4096)
            except OSError:
                break
    try:
        os.close(fd)
    except OSError:
        pass


def main() -> int:
    workdir = tempfile.mkdtemp(prefix="termd-deadpts-")
    sock = os.path.join(workdir, "termd.sock")
    env = dict(os.environ, SHELL=SHELL)

    # Start the daemon with stdout/stderr on a pty, in its own session so closing
    # the pty master won't SIGHUP it. Then close the master: the daemon's fd 1/2
    # now write to a deleted pts and fail with EIO -- exactly the lost-controlling
    # -terminal state that triggered the bug.
    master, slave = os.openpty()
    daemon = subprocess.Popen(
        [BIN, "start", "--socket", sock, "--listen", "127.0.0.1:0"],
        stdin=subprocess.DEVNULL, stdout=slave, stderr=slave,
        start_new_session=True, env=env,
    )
    os.close(slave)
    time.sleep(1.0)
    os.close(master)  # kill the "terminal": writes to fd 1/2 now return EIO
    time.sleep(0.3)

    try:
        created = subprocess.run(
            [BIN, "create", "--socket", sock],
            capture_output=True, text=True, env=env,
        )
        pty_id = created.stdout.strip()
        if not pty_id:
            print(f"FAIL: could not create a PTY: {created.stderr.strip()!r}")
            return 1

        before = subscriber_count(sock)
        print(f"subscribers before cycles: {before}")
        for i in range(CYCLES):
            attach_detach_cycle(sock, pty_id)
            time.sleep(0.6)
            n = subscriber_count(sock)
            print(f"subscribers after attach/detach cycle {i + 1}: {n}")

        leaked = subscriber_count(sock)
        if leaked != 0:
            print(
                f"\nFAIL: {leaked} subscriber(s) leaked after {CYCLES} clean "
                f"detach cycles (expected 0).\n{list_verbose(sock)}"
            )
            return 1
        print(f"\nPASS: no subscribers leaked across {CYCLES} attach/detach cycles.")
        return 0
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=5)
        except subprocess.TimeoutExpired:
            daemon.kill()


if __name__ == "__main__":
    sys.exit(main())
