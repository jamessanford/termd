#!/usr/bin/env python3
"""Regression guard: dynamic colors (OSC 10/11/12) and window title leaking across PTYs.

Standalone Python twin of the cargo test in ``tests/client_e2e.rs``
(``attach_clears_colors_and_title_on_switch_and_exit``) — same scenario and
assertions, handy for quick iteration without a cargo build cycle for the test
harness itself.

Background
----------
The refresh formatter emits OSC 10/11/12 and OSC 0 only when the target PTY has
a non-default value (``dynamicColorOverride`` returns null for unset), the same
emit-nothing-when-unset asymmetry as the kitty keyboard flags. Without an
explicit clear-to-baseline, a cursor/background color or title set by an app in
PTY A survived a switch to PTY B, bled into the client UI screens, and persisted
in the user's real terminal after detach. The ``2J`` in the refresh preamble also
ran while the stale OSC 11 background was still active, clearing the screen to
the previous PTY's background color.

The fix adds OSC 110/111/112 + an empty-title OSC 0 to both the ``do_refresh``
preamble (colors before the 2J) and ``RESET_TERMINAL_MODES``, and push/pops the
xterm title stack (CSI 22;0t / 23;0t) around the attach session so the host
terminal's own title is restored on exit.

What this script does
---------------------
Drives the real client binary under a pty: creates two PTYs, has PTY A set a
cursor color, background color, and title while PTY B stays plain, then attaches
to A, switches to B (C-a 1), and detaches (C-a d), asserting on the client's raw
output bytes at each step.

  * Before the fix: no OSC 110/111/112 or title clear on the switch; A's colors
    and title follow the client onto B and out of the session. EXIT 1.
  * After the fix:  clears precede the 2J and restores; nothing leaks. EXIT 0.

Usage
-----
    cargo build
    python3 tests/repro/color_title_reset.py

Set ``TERMD_BIN`` to point at a specific binary; otherwise target/debug then
target/release is used.
"""

import fcntl
import os
import pty
import struct
import subprocess
import sys
import tempfile
import termios
import time

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


BIN = find_binary()


def drain(fd: int, duration: float) -> bytes:
    """Read everything the client writes to its terminal for `duration` seconds."""
    os.set_blocking(fd, False)
    buf = b""
    deadline = time.time() + duration
    while time.time() < deadline:
        try:
            chunk = os.read(fd, 65536)
            if chunk:
                buf += chunk
        except (BlockingIOError, OSError):
            time.sleep(0.05)
    return buf


def main() -> int:
    workdir = tempfile.mkdtemp(prefix="termd-colorreset-")
    sock = os.path.join(workdir, "termd.sock")

    daemon = subprocess.Popen(
        [BIN, "start", "--socket", sock, "--listen", "127.0.0.1:0"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(0.8)

    try:
        def create() -> str:
            out = subprocess.check_output(
                [BIN, "create", "--socket", sock, "--cols", "80", "--rows", "24",
                 "--cmd", "sh"], text=True)
            return out.split()[0]

        pty_a = create()
        pty_b = create()

        # PTY A recolors the cursor + background and sets a title; B stays plain.
        subprocess.run(
            [BIN, "send", "--socket", sock, pty_a,
             "printf '\\033]12;#ff8800\\007\\033]11;#112233\\007\\033]0;PTY-A-TITLE\\007'\r"],
            stdout=subprocess.DEVNULL)
        time.sleep(0.5)

        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
        client = subprocess.Popen(
            [BIN, "attach", pty_a, "--socket", sock],
            stdin=slave, stdout=slave, stderr=subprocess.DEVNULL, close_fds=True)
        os.close(slave)

        attach = drain(master, 1.5)
        os.write(master, b"\x011")  # C-a 1: switch to PTY B (0-indexed list)
        switch = drain(master, 1.5)
        os.write(master, b"\x01d")  # C-a d: detach
        detach = drain(master, 1.5)

        client.terminate()
        try:
            client.wait(timeout=5)
        except subprocess.TimeoutExpired:
            client.kill()
        try:
            os.close(master)
        except OSError:
            pass

        checks = [
            ("attach: title pushed (CSI 22;0t)", b"\x1b[22;0t" in attach),
            ("attach: OSC 12 restore for PTY A", b"\x1b]12;rgb:ff/88/00" in attach),
            ("attach: OSC 11 restore for PTY A", b"\x1b]11;rgb:11/22/33" in attach),
            ("attach: title restore for PTY A", b"\x1b]0;PTY-A-TITLE" in attach),
            ("switch: cursor-color reset (OSC 112)", b"\x1b]112\x1b\\" in switch),
            ("switch: default-fg reset (OSC 110)", b"\x1b]110\x1b\\" in switch),
            ("switch: default-bg reset (OSC 111)", b"\x1b]111\x1b\\" in switch),
            ("switch: empty-title clear", b"\x1b]0;\x1b\\" in switch),
            ("switch: bg reset precedes 2J",
             b"\x1b]111\x1b\\" in switch
             and switch.find(b"\x1b]111\x1b\\") < switch.rfind(b"\x1b[2J")),
            ("switch: no OSC color restore leaks into plain B",
             b"\x1b]12;rgb:" not in switch and b"\x1b]11;rgb:" not in switch),
            ("switch: no PTY-A title leaks into plain B",
             b"\x1b]0;PTY-A-TITLE" not in switch),
            ("detach: colors reset",
             b"\x1b]112\x1b\\" in detach and b"\x1b]111\x1b\\" in detach),
            ("detach: title popped (CSI 23;0t)", b"\x1b[23;0t" in detach),
        ]

        failed = False
        for name, ok in checks:
            print(("PASS" if ok else "FAIL"), name)
            failed |= not ok
        if failed:
            print("\nFAIL: colors/title leaked across the PTY switch or session exit.")
            return 1
        print("\nPASS: colors and title cleared on switch and restored on detach.")
        return 0
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=5)
        except subprocess.TimeoutExpired:
            daemon.kill()


if __name__ == "__main__":
    sys.exit(main())
