// End-to-end tests that drive the real `termd` binaries: a daemon process plus an
// `attach` client running under a pty, asserting on the raw bytes the client
// writes to its terminal. Unlike blackbox.rs (in-process server over gRPC), these
// exercise the client attach loop itself — reset_terminal_modes, refresh
// application, and session enter/exit — which no in-process test reaches.
//
// `CARGO_BIN_EXE_termd` makes cargo build the binary before running these.

use std::io::Read;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_termd");

struct Daemon {
    child: Child,
    _dir: tempfile::TempDir,
    socket: PathBuf,
}

impl Daemon {
    fn start() -> Daemon {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("termd.sock");
        let child = Command::new(BIN)
            .args(["start", "--socket", socket.to_str().unwrap(), "--listen", "127.0.0.1:0"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket.exists() {
            assert!(Instant::now() < deadline, "daemon socket never appeared");
            std::thread::sleep(Duration::from_millis(25));
        }
        // The socket can exist before the listener accepts; give it a beat.
        std::thread::sleep(Duration::from_millis(200));
        Daemon { child, _dir: dir, socket }
    }

    fn run(&self, args: &[&str]) -> String {
        let out = Command::new(BIN)
            .args(args)
            .args(["--socket", self.socket.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(out.status.success(), "termd {args:?} failed: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn create_pty(&self) -> String {
        let out = self.run(&["create", "--cols", "80", "--rows", "24", "--cmd", "sh"]);
        out.split_whitespace().next().expect("create printed no pty id").to_string()
    }

    fn send(&self, pty_id: &str, keys: &str) {
        self.run(&["send", pty_id, keys]);
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// An `attach` client running under a real pty. Reads/writes go through the
// pty master, exactly as a user's terminal would see them.
struct AttachClient {
    child: Child,
    master: std::fs::File,
}

impl AttachClient {
    fn spawn(daemon: &Daemon, pty_id: &str, cols: u16, rows: u16) -> AttachClient {
        let ws = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
        let mut master_fd: libc::c_int = -1;
        let mut slave_fd: libc::c_int = -1;
        let rc = unsafe {
            libc::openpty(&mut master_fd, &mut slave_fd, std::ptr::null_mut(), std::ptr::null(), &ws)
        };
        assert_eq!(rc, 0, "openpty failed");
        let master: OwnedFd = unsafe { std::os::fd::FromRawFd::from_raw_fd(master_fd) };
        let slave: OwnedFd = unsafe { std::os::fd::FromRawFd::from_raw_fd(slave_fd) };

        // Non-blocking master so drain() can poll without hanging.
        unsafe {
            let flags = libc::fcntl(master.as_raw_fd(), libc::F_GETFL);
            libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        let child = Command::new(BIN)
            .args(["attach", pty_id, "--socket", daemon.socket.to_str().unwrap()])
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave))
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        AttachClient { child, master: master.into() }
    }

    /// Collect everything the client writes to its terminal for `dur`.
    fn drain(&mut self, dur: Duration) -> Vec<u8> {
        let mut buf = Vec::new();
        let deadline = Instant::now() + dur;
        let mut chunk = [0u8; 65536];
        while Instant::now() < deadline {
            match self.master.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => std::thread::sleep(Duration::from_millis(25)),
            }
        }
        buf
    }

    /// Type bytes on the client's terminal (keystrokes).
    fn write(&mut self, bytes: &[u8]) {
        use std::io::Write;
        self.master.write_all(bytes).unwrap();
    }
}

impl Drop for AttachClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn rfind(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).rposition(|w| w == needle)
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    find(haystack, needle).is_some()
}

// Regression guard: dynamic colors (OSC 10/11/12) and window title leaking
// across PTYs. The refresh formatter emits OSC 10/11/12 / OSC 0 only when the
// target PTY has a non-default value, so without an explicit clear-to-baseline
// a cursor/background color or title set by an app in PTY A survived a switch
// to plain PTY B and persisted in the user's real terminal after detach. The
// 2J in the refresh preamble also ran while the stale OSC 11 background was
// still active, clearing the screen to the previous PTY's background color.
#[test]
fn attach_clears_colors_and_title_on_switch_and_exit() {
    let daemon = Daemon::start();
    let pty_a = daemon.create_pty();
    let _pty_b = daemon.create_pty();

    // PTY A recolors the cursor + background and sets a title; B stays plain.
    daemon.send(
        &pty_a,
        "printf '\\033]12;#ff8800\\007\\033]11;#112233\\007\\033]0;PTY-A-TITLE\\007'\r",
    );
    std::thread::sleep(Duration::from_millis(500));

    let mut client = AttachClient::spawn(&daemon, &pty_a, 80, 24);
    let attach = client.drain(Duration::from_millis(1500));
    client.write(b"\x011"); // C-a 1: switch to PTY B (0-indexed list)
    let switch = client.drain(Duration::from_millis(1500));
    client.write(b"\x01d"); // C-a d: detach
    let detach = client.drain(Duration::from_millis(1500));

    // Attach: the client saves the host title, and the refresh restores A's
    // colors and title.
    assert!(contains(&attach, b"\x1b[22;0t"), "missing title push at session start");
    assert!(contains(&attach, b"\x1b]12;rgb:ff/88/00"), "missing OSC 12 restore for PTY A");
    assert!(contains(&attach, b"\x1b]11;rgb:11/22/33"), "missing OSC 11 restore for PTY A");
    assert!(contains(&attach, b"\x1b]0;PTY-A-TITLE"), "missing title restore for PTY A");

    // Switch to plain B: everything A set must be cleared, and nothing restored.
    assert!(contains(&switch, b"\x1b]110\x1b\\"), "missing default-fg reset on switch");
    assert!(contains(&switch, b"\x1b]111\x1b\\"), "missing default-bg reset on switch");
    assert!(contains(&switch, b"\x1b]112\x1b\\"), "missing cursor-color reset on switch");
    assert!(contains(&switch, b"\x1b]0;\x1b\\"), "missing empty-title clear on switch");
    let bg_reset = find(&switch, b"\x1b]111\x1b\\").unwrap();
    let erase = rfind(&switch, b"\x1b[2J").expect("missing clear-screen on switch");
    assert!(bg_reset < erase, "default-bg reset must precede the refresh 2J");
    assert!(!contains(&switch, b"\x1b]12;rgb:"), "PTY A cursor color leaked into plain PTY B");
    assert!(!contains(&switch, b"\x1b]11;rgb:"), "PTY A background leaked into plain PTY B");
    assert!(!contains(&switch, b"\x1b]0;PTY-A-TITLE"), "PTY A title leaked into plain PTY B");

    // Detach: colors reset and the host terminal's title restored via the pop.
    assert!(contains(&detach, b"\x1b]111\x1b\\"), "missing default-bg reset at detach");
    assert!(contains(&detach, b"\x1b]112\x1b\\"), "missing cursor-color reset at detach");
    assert!(contains(&detach, b"\x1b[23;0t"), "missing title pop at session exit");
}
