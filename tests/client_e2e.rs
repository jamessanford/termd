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

    /// Collect the client's terminal output until `marker` has been seen, then
    /// keep reading for a short settle window so trailing bytes of the same
    /// burst (which negative assertions depend on) are captured too. Panics if
    /// the marker never shows up within the timeout.
    fn drain_until(&mut self, marker: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        let timeout = Instant::now() + Duration::from_secs(5);
        let mut deadline: Option<Instant> = None;
        let mut chunk = [0u8; 65536];
        loop {
            match self.master.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => std::thread::sleep(Duration::from_millis(10)),
            }
            if deadline.is_none() && find(&buf, marker).is_some() {
                deadline = Some(Instant::now() + Duration::from_millis(250));
            }
            match deadline {
                Some(d) if Instant::now() >= d => break,
                None if Instant::now() >= timeout => {
                    panic!("marker {marker:?} never arrived; got {} bytes: {buf:?}", buf.len())
                }
                _ => {}
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
    // The OSC 12 restore is the last of the color restores in the refresh blob.
    let attach = client.drain_until(b"\x1b]12;rgb:ff/88/00");
    client.write(b"\x011"); // C-a 1: switch to PTY B (0-indexed list)
    // DECSTR appears only in the refresh preamble, so seeing it means B's
    // refresh (one write) has started arriving; the settle catches the rest.
    let switch = client.drain_until(b"\x1b[!p");
    client.write(b"\x01d"); // C-a d: detach
    let detach = client.drain_until(b"\x1b[23;0t");

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
    assert!(contains(&switch, b"\x1b[?2026l"), "missing synchronized-output disable on switch");
    assert!(contains(&switch, b"\x1b]8;;\x1b\\"), "missing hyperlink close on switch");
    assert!(contains(&switch, b"\x1b]104\x1b\\"), "missing palette reset on switch");
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

// When the viewed PTY exits, the client goes to the most recently viewed
// survivor instead of the picker. Visit A -> C -> B and exit B: the MRU says C
// (list order would say A or C; the old code showed the picker). Each PTY sets a
// distinct title, which its refresh restores, so the title identifies the PTY.
#[test]
fn attach_exit_switches_to_most_recent_pty() {
    let daemon = Daemon::start();
    let ptys: Vec<String> = (0..3).map(|_| daemon.create_pty()).collect();
    for (pty, title) in ptys.iter().zip(["MRU-A", "MRU-B", "MRU-C"]) {
        daemon.send(pty, &format!("printf '\\033]0;{title}\\007'\r"));
    }
    std::thread::sleep(Duration::from_millis(500));

    let mut client = AttachClient::spawn(&daemon, &ptys[0], 80, 24);
    client.drain_until(b"\x1b]0;MRU-A");
    client.write(b"\x012"); // C-a 2: C
    client.drain_until(b"\x1b]0;MRU-C");
    client.write(b"\x011"); // C-a 1: B
    client.drain_until(b"\x1b]0;MRU-B");

    daemon.send(&ptys[1], "exit\r");
    let after = client.drain_until(b"\x1b]0;MRU-C");
    assert!(!contains(&after, b"\x1b]0;MRU-A"), "exit went to A, not the most recent PTY");
}

// C-a o marks the current PTY keep_on_exit: when its command exits, it stays
// on screen showing how it ended, instead of switching away. C-a k then destroys
// it and moves to the most recent survivor. A kept PTY also survives exiting
// while unwatched, and clearing the flag (C-a o again) on a dead one reaps it
// and moves on.
#[test]
fn attach_keep_flag_holds_dead_pty_until_destroyed() {
    let daemon = Daemon::start();
    let ptys: Vec<String> = (0..3).map(|_| daemon.create_pty()).collect();
    for (pty, title) in ptys.iter().zip(["KEEP-A", "KEEP-B", "KEEP-C"]) {
        daemon.send(pty, &format!("printf '\\033]0;{title}\\007'\r"));
    }
    std::thread::sleep(Duration::from_millis(500));

    let mut client = AttachClient::spawn(&daemon, &ptys[0], 80, 24);
    client.drain_until(b"\x1b]0;KEEP-A");
    // Mark C kept (the banner goes to stderr, uncaptured; C repaints after it).
    client.write(b"\x012");
    client.drain_until(b"\x1b]0;KEEP-C");
    client.write(b"\x01o");
    client.drain_until(b"\x1b]0;KEEP-C");
    // Mark B kept.
    client.write(b"\x011");
    client.drain_until(b"\x1b]0;KEEP-B");
    client.write(b"\x01o");
    client.drain_until(b"\x1b]0;KEEP-B");

    daemon.send(&ptys[1], "exit 7\r");
    let after = client.drain_until(b"exited with code 7");
    assert!(!contains(&after, b"\x1b]0;KEEP-"), "kept PTY switched away on exit");

    // The dead screen survives a repaint (C-a R), exit banner included.
    client.write(b"\x01R");
    client.drain_until(b"exited with code 7");

    client.write(b"\x01k"); // C-a k: destroy B -> most recent survivor (C)
    client.drain_until(b"\x1b]0;KEEP-C");
    client.write(b"\x010"); // back to A
    client.drain_until(b"\x1b]0;KEEP-A");

    // C exits while unwatched; kept, so it's still listed and viewable.
    daemon.send(&ptys[2], "exit\r");
    std::thread::sleep(Duration::from_millis(300));
    assert!(daemon.run(&["list"]).contains(&ptys[2]), "kept PTY reaped on unwatched exit");
    client.write(b"\x011"); // list is now [A, C]
    client.drain_until(b"exited with code 0");
    client.write(b"\x01o"); // clear keep on dead C -> reaped, back to A
    client.drain_until(b"\x1b]0;KEEP-A");
    let listed = daemon.run(&["list"]);
    assert!(!listed.contains(&ptys[2]), "dead PTY not reaped after clearing keep: {listed}");
}

// A bare ESC typed on the client (not a CSI-u encoded one) reaches the PTY on
// its own, without waiting for another key: the client holds it only briefly
// in case it starts an escape sequence. `cat -v` in non-canonical mode shows
// it as ^[ the moment it arrives.
#[test]
fn attach_bare_esc_passes_through_without_next_key() {
    let daemon = Daemon::start();
    let pty = daemon.create_pty();
    daemon.send(&pty, "stty -icanon -echo; printf 'CAT-READY\\n'; cat -v\r");
    std::thread::sleep(Duration::from_millis(500));

    let mut client = AttachClient::spawn(&daemon, &pty, 80, 24);
    client.drain_until(b"CAT-READY");
    client.write(&[0x1b]);
    client.drain_until(b"^[");
}
