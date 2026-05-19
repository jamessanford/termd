use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    os::unix::io::{AsRawFd, FromRawFd, IntoRawFd},
    os::fd::OwnedFd,
    sync::{Arc, Mutex, RwLock, atomic::{AtomicU32, AtomicU64, Ordering}},
    time::SystemTime,
};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use libghostty_vt::{Terminal, TerminalOptions, ffi};
use libghostty_vt::fmt::{Format, Formatter, FormatterOptions};
use libghostty_vt::screen::{Screen, Selection};
use libghostty_vt::terminal::{Point, PointCoordinate};
use nix::{
    pty::openpty,
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tokio::sync::{broadcast, oneshot};

const TERM_NAME: &str = "xterm-ghostty";
// TODO: expose from libghostty_vt::build_info when available

#[derive(Clone, Debug)]
pub struct PtyInfo {
    pub id: String,
    pub hostname: String,
    pub pts_name: String,
    pub cols: u32,
    pub rows: u32,
    pub title: String,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PtyChunkKind {
    /// Raw bytes from the PTY master — incremental update.
    Stream,
    /// Full screen repaint (formatter output) — treat as a refresh snapshot.
    Repaint,
}

#[derive(Debug)]
pub struct PtyChunk {
    pub generation: u64,
    pub data: Bytes,
    pub kind: PtyChunkKind,
}

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
    pub generation: u64,
    pub info: PtyInfo,
}

#[derive(Debug)]
pub enum PtyEvent {
    Data(Arc<PtyChunk>),
    Metadata(Arc<PtyMetadata>),
}

pub struct RefreshData {
    pub generation: u64,
    pub data: Bytes,
    pub cursor_x: u32,
    pub cursor_y: u32,
}

// pty_id is not included — callers supply it directly from the request (see RefreshData).
pub struct ScrollbackData {
    pub generation: u64,
    pub data: Bytes,
    pub total_scrollback_rows: u32,
}

pub struct PtyHandle {
    id: String,
    pts_name: String,
    created_at: SystemTime,
    hostname: String,
    cols: AtomicU32,
    rows: AtomicU32,
    title: Arc<Mutex<String>>,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    writer: Mutex<File>,
    refresh_tx:    std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    scrollback_tx: std::sync::mpsc::SyncSender<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>,
    resize_tx:     std::sync::mpsc::SyncSender<(u32, u32)>,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    generation: Arc<AtomicU64>,
    child_pid: Pid,
    wakeup_write: OwnedFd,
}

impl PtyHandle {
    pub fn info(&self) -> PtyInfo {
        PtyInfo {
            id: self.id.clone(),
            hostname: self.hostname.clone(),
            pts_name: self.pts_name.clone(),
            cols: self.cols.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            title: self.title.lock().unwrap().clone(),
            created_at: self.created_at,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<PtyChunk>> {
        self.tx.subscribe()
    }

    pub fn meta_subscribe(&self) -> broadcast::Receiver<Arc<PtyMetadata>> {
        self.meta_tx.subscribe()
    }

    pub fn broadcast_metadata(&self, meta: Arc<PtyMetadata>) {
        let _ = self.meta_tx.send(meta);
    }

    pub fn write(&self, data: &[u8]) -> Result<()> {
        self.writer.lock().unwrap().write_all(data).context("write to PTY")
    }

    pub fn resize(&self, cols: u32, rows: u32) -> Result<()> {
        use nix::pty::Winsize;
        use nix::libc;
        let ws = Winsize {
            ws_col: cols as u16,
            ws_row: rows as u16,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let fd = {
            use std::os::unix::io::AsRawFd;
            self.writer.lock().unwrap().as_raw_fd()
        };
        let ret = unsafe { libc::ioctl(fd, libc::TIOCSWINSZ, &ws as *const Winsize) };
        if ret < 0 {
            return Err(anyhow!("TIOCSWINSZ failed: {}", std::io::Error::last_os_error()));
        }
        self.cols.store(cols, Ordering::Relaxed);
        self.rows.store(rows, Ordering::Relaxed);
        // Notify reader thread so libghostty Terminal dimensions stay in sync.
        // Best-effort: if the channel is full or the thread is gone, skip silently.
        let _ = self.resize_tx.try_send((cols, rows));
        let wfd = self.wakeup_write.as_raw_fd();
        let _ = unsafe { libc::write(wfd, [2u8].as_ptr() as *const libc::c_void, 1) };
        // Broadcast updated state to all subscribers
        let _ = self.meta_tx.send(Arc::new(PtyMetadata {
            reason: MetadataReason::Resize,
            exit_code: None,
            generation: self.generation.load(Ordering::Relaxed),
            info: self.info(),
        }));
        Ok(())
    }

    pub fn set_title(&self, title: &str) {
        *self.title.lock().unwrap() = title.to_string();
    }

    pub async fn refresh(&self) -> Result<RefreshData> {
        let (tx, rx) = oneshot::channel();
        self.refresh_tx.send(tx).map_err(|_| anyhow!("PTY reader thread is dead"))?;
        // Wake the reader immediately so it handles the refresh before the next PTY event.
        let wfd = self.wakeup_write.as_raw_fd();
        let ret = unsafe { libc::write(wfd, [1u8].as_ptr() as *const libc::c_void, 1) };
        if ret < 0 {
            tracing::debug!("wakeup write failed: {}", std::io::Error::last_os_error());
        }
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped refresh response"))?
    }

    pub async fn scrollback(&self, row_offset: u32, row_count: u32) -> Result<ScrollbackData> {
        let (tx, rx) = oneshot::channel();
        self.scrollback_tx.send((row_offset, row_count, tx))
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        let wfd = self.wakeup_write.as_raw_fd();
        let ret = unsafe { libc::write(wfd, [1u8].as_ptr() as *const libc::c_void, 1) };
        if ret < 0 {
            tracing::debug!("wakeup write failed: {}", std::io::Error::last_os_error());
        }
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped scrollback response"))?
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

pub struct PtyRegistry {
    ptys: RwLock<HashMap<String, Arc<PtyHandle>>>,
}

impl PtyRegistry {
    pub fn new() -> Self {
        Self { ptys: RwLock::new(HashMap::new()) }
    }

    pub fn create(&self, cols: u32, rows: u32, command: Option<&str>) -> Result<Arc<PtyHandle>> {
        let id = uuid::Uuid::new_v4().to_string();
        let hostname = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();

        // Allocate PTY with the requested dimensions so the kernel PTY has the
        // correct window size before the child shell starts and calls TIOCGWINSZ.
        let init_winsize = nix::pty::Winsize {
            ws_col: cols as u16,
            ws_row: rows as u16,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let pty = openpty(Some(&init_winsize), None).context("openpty")?;
        let master_fd = pty.master.into_raw_fd();
        let slave_fd = pty.slave.into_raw_fd();

        // Get slave device name (/dev/pts/N) using thread-safe ptsname_r
        let pts_name = unsafe {
            let borrowed = std::os::unix::io::BorrowedFd::borrow_raw(master_fd);
            let owned = borrowed.try_clone_to_owned().context("clone master fd for ptsname_r")?;
            nix::pty::ptsname_r(&nix::pty::PtyMaster::from_owned_fd(owned))
        }.unwrap_or_else(|_| String::from("unknown"));

        // Set close-on-exec on master fd so child doesn't inherit it
        let rc = unsafe { libc::fcntl(master_fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        if rc < 0 {
            return Err(std::io::Error::last_os_error()).context("set FD_CLOEXEC on master fd");
        }

        // Spawn child shell
        let shell = command.map(String::from).unwrap_or_else(|| {
            std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
        });
        let title = Arc::new(Mutex::new(pts_name.clone()));

        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};
        // dup slave_fd so each Stdio owns its own fd; the child will have them as 0/1/2
        let slave_stdout = unsafe { libc::dup(slave_fd) };
        let slave_stderr = unsafe { libc::dup(slave_fd) };
        if slave_stdout < 0 || slave_stderr < 0 {
            return Err(std::io::Error::last_os_error()).context("dup slave fd");
        }
        // Set FD_CLOEXEC on all slave fds so concurrent forks (e.g., tests spawning
        // other PTYs) don't inherit them.  Rust's spawn dup2s them to 0/1/2 in the
        // child before exec, so the shell still gets them correctly.
        for &fd in &[slave_fd, slave_stdout, slave_stderr] {
            let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
            if rc < 0 {
                return Err(std::io::Error::last_os_error()).context("set FD_CLOEXEC on slave fd");
            }
        }
        let mut cmd = Command::new(&shell);
        cmd.env("TERM", TERM_NAME)
           .env_remove("TERM_PROGRAM")
           .stdin(unsafe { Stdio::from_raw_fd(slave_fd) })
           .stdout(unsafe { Stdio::from_raw_fd(slave_stdout) })
           .stderr(unsafe { Stdio::from_raw_fd(slave_stderr) });
        // pre_exec runs in child after fork, before exec.
        // At this point stdin/stdout/stderr are already the slave fd.
        unsafe {
            cmd.pre_exec(move || {
                // New session — detach from parent's controlling terminal
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // fd 0 is already the slave; set it as controlling terminal
                if libc::ioctl(0, libc::TIOCSCTTY, 0i32) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // TODO: systemd-logind session registration (Linux)
                Ok(())
            });
        }

        // Dup master_fd for the reader thread before File::from_raw_fd takes ownership
        let master_reader_fd = unsafe { libc::dup(master_fd) };
        if master_reader_fd < 0 {
            return Err(std::io::Error::last_os_error()).context("dup master fd for reader");
        }
        // Set O_NONBLOCK so the reader can drain all available bytes in a loop
        let flags = unsafe { libc::fcntl(master_reader_fd, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(master_reader_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error()).context("set O_NONBLOCK on master reader fd");
        }

        let (tx, _) = broadcast::channel::<Arc<PtyChunk>>(512);
        let (meta_tx, _) = broadcast::channel::<Arc<PtyMetadata>>(64);
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let (scrollback_tx, scrollback_rx) =
            std::sync::mpsc::sync_channel::<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>(8);
        let (resize_tx, resize_rx) = std::sync::mpsc::sync_channel::<(u32, u32)>(8);
        let generation = Arc::new(AtomicU64::new(0));

        // Create wakeup pipe before spawning the child so that a pipe2 failure doesn't
        // leave a zombie process behind.  O_CLOEXEC ensures the child won't inherit these fds.
        // O_NONBLOCK is required so that the unconditional drain-read at the top of the
        // reader loop returns EAGAIN immediately when the pipe is empty.
        let mut pipe_fds = [0i32; 2];
        let rc = unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if rc < 0 {
            return Err(std::io::Error::last_os_error()).context("pipe2 for wakeup");
        }
        let wakeup_read = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        let wakeup_write = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };

        let child = cmd.spawn().context("spawn shell")?;
        // slave fds are owned by the Stdio objects passed to Command and closed after fork;

        let child_pid = Pid::from_raw(child.id() as i32);
        crate::utmp::add_record(master_reader_fd, &hostname);
        let created_at = SystemTime::now();
        let meta_tx_for_thread = meta_tx.clone();
        let id_for_thread = id.clone();
        let hostname_for_thread = hostname.clone();
        let pts_name_for_thread = pts_name.clone();
        let handle = Arc::new(PtyHandle {
            id: id.clone(),
            pts_name,
            created_at,
            hostname,
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            title: title.clone(),
            tx: tx.clone(),
            writer: Mutex::new(unsafe { File::from_raw_fd(master_fd) }),
            refresh_tx,
            scrollback_tx,
            resize_tx,
            meta_tx: meta_tx.clone(),
            generation: generation.clone(),
            child_pid,
            wakeup_write,
        });

        // Spawn dedicated reader thread — owns libghostty state and child process
        let master_reader = unsafe { File::from_raw_fd(master_reader_fd) };
        let title_for_thread = title.clone();
        std::thread::Builder::new()
            .name(format!("pty-reader-{id}"))
            .spawn(move || reader_thread(
                master_reader, tx, generation, refresh_rx, scrollback_rx, resize_rx, wakeup_read,
                child, title_for_thread, cols, rows,
                meta_tx_for_thread, id_for_thread, hostname_for_thread,
                pts_name_for_thread, created_at,
            ))
            .context("spawn reader thread")?;

        self.ptys.write().unwrap().insert(id, handle.clone());
        Ok(handle)
    }

    pub fn destroy(&self, id: &str) -> Result<()> {
        let handle = self.ptys.write().unwrap().remove(id)
            .ok_or_else(|| anyhow!("PTY {id} not found"))?;
        let _ = kill(handle.child_pid, Signal::SIGHUP);
        // handle drops at end of scope: wakeup_write closes → reader sees POLLHUP and exits.
        // If callers hold Arc<PtyHandle> clones (e.g. an in-flight refresh), wakeup_write
        // stays open until the last clone drops — POLLHUP fires then, not immediately on return.
        Ok(())
    }

    pub fn destroy_all(&self) {
        let ids: Vec<String> = self.ptys.read().unwrap().keys().cloned().collect();
        for id in ids {
            let _ = self.destroy(&id);
        }
    }

    pub fn get(&self, id: &str) -> Option<Arc<PtyHandle>> {
        self.ptys.read().unwrap().get(id).cloned()
    }

    pub fn list(&self) -> Vec<Arc<PtyHandle>> {
        self.ptys.read().unwrap().values().cloned().collect()
    }
}

impl Default for PtyRegistry {
    fn default() -> Self { Self::new() }
}

// Note: on-demand refresh only renders the active screen at call time.  The screen-switch
// broadcast in reader_thread (screen_changed path) mitigates the primary↔alternate gap by
// pushing a full render of the new screen to all subscribers immediately after the switch.
fn do_refresh(
    terminal: &Terminal<'static, 'static>,
    cols: u32,
    rows: u32,
    generation: u64,
) -> Result<RefreshData> {
    let cursor_x = terminal.cursor_x().unwrap_or(0) as u32;
    let cursor_y = terminal.cursor_y().unwrap_or(0) as u32;

    // Restrict to the active screen — the server terminal has scrollback and we don't want it.
    // Point::Active resolves within the visible grid only, ignoring history rows.
    let top_left = terminal.grid_ref(Point::Active(PointCoordinate { x: 0, y: 0 }))?;
    let bottom_right = terminal.grid_ref(Point::Active(PointCoordinate {
        x: cols.saturating_sub(1) as u16,
        y: rows.saturating_sub(1),
    }))?;
    let selection = Selection { start: top_left, end: bottom_right, rectangle: false };

    let extra = ffi::FormatterTerminalExtra {
        size: std::mem::size_of::<ffi::FormatterTerminalExtra>(),
        scrolling_region: true, // restore server app's DECSTBM/DECSLRM state
        modes: true,            // restore terminal modes (mouse tracking, cursor visibility, etc.)
        palette: false,         // don't override the host terminal's color palette
        tabstops: false,        // tabstop restoration moves cursor, corrupting final position
        pwd: false,
        keyboard: false,
        screen: ffi::FormatterScreenExtra {
            size: std::mem::size_of::<ffi::FormatterScreenExtra>(),
            cursor: true, // emit final cursor position at end of output
            style: false,
            hyperlink: false,
            protection: false,
            kitty_keyboard: false,
            charsets: false,
        },
    };

    let mut fmt = Formatter::new(terminal, FormatterOptions {
        format: Format::Vt,
        trim: false,
        unwrap: false,
        selection: Some(selection),
        extra,
    })?;

    let mut out: Vec<u8> = Vec::new();
    // Soft reset (DECSTR) + clear screen + cursor home.
    // DECSTR resets cursor visibility to the default (visible); modes:true in the formatter
    // then re-emits ?25l if the server app has the cursor hidden.  No need to hide here —
    // the formatter output is sent as one blob so there is no per-character cursor flicker.
    out.extend_from_slice(b"\x1b[!p\x1b[2J\x1b[H");
    let vt = fmt.format_alloc(None)?;
    out.extend_from_slice(&vt);
    out.extend_from_slice(b"\x1b[0m"); // trailing SGR reset

    Ok(RefreshData {
        generation,
        data: Bytes::from(out),
        cursor_x,
        cursor_y,
    })
}

// Reader thread — owns libghostty Terminal state (not Send + Sync).
fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    generation: Arc<AtomicU64>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
    _scrollback_rx: std::sync::mpsc::Receiver<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>,
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
    let mut terminal = match Terminal::new(TerminalOptions {
        cols: init_cols as u16,
        rows: init_rows as u16,
        max_scrollback: 10_000,
    }) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!("PTY reader: failed to create terminal: {e}");
            return;
        }
    };

    let title_cb = title.clone();
    if let Err(e) = terminal.on_title_changed(move |term| {
        if let Ok(t) = term.title() {
            *title_cb.lock().unwrap() = t.to_string();
        }
    }) {
        tracing::debug!("PTY reader: failed to register title callback: {e}");
        return;
    }

    let mut current_cols = init_cols;
    let mut current_rows = init_rows;
    // Initialize prev_title to match the initial title mutex value (pts_name),
    // so we don't emit a spurious TitleChanged before the shell sets any title.
    let mut prev_title = pts_name.clone();
    let mut prev_screen = Screen::Primary;

    // master_fd is only valid as long as master (the owning File) is alive
    let master_fd = master.as_raw_fd();
    let wakeup_fd = wakeup_read.as_raw_fd();

    let mut buf = [0u8; 4096];
    'main: loop {
        // Drain the wakeup pipe, then handle any pending resize and refresh requests
        // before waiting for PTY data.
        let mut wake_byte = [0u8; 64];
        unsafe { libc::read(wakeup_fd, wake_byte.as_mut_ptr() as *mut libc::c_void, wake_byte.len()) };
        while let Ok((cols, rows)) = resize_rx.try_recv() {
            current_cols = cols;
            current_rows = rows;
            if let Err(e) = terminal.resize(cols as u16, rows as u16, 0, 0) {
                tracing::debug!("PTY reader: terminal resize failed: {e}");
            } else {
                let refresh_gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
                match do_refresh(&terminal, current_cols, current_rows, refresh_gen) {
                    Ok(data) => { let _ = tx.send(Arc::new(PtyChunk { generation: refresh_gen, data: data.data, kind: PtyChunkKind::Repaint })); }
                    Err(e) => tracing::debug!("PTY reader: resize refresh failed: {e}"),
                }
            }
        }
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_refresh(&terminal, current_cols, current_rows, gen);
            let _ = reply_tx.send(result);
        }

        // Poll both the PTY master and the wakeup pipe.  Writes to wakeup_write
        // (refresh or resize) unblock the poll so requests are handled promptly.
        let mut pfds = [
            libc::pollfd { fd: master_fd, events: libc::POLLIN, revents: 0 },
            libc::pollfd { fd: wakeup_fd, events: libc::POLLIN, revents: 0 },
        ];
        let poll_ret = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, -1) };

        if poll_ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR — retry
            }
            tracing::debug!("PTY reader poll error: {err}");
            break;
        }

        // Check for POLLHUP on the wakeup pipe — means the write end was closed
        // (PtyHandle dropped / destroy() called).  Exit the reader loop.
        if pfds[1].revents & libc::POLLHUP != 0 {
            tracing::debug!("PTY reader: wakeup pipe closed, exiting");
            break;
        }

        // Only read from PTY master if it actually has data ready.
        // poll_ret > 0 may be due to the wakeup pipe alone; reading master
        // when it is not ready would block indefinitely.
        if pfds[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
            // Only the wakeup pipe fired — loop back to handle refresh requests.
            continue;
        }

        // Data available (or HUP/ERR) on PTY master — drain all buffered bytes.
        let mut batch: Vec<u8> = Vec::new();
        loop {
            match master.read(&mut buf) {
                Ok(0) => {
                    tracing::debug!("PTY reader: EOF on master fd");
                    break 'main;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    tracing::debug!("PTY reader error: {e}");
                    break 'main;
                }
                Ok(n) => batch.extend_from_slice(&buf[..n]),
            }
        }

        if batch.is_empty() {
            continue;
        }

        terminal.vt_write(&batch);
        let current_screen = terminal.active_screen().unwrap_or(Screen::Primary);
        let screen_changed = current_screen != prev_screen;
        if screen_changed {
            prev_screen = current_screen;
        }
        let current_title = title.lock().unwrap().clone();
        let title_changed = current_title != prev_title;
        if title_changed {
            prev_title = current_title.clone();
        }
        let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
        let chunk = Arc::new(PtyChunk {
            generation: gen,
            data: Bytes::from(batch),
            kind: PtyChunkKind::Stream,
        });
        let _ = tx.send(chunk); // ignore SendError (no subscribers is fine)
        // Emit TitleChanged after fetch_add so generation matches the accompanying chunk.
        if title_changed {
            let _ = meta_tx.send(Arc::new(PtyMetadata {
                reason: MetadataReason::TitleChanged,
                exit_code: None,
                generation: gen,
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
        // On screen switch (primary ↔ alternate), broadcast a full render of the new screen
        // so subscribers that hadn't seen it get the correct content immediately.
        if screen_changed {
            let refresh_gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
            match do_refresh(&terminal, current_cols, current_rows, refresh_gen) {
                Ok(data) => { let _ = tx.send(Arc::new(PtyChunk { generation: refresh_gen, data: data.data, kind: PtyChunkKind::Repaint })); }
                Err(e) => tracing::debug!("PTY reader: screen-switch refresh failed: {e}"),
            }
        }
    }

    // Reap child and broadcast exit notification
    let status = child.try_wait().ok().flatten().or_else(|| child.wait().ok());
    crate::utmp::remove_record(master.as_raw_fd());
    let exit_msg = {
        let title = title.lock().unwrap().clone();
        match status {
            Some(s) => {
                if let Some(code) = s.code() {
                    format!("\r\n[Command {} exited with code {}]\r\n", title, code)
                } else {
                    format!("\r\n[Command {} was killed]\r\n", title)
                }
            }
            None => format!("\r\n[Command {} terminated]\r\n", title),
        }
    };
    let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
    let _ = tx.send(Arc::new(PtyChunk {
        generation: gen,
        data: Bytes::from(exit_msg.into_bytes()),
        kind: PtyChunkKind::Stream,
    }));
    let exit_code = status.as_ref().and_then(|s| s.code());
    let _ = meta_tx.send(Arc::new(PtyMetadata {
        reason: MetadataReason::Closed,
        exit_code,
        generation: generation.load(Ordering::Relaxed),
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

    // Drain any refresh requests that arrived just before exit
    while let Ok(reply_tx) = refresh_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_refresh(&terminal, current_cols, current_rows, gen);
        let _ = reply_tx.send(result);
    }

    drop(wakeup_read); // closes read end; wakeup_write already closed (PtyHandle dropped)
}
