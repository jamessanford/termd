use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    os::unix::io::{FromRawFd, IntoRawFd},
    sync::{Arc, Mutex, RwLock, atomic::{AtomicU32, AtomicU64, Ordering}},
    time::SystemTime,
};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use libghostty_vt::{Terminal, TerminalOptions, RenderState};
use libghostty_vt::render::{RowIterator, CellIterator};
use nix::{
    pty::openpty,
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tokio::sync::{broadcast, oneshot};

const TERM_NAME: &str = "xterm-ghostty";
// TODO: expose from libghostty_vt::build_info when available

pub struct PtyInfo {
    pub id: String,
    pub hostname: String,
    pub pts_name: String,
    pub cols: u32,
    pub rows: u32,
    pub title: String,
    pub created_at: SystemTime,
}

pub struct PtyChunk {
    pub generation: u64,
    pub data: Bytes,
}

pub struct RefreshData {
    pub generation: u64,
    pub data: Bytes,
    pub cursor_x: u32,
    pub cursor_y: u32,
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
    refresh_tx: std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    child_pid: u32,
    child: Mutex<Option<std::process::Child>>,
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
        Ok(())
    }

    pub fn set_title(&self, title: &str) {
        *self.title.lock().unwrap() = title.to_string();
    }

    pub async fn refresh(&self) -> Result<RefreshData> {
        let (tx, rx) = oneshot::channel();
        self.refresh_tx.send(tx).map_err(|_| anyhow!("PTY reader thread is dead"))?;
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped refresh response"))?
    }

    pub fn id(&self) -> &str {
        &self.id
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

        // Allocate PTY
        let pty = openpty(None, None).context("openpty")?;
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

        let child = cmd.spawn().context("spawn shell")?;
        // slave fds are owned by the Stdio objects passed to Command and closed after fork;

        let (tx, _) = broadcast::channel::<Arc<PtyChunk>>(512);
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let generation = Arc::new(AtomicU64::new(0));

        let child_pid = child.id();
        let handle = Arc::new(PtyHandle {
            id: id.clone(),
            pts_name,
            created_at: SystemTime::now(),
            hostname,
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            title: title.clone(),
            tx: tx.clone(),
            writer: Mutex::new(unsafe { File::from_raw_fd(master_fd) }),
            refresh_tx,
            child_pid,
            child: Mutex::new(Some(child)),
        });

        // Spawn dedicated reader thread — owns all libghostty state
        let master_reader = unsafe { File::from_raw_fd(master_reader_fd) };
        let title_for_thread = title.clone();
        std::thread::Builder::new()
            .name(format!("pty-reader-{id}"))
            .spawn(move || reader_thread(master_reader, tx, generation, refresh_rx, title_for_thread, cols, rows))
            .context("spawn reader thread")?;

        self.ptys.write().unwrap().insert(id, handle.clone());
        Ok(handle)
    }

    pub fn destroy(&self, id: &str) -> Result<()> {
        let handle = self.ptys.write().unwrap().remove(id)
            .ok_or_else(|| anyhow!("PTY {id} not found"))?;
        let _ = kill(Pid::from_raw(handle.child_pid as i32), Signal::SIGHUP);
        if let Some(mut child) = handle.child.lock().unwrap().take() {
            std::thread::spawn(move || { let _ = child.wait(); });
        }
        Ok(())
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

fn do_refresh(
    terminal: &mut Terminal<'static, 'static>,
    render_state: &mut RenderState<'static>,
    row_iter_obj: &mut RowIterator<'static>,
    cell_iter_obj: &mut CellIterator<'static>,
    generation: u64,
) -> Result<RefreshData> {
    let cursor_x = terminal.cursor_x().unwrap_or(0) as u32;
    let cursor_y = terminal.cursor_y().unwrap_or(0) as u32;

    let snapshot = render_state.update(terminal)?;
    let mut row_iter = row_iter_obj.update(&snapshot)?;

    let mut out = Vec::new();
    let mut enc = [0u8; 4];
    while let Some(row) = row_iter.next() {
        let mut cell_iter = cell_iter_obj.update(row)?;
        while let Some(cell) = cell_iter.next() {
            let graphemes = cell.graphemes()?;
            if graphemes.is_empty() {
                out.push(b' ');
                continue;
            }
            for ch in &graphemes {
                out.extend_from_slice(ch.encode_utf8(&mut enc).as_bytes());
            }
        }
        out.push(b'\n');
    }

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
    title: Arc<Mutex<String>>,
    init_cols: u32,
    init_rows: u32,
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

    let mut render_state = match RenderState::new() {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!("PTY reader: failed to create render state: {e}");
            return;
        }
    };
    let mut row_iter_obj = match RowIterator::new() {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!("PTY reader: failed to create row iterator: {e}");
            return;
        }
    };
    let mut cell_iter_obj = match CellIterator::new() {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!("PTY reader: failed to create cell iterator: {e}");
            return;
        }
    };

    use std::os::unix::io::AsRawFd;
    let master_fd = master.as_raw_fd();

    let mut buf = [0u8; 4096];
    loop {
        // Drain any pending refresh requests before waiting for PTY data
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_refresh(&mut terminal, &mut render_state, &mut row_iter_obj, &mut cell_iter_obj, gen);
            let _ = reply_tx.send(result);
        }

        // Use poll() with a 50ms timeout so we can service refresh requests
        // even when the PTY is idle (no new output).
        let mut pfd = libc::pollfd {
            fd: master_fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let poll_ret = unsafe { libc::poll(&mut pfd as *mut libc::pollfd, 1, 50) };

        if poll_ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR — retry
            }
            tracing::debug!("PTY reader poll error: {err}");
            break;
        }

        if poll_ret == 0 {
            // Timeout — no data yet; loop back to drain refresh requests
            continue;
        }

        // Data available (or HUP/ERR)
        let n = match master.read(&mut buf) {
            Ok(0) => {
                // EOF — shell exited. Stay alive for refresh requests until refresh_rx closes.
                tracing::debug!("PTY reader: EOF on master fd");
                loop {
                    match refresh_rx.recv() {
                        Ok(reply_tx) => {
                            let gen = generation.load(Ordering::Relaxed);
                            let result = do_refresh(&mut terminal, &mut render_state, &mut row_iter_obj, &mut cell_iter_obj, gen);
                            let _ = reply_tx.send(result);
                        }
                        Err(_) => return, // PtyHandle dropped (destroyed)
                    }
                }
            }
            Err(e) => {
                tracing::debug!("PTY reader EOF: {e}");
                // EOF/error — shell exited. Stay alive for refresh requests until refresh_rx closes.
                loop {
                    match refresh_rx.recv() {
                        Ok(reply_tx) => {
                            let gen = generation.load(Ordering::Relaxed);
                            let result = do_refresh(&mut terminal, &mut render_state, &mut row_iter_obj, &mut cell_iter_obj, gen);
                            let _ = reply_tx.send(result);
                        }
                        Err(_) => return, // PtyHandle dropped (destroyed)
                    }
                }
            }
            Ok(n) => n,
        };

        terminal.vt_write(&buf[..n]);
        let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
        let chunk = Arc::new(PtyChunk {
            generation: gen,
            data: Bytes::copy_from_slice(&buf[..n]),
        });
        let _ = tx.send(chunk); // ignore SendError (no subscribers is fine)
    }
}
