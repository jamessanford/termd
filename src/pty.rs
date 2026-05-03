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
    generation: Arc<AtomicU64>,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    writer: Mutex<File>,
    refresh_tx: std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    child_pid: u32,
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
            return Err(anyhow!("TIOCSWINSZ failed"));
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

        // Get slave device name (/dev/pts/N)
        let pts_name = unsafe {
            let name = libc::ptsname(master_fd);
            if name.is_null() { String::from("unknown") }
            else { std::ffi::CStr::from_ptr(name).to_string_lossy().into_owned() }
        };

        // Set close-on-exec on master fd so child doesn't inherit it
        unsafe { libc::fcntl(master_fd, libc::F_SETFD, libc::FD_CLOEXEC) };

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
        let child = cmd.spawn().context("spawn shell")?;
        // slave fds are owned by the Stdio objects passed to Command and closed after fork;

        let (tx, _) = broadcast::channel::<Arc<PtyChunk>>(512);
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let generation = Arc::new(AtomicU64::new(0));

        let handle = Arc::new(PtyHandle {
            id: id.clone(),
            pts_name,
            created_at: SystemTime::now(),
            hostname,
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            title: title.clone(),
            generation: generation.clone(),
            tx: tx.clone(),
            writer: Mutex::new(unsafe { File::from_raw_fd(master_fd) }),
            refresh_tx,
            child_pid: child.id(),
        });

        // Spawn dedicated reader thread — owns all libghostty state (wired in Task 3)
        let master_reader = unsafe { File::from_raw_fd(libc::dup(master_fd)) };
        std::thread::Builder::new()
            .name(format!("pty-reader-{id}"))
            .spawn(move || reader_thread(master_reader, tx, generation, refresh_rx))
            .context("spawn reader thread")?;

        self.ptys.write().unwrap().insert(id, handle.clone());
        Ok(handle)
    }

    pub fn destroy(&self, id: &str) -> Result<()> {
        let handle = self.ptys.write().unwrap().remove(id)
            .ok_or_else(|| anyhow!("PTY {id} not found"))?;
        let _ = kill(Pid::from_raw(handle.child_pid as i32), Signal::SIGHUP);
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

// Reader thread — owns libghostty Terminal (wired in Task 3).
// For now: read raw bytes, broadcast, handle refresh requests (stub).
fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<Arc<PtyChunk>>,
    generation: Arc<AtomicU64>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
) {
    let mut buf = [0u8; 4096];
    loop {
        // Drain any pending refresh requests with a stub response
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let _ = reply_tx.send(Ok(RefreshData {
                generation: gen,
                data: Bytes::new(), // filled in Task 3
                cursor_x: 0,
                cursor_y: 0,
            }));
        }

        let n = match master.read(&mut buf) {
            Ok(0) | Err(_) => {
                // EOF — shell exited. Stay alive for refresh requests until refresh_rx closes.
                loop {
                    match refresh_rx.recv() {
                        Ok(reply_tx) => {
                            let gen = generation.load(Ordering::Relaxed);
                            let _ = reply_tx.send(Ok(RefreshData {
                                generation: gen,
                                data: Bytes::new(),
                                cursor_x: 0,
                                cursor_y: 0,
                            }));
                        }
                        Err(_) => return, // PtyHandle dropped (destroyed)
                    }
                }
            }
            Ok(n) => n,
        };

        let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
        let chunk = Arc::new(PtyChunk {
            generation: gen,
            data: Bytes::copy_from_slice(&buf[..n]),
        });
        let _ = tx.send(chunk); // ignore SendError (no subscribers is fine)
    }
}
