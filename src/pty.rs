use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Write},
    os::unix::io::{AsRawFd, FromRawFd},
    os::fd::OwnedFd,
    sync::{Arc, Mutex, RwLock, atomic::{AtomicU32, AtomicU64, Ordering}},
    time::SystemTime,
};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use libghostty_vt::{Terminal, TerminalOptions, RenderState, ffi};
use libghostty_vt::fmt::{Format, Formatter, FormatterOptions};
use libghostty_vt::screen::Screen;
use libghostty_vt::selection::Selection;
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
pub struct SubscriberInfo {
    pub hostname:   String,
    pub cols:       u32,
    pub rows:       u32,
    pub created_at: std::time::SystemTime,
}

#[derive(Clone, Debug)]
pub struct PtyInfo {
    pub id:                u64,
    pub hostname:          String,
    pub pts_name:          String,
    pub cols:              u32,
    pub rows:              u32,
    pub title:             String,
    pub created_at:        SystemTime,
    pub last_subscribed_at: Option<SystemTime>,
    pub subscribers:       Option<Vec<(String, SubscriberInfo)>>,
    pub sort_order:        u32,
}

#[derive(Debug, Clone)]
pub struct PtyChunk {
    pub generation: u64,
    pub data: Bytes,
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

#[derive(Debug, Clone)]
pub enum PtyEvent {
    Data(PtyChunk),
    Refresh(Arc<RefreshData>),
    Metadata(Arc<PtyMetadata>),
}

#[derive(Clone, Debug)]
pub struct RefreshData {
    pub generation: u64,
    pub data: Bytes,
    pub cols: u32,
    pub rows: u32,
}

// pty_id is not included — callers supply it directly from the request (see RefreshData).
pub struct ScrollbackData {
    pub generation: u64,
    pub data: Bytes,
    pub total_scrollback_rows: u32,
}

pub struct PtyHandle {
    id: u64,
    pts_name: String,
    created_at: SystemTime,
    hostname: String,
    cols: AtomicU32,
    rows: AtomicU32,
    title: Arc<Mutex<String>>,
    tx: broadcast::Sender<PtyEvent>,
    writer: Mutex<File>,
    refresh_tx:    std::sync::mpsc::SyncSender<oneshot::Sender<Result<RefreshData>>>,
    scrollback_tx: std::sync::mpsc::SyncSender<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>,
    resize_tx:     std::sync::mpsc::SyncSender<(u32, u32)>,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    generation: Arc<AtomicU64>,
    subscribers: Arc<RwLock<HashMap<String, SubscriberInfo>>>,
    last_subscribed_at: Mutex<Option<SystemTime>>,
    // Assigned once at registration (next available slot, 0-based) for stable
    // list ordering; never changes for the life of the handle.
    sort_order: AtomicU32,
    child_pid: Pid,
    wakeup_write: OwnedFd,
}

impl PtyHandle {
    pub fn info(&self) -> PtyInfo {
        let subscribers = {
            let map = self.subscribers.read().unwrap();
            let mut v: Vec<(String, SubscriberInfo)> =
                map.iter().map(|(id, s)| (id.clone(), s.clone())).collect();
            v.sort_by_key(|(_, s)| s.created_at);
            v
        };  // read-lock released here
        PtyInfo {
            id:                self.id,
            hostname:          self.hostname.clone(),
            pts_name:          self.pts_name.clone(),
            cols:              self.cols.load(Ordering::Relaxed),
            rows:              self.rows.load(Ordering::Relaxed),
            title:             self.title.lock().unwrap().clone(),
            created_at:        self.created_at,
            last_subscribed_at: *self.last_subscribed_at.lock().unwrap(),
            subscribers:       Some(subscribers),
            sort_order:        self.sort_order.load(Ordering::Relaxed),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PtyEvent> {
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

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn touch_last_subscribed(&self) {
        *self.last_subscribed_at.lock().unwrap() = Some(SystemTime::now());
    }

    pub fn upsert_subscriber(&self, subscriber_id: &str, info: SubscriberInfo) {
        let mut map = self.subscribers.write().unwrap();
        map.entry(subscriber_id.to_owned())
            .and_modify(|e| {
                e.hostname = info.hostname.clone();
                e.cols     = info.cols;
                e.rows     = info.rows;
                // created_at intentionally not updated — preserve original
            })
            .or_insert(info);
    }

    pub fn remove_subscriber(&self, subscriber_id: &str) {
        self.subscribers.write().unwrap().remove(subscriber_id);
    }
}

pub struct PtyRegistry {
    ptys: RwLock<HashMap<u64, Arc<PtyHandle>>>,
}

impl PtyRegistry {
    pub fn new() -> Self {
        Self { ptys: RwLock::new(HashMap::new()) }
    }

    pub fn create(&self, cols: u32, rows: u32, command: Option<&str>) -> Result<Arc<PtyHandle>> {
        let id: u64 = uuid::Uuid::new_v4().as_u64_pair().0;
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
        let master = pty.master; // OwnedFd — closed automatically on any error return below
        let slave_fd = pty.slave;   // OwnedFd

        // Get slave device name (/dev/pts/N); ptsname_r is Linux-only, macOS only has ptsname
        let pts_name = unsafe {
            let borrowed = std::os::unix::io::BorrowedFd::borrow_raw(master.as_raw_fd());
            let owned = borrowed.try_clone_to_owned().context("clone master fd for ptsname")?;
            #[cfg(not(target_os = "macos"))]
            { nix::pty::ptsname_r(&nix::pty::PtyMaster::from_owned_fd(owned)) }
            #[cfg(target_os = "macos")]
            { nix::pty::ptsname(&nix::pty::PtyMaster::from_owned_fd(owned)) }
        }.unwrap_or_else(|_| String::from("unknown"));

        // Dup master for the reader thread before transferring ownership to File
        let master_reader = dup_fd(master.as_raw_fd()).context("dup master fd for reader")?;
        // Set O_NONBLOCK so the reader can drain all available bytes in a loop
        let flags = unsafe { libc::fcntl(master_reader.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(master_reader.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error()).context("set O_NONBLOCK on master reader fd");
        }

        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};
        // dup slave so each Stdio owns its own fd; the child will have them as 0/1/2
        let slave_stdout = dup_fd(slave_fd.as_raw_fd()).context("dup slave fd for stdout")?;
        let slave_stderr = dup_fd(slave_fd.as_raw_fd()).context("dup slave fd for stderr")?;
        // Set FD_CLOEXEC on all master and slave fds so concurrent forks
        // don't inherit them. Rust's spawn dup2s them to 0/1/2 in the
        // child before exec, so the shell still gets them correctly.
        for fd in [master.as_raw_fd(), master_reader.as_raw_fd(), slave_fd.as_raw_fd(), slave_stdout.as_raw_fd(), slave_stderr.as_raw_fd()] {
            let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
            if rc < 0 {
                return Err(std::io::Error::last_os_error()).context("set FD_CLOEXEC on open fds");
            }
        }
        // Spawn child shell
        let shell = command.map(String::from).unwrap_or_else(|| {
            std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
        });
        let mut cmd = Command::new(&shell);
        cmd.env("TERM", TERM_NAME)
           .env_remove("TERM_PROGRAM")
           .stdin(Stdio::from(slave_fd))
           .stdout(Stdio::from(slave_stdout))
           .stderr(Stdio::from(slave_stderr));
        // pre_exec runs in child after fork, before exec.
        // At this point stdin/stdout/stderr are already the slave fd.
        unsafe {
            cmd.pre_exec(move || {
                // New session — detach from parent's controlling terminal
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // fd 0 is already the slave; set it as controlling terminal
                // macOS ioctl takes c_ulong for the request; Linux takes c_int
                #[cfg(target_os = "macos")]
                let tiocsctty: libc::c_ulong = libc::TIOCSCTTY.into();
                #[cfg(not(target_os = "macos"))]
                let tiocsctty = libc::TIOCSCTTY;
                if libc::ioctl(0, tiocsctty, 0i32) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // TODO: systemd-logind session registration (Linux)
                Ok(())
            });
        }

        let (tx, _) = broadcast::channel::<PtyEvent>(512);
        let (meta_tx, _) = broadcast::channel::<Arc<PtyMetadata>>(64);
        let (refresh_tx, refresh_rx) =
            std::sync::mpsc::sync_channel::<oneshot::Sender<Result<RefreshData>>>(8);
        let (scrollback_tx, scrollback_rx) =
            std::sync::mpsc::sync_channel::<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>(8);
        let (resize_tx, resize_rx) = std::sync::mpsc::sync_channel::<(u32, u32)>(8);
        let generation = Arc::new(AtomicU64::new(0));

        // Create wakeup pipe before spawning the child so that a failure here doesn't
        // leave a zombie process behind.  O_CLOEXEC and O_NONBLOCK are set atomically
        // (Linux) or via fcntl fallback (macOS) inside wakeup_pipe().
        let (wakeup_read, wakeup_write) = wakeup_pipe().context("wakeup pipe")?;

        let child = cmd.spawn().context("spawn shell")?;
        // slave fds are owned by the Stdio objects passed to Command and closed after fork;

        let child_pid = Pid::from_raw(child.id() as i32);
        crate::utmp::add_record(master.as_raw_fd(), &hostname);
        let created_at = SystemTime::now();
        let title = Arc::new(Mutex::new(pts_name.clone()));
        let meta_tx_for_thread = meta_tx.clone();
        let id_for_thread = id;
        let hostname_for_thread = hostname.clone();
        let pts_name_for_thread = pts_name.clone();
        let handle = Arc::new(PtyHandle {
            id,
            pts_name,
            created_at,
            hostname,
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            title: title.clone(),
            tx: tx.clone(),
            writer: Mutex::new(File::from(master)),
            refresh_tx,
            scrollback_tx,
            resize_tx,
            meta_tx: meta_tx.clone(),
            generation: generation.clone(),
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            last_subscribed_at: Mutex::new(None),
            sort_order: AtomicU32::new(0), // real value assigned under the registry lock below
            child_pid,
            wakeup_write,
        });

        // Spawn dedicated reader thread — owns libghostty state and child process
        let master_reader = File::from(master_reader);
        let title_for_thread = title.clone();
        std::thread::Builder::new()
            .name(format!("pty-reader-{id:016x}"))
            .spawn(move || reader_thread(
                master_reader, tx, generation, refresh_rx, scrollback_rx, resize_rx, wakeup_read,
                child, title_for_thread, cols, rows,
                meta_tx_for_thread, id_for_thread, hostname_for_thread,
                pts_name_for_thread, created_at,
            ))
            .context("spawn reader thread")?;

        // Assign sort_order and insert atomically so concurrent creates can't
        // pick the same slot. Smallest unused 0-based value, reusing gaps left
        // by destroyed PTYs.
        {
            let mut map = self.ptys.write().unwrap();
            let used: HashSet<u32> =
                map.values().map(|h| h.sort_order.load(Ordering::Relaxed)).collect();
            let order = (0u32..).find(|n| !used.contains(n)).unwrap();
            handle.sort_order.store(order, Ordering::Relaxed);
            map.insert(id, handle.clone());
        }
        Ok(handle)
    }

    pub fn destroy(&self, id: u64) -> Result<()> {
        let handle = self.ptys.write().unwrap().remove(&id)
            .ok_or_else(|| anyhow!("PTY {:016x} not found", id))?;
        let _ = kill(handle.child_pid, Signal::SIGHUP);
        // handle drops at end of scope: wakeup_write closes → reader sees POLLHUP and exits.
        // If callers hold Arc<PtyHandle> clones (e.g. an in-flight refresh), wakeup_write
        // stays open until the last clone drops — POLLHUP fires then, not immediately on return.
        Ok(())
    }

    pub fn destroy_all(&self) {
        let ids: Vec<u64> = self.ptys.read().unwrap().keys().copied().collect();
        for id in ids {
            let _ = self.destroy(id);
        }
    }

    pub fn get(&self, id: u64) -> Option<Arc<PtyHandle>> {
        self.ptys.read().unwrap().get(&id).cloned()
    }

    pub fn list(&self) -> Vec<Arc<PtyHandle>> {
        self.ptys.read().unwrap().values().cloned().collect()
    }
}

impl Default for PtyRegistry {
    fn default() -> Self { Self::new() }
}

fn dup_fd(fd: std::os::unix::io::RawFd) -> std::io::Result<OwnedFd> {
    let new = unsafe { libc::dup(fd) };
    if new < 0 { Err(std::io::Error::last_os_error()) } else { Ok(unsafe { OwnedFd::from_raw_fd(new) }) }
}

// Creates a pipe with O_CLOEXEC and O_NONBLOCK set on both ends.
// pipe2 is Linux-only; on macOS we fall back to pipe + fcntl.
fn wakeup_pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0i32; 2];
    #[cfg(not(target_os = "macos"))]
    let ok = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) == 0 };
    #[cfg(target_os = "macos")]
    let ok = unsafe {
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            false
        } else {
            let mut success = true;
            for &fd in &fds {
                let fl = libc::fcntl(fd, libc::F_GETFL);
                let dfl = libc::fcntl(fd, libc::F_GETFD);
                if fl < 0 || dfl < 0
                    || libc::fcntl(fd, libc::F_SETFD, dfl | libc::FD_CLOEXEC) < 0
                    || libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK) < 0
                {
                    success = false;
                    break;
                }
            }
            if !success {
                libc::close(fds[0]);
                libc::close(fds[1]);
            }
            success
        }
    };
    if ok {
        Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
    } else {
        Err(std::io::Error::last_os_error())
    }
}

// Note: on-demand refresh only renders the active screen at call time.  The screen-switch
// broadcast in reader_thread (screen_changed path) mitigates the primary↔alternate gap by
// pushing a full render of the new screen to all subscribers immediately after the switch.
fn do_refresh(
    terminal: &Terminal<'static, 'static>,
    generation: u64,
) -> Result<RefreshData> {

    let cols = terminal.cols()? as u32;
    let rows = terminal.rows()? as u32;

    // Snapshot cursor visibility; formatter modes:true may not emit ?25h for the default-visible
    // case, so we emit it explicitly at the end to guarantee correct state after a PTY switch.
    let cursor_visible = {
        let mut rs = RenderState::new()?;
        rs.update(terminal)?.cursor_visible().unwrap_or(true)
    };

    // Restrict to the active screen — the server terminal has scrollback and we don't want it.
    // Point::Active resolves within the visible grid only, ignoring history rows.
    let top_left = terminal.grid_ref(Point::Active(PointCoordinate { x: 0, y: 0 }))?;
    let bottom_right = terminal.grid_ref(Point::Active(PointCoordinate {
        x: cols.saturating_sub(1) as u16,
        y: rows.saturating_sub(1),
    }))?;
    let selection = Selection::new(top_left, bottom_right, false);

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
            cursor: true,        // emit final cursor position at end of output
            style: true,         // restore SGR attributes at cursor so subsequent output is styled correctly
            hyperlink: false,
            protection: false,
            kitty_keyboard: false,
            charsets: true,      // restore G0-G3 charset designations (e.g. DEC line-drawing)
            saved_cursor: true,  // re-establish DECSC save slot for cursor restore
            // TODO: pending_wrap is not restored — CUP clears it, so if the server
            // cursor was at the last column with pending_wrap=true the client will
            // overwrite instead of wrapping on the next print.  Fixing this likely
            // requires a formatter-level mechanism (e.g. print+backspace at the last
            // column) and careful testing.
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
    // Soft reset (DECSTR) + explicit mouse-mode disables + clear screen + cursor home.
    // DECSTR alone does not reliably disable mouse-reporting modes on all terminals, so we
    // disable them explicitly before the formatter re-enables whatever the server PTY has set
    // (via modes:true).  The formatter output is sent as one blob, so no cursor flicker.
    out.extend_from_slice(b"\x1b[!p");
    out.extend_from_slice(b"\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l");
    out.extend_from_slice(b"\x1b[2J\x1b[H");
    let vt = fmt.format_alloc(None)?;
    out.extend_from_slice(&vt);
    out.extend_from_slice(b"\x1b[0m"); // trailing SGR reset
    // Explicit cursor visibility — formatter modes:true may not emit ?25h when cursor is visible
    // (treating it as the default, no-op), which leaves the cursor hidden after a switch from a
    // PTY that had it hidden.
    if cursor_visible {
        out.extend_from_slice(b"\x1b[?25h");
    } else {
        out.extend_from_slice(b"\x1b[?25l");
    }

    Ok(RefreshData {
        generation,
        data: Bytes::from(out),
        cols,
        rows,
    })
}

fn do_scrollback(
    terminal:   &Terminal<'static, 'static>,
    row_offset: u32,
    row_count:  u32,
    generation: u64,
    cols:       u32,
) -> Result<ScrollbackData> {
    // Screen space covers history + active screen; y=0 is oldest history, y=total-1
    // is the last row of the active screen. row_offset=0 therefore starts from the
    // visible screen and increases upward into history.
    let total = terminal.total_rows()? as u32;

    if total == 0 || row_count == 0 {
        return Ok(ScrollbackData { generation, data: Bytes::new(), total_scrollback_rows: total });
    }
    if row_offset >= total {
        return Ok(ScrollbackData { generation, data: Bytes::new(), total_scrollback_rows: total });
    }

    // Helper closure: FormatterTerminalExtra with all flags false.
    // Both FormatterTerminalExtra and FormatterScreenExtra use the sized-struct ABI —
    // the `size` field must be set explicitly; Default::default() would leave size=0.
    let make_extra = || ffi::FormatterTerminalExtra {
        size: std::mem::size_of::<ffi::FormatterTerminalExtra>(),
        scrolling_region: false,
        modes: false,
        palette: false,
        tabstops: false,
        pwd: false,
        keyboard: false,
        screen: ffi::FormatterScreenExtra {
            size: std::mem::size_of::<ffi::FormatterScreenExtra>(),
            cursor: false,
            style: false,
            hyperlink: false,
            protection: false,
            kitty_keyboard: false,
            charsets: false,
            saved_cursor: false,
        },
    };

    // Full-buffer case: use Point::Screen endpoints to cover history + active screen.
    // NOTE: grid_ref(Point::Screen(...)) traverses the internal scrollback page list to
    // locate the target row, which is O(scrollback_depth). If scrollback requests become a
    // latency concern (do_scrollback runs on the reader thread, blocking live PTY I/O),
    // consider offloading to a background thread.
    if row_offset == 0 && row_count >= total {
        let top_left = terminal.grid_ref(Point::Screen(PointCoordinate { x: 0, y: 0 }))?;
        let bot_right = terminal.grid_ref(Point::Screen(PointCoordinate {
            x: cols.saturating_sub(1) as u16,
            y: total - 1,
        }))?;
        let selection = Selection::new(top_left, bot_right, false);
        let mut fmt = Formatter::new(terminal, FormatterOptions {
            format: Format::Vt,
            trim: false,
            unwrap: false,
            selection: Some(selection),
            extra: make_extra(),
        })?;
        let vt = fmt.format_alloc(None)?;
        return Ok(ScrollbackData {
            generation,
            data: Bytes::from(vt.to_vec()),
            total_scrollback_rows: total,
        });
    }

    // Partial range: convert row_offset (distance from bottom of screen) to Point::Screen y-coords.
    // Screen: y=0 = oldest history row, y=total-1 = last row of active screen.
    let end_y   = total - 1 - row_offset;
    let rows    = row_count.min(end_y + 1);
    let start_y = end_y + 1 - rows;

    let top_left = terminal.grid_ref(Point::Screen(PointCoordinate { x: 0, y: start_y }))?;
    let bot_right = terminal.grid_ref(Point::Screen(PointCoordinate {
        x: cols.saturating_sub(1) as u16,
        y: end_y,
    }))?;
    let selection = Selection::new(top_left, bot_right, false);

    let mut fmt = Formatter::new(terminal, FormatterOptions {
        format: Format::Vt,
        trim: false,
        unwrap: false,
        selection: Some(selection),
        extra: make_extra(),
    })?;
    let vt = fmt.format_alloc(None)?;
    Ok(ScrollbackData {
        generation,
        data: Bytes::from(vt.to_vec()),
        total_scrollback_rows: total,
    })
}

// Reader thread — owns libghostty Terminal state (not Send + Sync).
#[allow(clippy::too_many_arguments)]
fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<PtyEvent>,
    generation: Arc<AtomicU64>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
    scrollback_rx: std::sync::mpsc::Receiver<(u32, u32, oneshot::Sender<Result<ScrollbackData>>)>,
    resize_rx: std::sync::mpsc::Receiver<(u32, u32)>,
    wakeup_read: OwnedFd,
    mut child: std::process::Child,
    title: Arc<Mutex<String>>,
    init_cols: u32,
    init_rows: u32,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    pty_id: u64,
    hostname: String,
    pts_name: String,
    created_at: SystemTime,
) {
    let mut terminal = match Terminal::new(TerminalOptions {
        cols: init_cols as u16,
        rows: init_rows as u16,
        max_scrollback: 1_000_000, // NOTE: this is bytes of scrollback, not lines
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
                match do_refresh(&terminal, refresh_gen) {
                    Ok(data) => { let _ = tx.send(PtyEvent::Refresh(Arc::new(data))); }
                    Err(e) => tracing::debug!("PTY reader: resize refresh failed: {e}"),
                }
            }
        }
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_refresh(&terminal, gen);
            let _ = reply_tx.send(result);
        }
        while let Ok((row_offset, row_count, reply_tx)) = scrollback_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_scrollback(&terminal, row_offset, row_count, gen, current_cols);
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
        let _ = tx.send(PtyEvent::Data(PtyChunk {
            generation: gen,
            data: Bytes::from(batch),
        })); // ignore SendError (no subscribers is fine)
        // Emit TitleChanged after fetch_add so generation matches the accompanying chunk.
        if title_changed {
            let _ = meta_tx.send(Arc::new(PtyMetadata {
                reason: MetadataReason::TitleChanged,
                exit_code: None,
                generation: gen,
                info: PtyInfo {
                    id: pty_id,
                    hostname: hostname.clone(),
                    pts_name: pts_name.clone(),
                    cols: current_cols,
                    rows: current_rows,
                    title: current_title,
                    created_at,
                    last_subscribed_at: None,
                    subscribers: None, // subscriber map lives on PtyHandle, unavailable here
                    sort_order: 0,     // lives on PtyHandle, unavailable here
                },
            }));
        }
        // On screen switch (primary ↔ alternate), broadcast a full render of the new screen
        // so subscribers that hadn't seen it get the correct content immediately.
        if screen_changed {
            let refresh_gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
            match do_refresh(&terminal, refresh_gen) {
                Ok(data) => { let _ = tx.send(PtyEvent::Refresh(Arc::new(data))); }
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
    let _ = tx.send(PtyEvent::Data(PtyChunk {
        generation: gen,
        data: Bytes::from(exit_msg.into_bytes()),
    }));
    let exit_code = status.as_ref().and_then(|s| s.code());
    let _ = meta_tx.send(Arc::new(PtyMetadata {
        reason: MetadataReason::Closed,
        exit_code,
        generation: generation.load(Ordering::Relaxed),
        info: PtyInfo {
            id: pty_id,
            hostname: hostname.clone(),
            pts_name: pts_name.clone(),
            cols: current_cols,
            rows: current_rows,
            title: title.lock().unwrap().clone(),
            created_at,
            last_subscribed_at: None,
            subscribers: None, // subscriber map lives on PtyHandle, unavailable here
            sort_order: 0,     // lives on PtyHandle, unavailable here
        },
    }));

    // Drain any refresh requests that arrived just before exit
    while let Ok(reply_tx) = refresh_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_refresh(&terminal, gen);
        let _ = reply_tx.send(result);
    }
    while let Ok((row_offset, row_count, reply_tx)) = scrollback_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_scrollback(&terminal, row_offset, row_count, gen, current_cols);
        let _ = reply_tx.send(result);
    }

    drop(wakeup_read); // closes read end; wakeup_write already closed (PtyHandle dropped)
}

#[cfg(test)]
mod subscriber_tests {
    use super::*;

    fn make_handle() -> Arc<PtyHandle> {
        let reg = PtyRegistry::new();
        reg.create(80, 24, None).unwrap()
    }

    fn make_info(hostname: &str) -> SubscriberInfo {
        SubscriberInfo { hostname: hostname.into(), cols: 80, rows: 24, created_at: SystemTime::now() }
    }

    #[test]
    fn upsert_inserts_new_subscriber() {
        let h = make_handle();
        h.upsert_subscriber("abc", make_info("host1"));
        let subs = h.info().subscribers.unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].0, "abc");
        assert_eq!(subs[0].1.hostname, "host1");
    }

    #[test]
    fn upsert_updates_existing_preserves_created_at() {
        let h = make_handle();
        let t = SystemTime::UNIX_EPOCH;
        h.upsert_subscriber("abc", SubscriberInfo {
            hostname: "host1".into(), cols: 80, rows: 24, created_at: t,
        });
        h.upsert_subscriber("abc", SubscriberInfo {
            hostname: "host2".into(), cols: 100, rows: 30, created_at: SystemTime::now(),
        });
        let subs = h.info().subscribers.unwrap();
        assert_eq!(subs.len(), 1, "no duplicate");
        assert_eq!(subs[0].1.hostname, "host2");
        assert_eq!(subs[0].1.cols, 100);
        assert_eq!(subs[0].1.created_at, t, "original created_at preserved");
    }

    #[test]
    fn remove_subscriber_deletes_entry() {
        let h = make_handle();
        h.upsert_subscriber("abc", make_info("host1"));
        h.remove_subscriber("abc");
        assert!(h.info().subscribers.unwrap().is_empty());
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let h = make_handle();
        h.remove_subscriber("nonexistent"); // must not panic
    }
}

#[cfg(test)]
mod scrollback_tests {
    use super::*;

    fn make_terminal(cols: u16, rows: u16, scrollback: usize) -> Terminal<'static, 'static> {
        Terminal::new(TerminalOptions { cols, rows, max_scrollback: scrollback }).unwrap()
    }

    #[test]
    fn do_scrollback_empty_when_row_count_zero() {
        let terminal = make_terminal(80, 24, 1000);
        let result = do_scrollback(&terminal, 0, 0, 42, 80).unwrap();
        assert_eq!(result.generation, 42);
        assert!(result.data.is_empty());
    }

    #[test]
    fn do_scrollback_offset_beyond_total_returns_empty() {
        let mut terminal = make_terminal(80, 5, 1000);
        // Push 10 rows of scrollback by writing 15 lines (5-row screen scrolls 10 into history)
        for i in 0..15u8 {
            terminal.vt_write(format!("line{}\n", i).as_bytes());
        }
        let total = terminal.total_rows().unwrap() as u32;
        assert!(total > 0, "expected rows");
        let result = do_scrollback(&terminal, total, 10, 7, 80).unwrap();
        assert!(result.data.is_empty());
        assert_eq!(result.total_scrollback_rows, total);
    }

    // --- FFI selection-pointer regression guards ----------------------------
    //
    // do_refresh and do_scrollback build a `Selection` from grid_refs and hand
    // it across the FFI boundary to the libghostty-vt formatter. A dangling
    // selection pointer on the Rust side (e.g. a libghostty-rs change that
    // reintroduces the `&s.inner`-from-a-match-arm use-after-free) corrupts the
    // page-list pin and segfaults inside Ghostty's page iterator. These render
    // real content through both paths so a reintroduced bug fails the suite.
    //
    // The use-after-free faults *reliably* only under `--release`; in a debug
    // build the freed stack slot usually still holds valid bytes, so a debug run
    // may not fault. Run `cargo test --release` for the dependable check.

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn do_refresh_renders_selection_content() {
        let mut terminal = make_terminal(80, 24, 1000);
        terminal.vt_write(b"Hello World");
        let r = do_refresh(&terminal, 7).unwrap();
        assert_eq!(r.generation, 7);
        assert!(!r.data.is_empty(), "do_refresh produced no output");
        assert!(
            contains(&r.data, b"Hello World"),
            "rendered output missing the screen content (selection path broken?)"
        );
    }

    #[test]
    fn do_refresh_empty_terminal_does_not_crash() {
        let terminal = make_terminal(80, 24, 1000);
        let r = do_refresh(&terminal, 1).unwrap();
        assert_eq!(r.generation, 1);
    }

    #[test]
    fn do_scrollback_renders_history_content() {
        let mut terminal = make_terminal(80, 5, 1000);
        // 12 lines into a 5-row screen pushes 7 rows of history above the screen.
        for i in 0..12u8 {
            terminal.vt_write(format!("line{}\r\n", i).as_bytes());
        }
        let total = terminal.total_rows().unwrap() as u32;
        // Full buffer (history + active) exercises the Point::Screen selection path.
        let r = do_scrollback(&terminal, 0, total, 9, 80).unwrap();
        assert_eq!(r.generation, 9);
        assert_eq!(r.total_scrollback_rows, total);
        assert!(!r.data.is_empty(), "do_scrollback produced no output");
        assert!(
            contains(&r.data, b"line0"),
            "scrollback output missing an early history line (selection path broken?)"
        );
    }
}
