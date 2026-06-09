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
use libghostty_vt::{Terminal, TerminalOptions};
use libghostty_vt::screen::Screen;
use libghostty_vt::screen::TrackedGridRef;
use nix::{
    pty::openpty,
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tokio::sync::{broadcast, oneshot};

mod snapshot;
use snapshot::{do_refresh, do_scrollback};

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
    /// True when this snapshot was forced mid-sequence by the stall timeout,
    /// and is not pinned at a clean VT escape sequence boundary.
    /// See docs/REFRESH.md.
    pub degraded: bool,
}

// pty_id is not included — callers supply it directly from the request (see RefreshData).
pub struct ScrollbackData {
    pub generation: u64,
    pub data: Bytes,
    pub total_scrollback_rows: u32,
    /// Viewport bottom-edge distance from the live tail (0 = tail).
    pub row_offset: u32,
}

/// Imperative scrollback intent. The pin's position lives on the server; the
/// client only places (`Open`), nudges (`Move`), or removes (`Close`) it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrollbackOp { Open, Move, Close }

/// One scrollback request handed to the reader thread over `scrollback_tx`.
struct ScrollbackReq {
    subscriber_id: String,
    op: ScrollbackOp,
    amount: i32,
    row_count: u32,
    reply: oneshot::Sender<Result<ScrollbackData>>,
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
    scrollback_tx: std::sync::mpsc::SyncSender<ScrollbackReq>,
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

    pub async fn scrollback(
        &self,
        subscriber_id: &str,
        op: ScrollbackOp,
        amount: i32,
        row_count: u32,
    ) -> Result<ScrollbackData> {
        let (tx, rx) = oneshot::channel();
        self.scrollback_tx.send(ScrollbackReq {
            subscriber_id: subscriber_id.to_owned(), op, amount, row_count, reply: tx,
        }).map_err(|_| anyhow!("PTY reader thread is dead"))?;
        let wfd = self.wakeup_write.as_raw_fd();
        let ret = unsafe { libc::write(wfd, [1u8].as_ptr() as *const libc::c_void, 1) };
        if ret < 0 {
            tracing::debug!("wakeup write failed: {}", std::io::Error::last_os_error());
        }
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped scrollback response"))?
    }

    /// Best-effort release of a subscriber's scrollback pin (teardown paths).
    pub fn close_scrollback(&self, subscriber_id: &str) {
        let (tx, _rx) = oneshot::channel();
        let _ = self.scrollback_tx.try_send(ScrollbackReq {
            subscriber_id: subscriber_id.to_owned(),
            op: ScrollbackOp::Close, amount: 0, row_count: 0, reply: tx,
        });
        let wfd = self.wakeup_write.as_raw_fd();
        let _ = unsafe { libc::write(wfd, [1u8].as_ptr() as *const libc::c_void, 1) };
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
            std::sync::mpsc::sync_channel::<ScrollbackReq>(8);
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


/// Milliseconds to wait when a refresh is deferred but the parser is stuck
/// mid-sequence (the app wrote a partial escape sequence then went idle). Bounds
/// how long an attach/refresh can block before we give up and snapshot anyway.
const REFRESH_STALL_TIMEOUT_MS: libc::c_int = 1000;

/// Service deferred refreshes — the on-demand (attach) replies and the
/// resize/screen-switch broadcast. Callers gate this on `vt_at_boundary()` so the
/// snapshot is taken at a VT ground boundary: the pinned generation then names a
/// batch that ended clean, so a client dropping `<= gen` resumes on the next batch
/// (which starts clean) rather than on an orphaned escape-sequence tail.
///
/// The one exception is the stall-timeout fallback, which calls this mid-sequence
/// on purpose to avoid blocking an attach forever; that path accepts a degraded
/// boundary (rare, and self-heals on the app's next full repaint).
fn flush_refreshes(
    terminal: &Terminal<'static, 'static>,
    generation: &AtomicU64,
    tx: &broadcast::Sender<PtyEvent>,
    pending_replies: &mut Vec<oneshot::Sender<Result<RefreshData>>>,
    pending_broadcast: &mut bool,
    degraded: bool,
) {
    if pending_replies.is_empty() && !*pending_broadcast {
        return;
    }
    let refresh_gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
    match do_refresh(terminal, refresh_gen) {
        Ok(mut data) => {
            data.degraded = degraded; // true only on the stall-timeout path

            for reply in pending_replies.drain(..) {
                let _ = reply.send(Ok(data.clone()));
            }
            if *pending_broadcast {
                *pending_broadcast = false;
                let _ = tx.send(PtyEvent::Refresh(Arc::new(data)));
            }
        }
        Err(e) => {
            tracing::debug!("PTY reader: deferred refresh failed: {e}");
            *pending_broadcast = false;
            for reply in pending_replies.drain(..) {
                let _ = reply.send(Err(anyhow!("refresh failed: {e}")));
            }
        }
    }
}

/// Apply one PTY read to the terminal and broadcast it to subscribers, returning
/// the generation stamped on the last data chunk emitted (so a co-emitted metadata
/// event can carry the matching generation).
///
/// Fast path (no refresh pending): write and broadcast the whole read as a single
/// generation — the read boundary was never meaningful to subscribers, who just
/// concatenate the bytes.
///
/// When a refresh *is* pending the parser is, by construction, mid-sequence: the
/// boundary check before `poll()` flushes any pending refresh whenever the parser
/// sits at ground, and nothing writes to the terminal between that check and here.
/// So a surviving pending refresh means the previous read ended inside an escape
/// sequence (or a multi-byte codepoint). Rather than hope some later read happens
/// to end at a boundary, walk this read one byte at a time until the parser returns
/// to ground, then split the read there: broadcast the head (which now ends clean),
/// pin the refresh snapshot at that boundary, and broadcast the tail as its own
/// generation. An attaching client drops everything `<=` the refresh generation
/// (the head) and resumes on the tail, which starts at ground — no orphaned tail.
///
/// If the whole read sits inside one unfinished sequence (e.g. a long OSC), there
/// is no boundary to split on: broadcast it whole and leave the refresh pending for
/// a later read. The `poll()` stall timeout covers the case where no later read ever
/// comes (the app went idle mid-sequence) so the attach can't block forever.
fn process_read(
    terminal: &mut Terminal<'static, 'static>,
    batch: Bytes,
    generation: &AtomicU64,
    tx: &broadcast::Sender<PtyEvent>,
    pending_replies: &mut Vec<oneshot::Sender<Result<RefreshData>>>,
    pending_broadcast: &mut bool,
) -> u64 {
    // Stamp the next generation onto a raw chunk and broadcast it; returns the gen.
    let broadcast_data = |data: Bytes| -> u64 {
        let gen = generation.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = tx.send(PtyEvent::Data(PtyChunk { generation: gen, data }));
        gen
    };

    if pending_replies.is_empty() && !*pending_broadcast {
        // Fast path: nothing waiting on a boundary, so don't pay the byte-by-byte
        // scan — write and broadcast the read whole.
        terminal.vt_write(&batch);
        return broadcast_data(batch);
    }

    // A refresh is pending => the parser is mid-sequence. Feed bytes one at a time
    // until it returns to a ground boundary; that offset is where we split the read.
    let mut split_at = None;
    for i in 0..batch.len() {
        terminal.vt_write(&batch[i..i + 1]);
        if terminal.vt_at_boundary() {
            split_at = Some(i + 1);
            break;
        }
    }

    match split_at {
        Some(i) => {
            // batch[..i] completes the open sequence and ends clean; snapshot there.
            let head_gen = broadcast_data(batch.slice(0..i));
            flush_refreshes(terminal, generation, tx, pending_replies, pending_broadcast, false);
            let tail = batch.slice(i..);
            if tail.is_empty() {
                head_gen // read ended exactly at the boundary — no tail to emit
            } else {
                // The split loop only wrote batch[..i]; apply the tail now so the
                // reader's parser/screen stays in sync with the real byte stream
                // (vt_at_boundary on later reads depends on it). Snapshot is already
                // pinned above, so the tail correctly lands after it.
                terminal.vt_write(&tail);
                broadcast_data(tail) // resumes on ground for any attaching client
            }
        }
        None => {
            // The whole read stayed inside one unfinished sequence (the loop already
            // wrote every byte). Broadcast it whole; the refresh stays pending for
            // the read that finally completes the sequence.
            broadcast_data(batch)
        }
    }
}

impl PtyInfo {
    /// Barebones info for a `Closed` event. Only `id` is read by consumers off a
    /// `Closed` (it identifies the PTY); the rest is ignored there — see
    /// `ClosedNotifier`. `exit_code` / `generation` travel as their own fields on
    /// `PtyMetadata`, not in here.
    fn closed(id: u64, created_at: SystemTime) -> Self {
        PtyInfo {
            id,
            hostname: String::new(),
            pts_name: String::new(),
            cols: 0,
            rows: 0,
            title: String::new(),
            created_at,
            last_subscribed_at: None,
            subscribers: None,
            sort_order: 0,
        }
    }
}

/// Sole emitter of `reader_thread`'s `Closed` metadata, fired from `Drop` so it runs
/// whether the thread returns normally or unwinds. The normal exit path sets
/// `exit_code` before falling through; a panic leaves it `None` ("unknown"). The
/// payload is deliberately barebones (no consumer reads title/size/host off a
/// `Closed`), which keeps this guard to a few copy-cheap fields with no mirrored
/// state. It does not touch the libghostty `Terminal` (unsafe mid-panic) or the utmp
/// record (a leaked record on a reader panic is tolerated; the normal path removes it).
struct ClosedNotifier {
    meta_tx:    broadcast::Sender<Arc<PtyMetadata>>,
    generation: Arc<AtomicU64>,
    pty_id:     u64,
    created_at: SystemTime,
    exit_code:  Option<i32>,
}

impl Drop for ClosedNotifier {
    fn drop(&mut self) {
        let _ = self.meta_tx.send(Arc::new(PtyMetadata {
            reason:     MetadataReason::Closed,
            exit_code:  self.exit_code,
            generation: self.generation.load(Ordering::Relaxed),
            info:       PtyInfo::closed(self.pty_id, self.created_at),
        }));
    }
}

// Reader thread — owns libghostty Terminal state (not Send + Sync).
#[allow(clippy::too_many_arguments)]
fn reader_thread(
    mut master: File,
    tx: broadcast::Sender<PtyEvent>,
    generation: Arc<AtomicU64>,
    refresh_rx: std::sync::mpsc::Receiver<oneshot::Sender<Result<RefreshData>>>,
    scrollback_rx: std::sync::mpsc::Receiver<ScrollbackReq>,
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
        // Byte budget for scrollback page memory, NOT a line count. libghostty
        // allocates whole ~0.5 MB pages (a page is sized for a 215x215 grid; each
        // Cell is 8 bytes), and every row costs cols*8 bytes regardless of how few
        // glyphs it holds. Rough rule at 80 cols: ~2 KB/line, so 16 MB ≈ ~8k lines
        // (proportionally fewer on wider terminals).
        max_scrollback: 16_000_000,
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
    // One scrollback pin per subscriber; the pin marks the viewport's top row and
    // libghostty keeps it on its content across appends and eviction.
    let mut scrollback_pins: HashMap<String, TrackedGridRef> = HashMap::new();
    // Initialize prev_title to match the initial title mutex value (pts_name),
    // so we don't emit a spurious TitleChanged before the shell sets any title.
    let mut prev_title = pts_name.clone();
    let mut prev_screen = Screen::Primary;

    // Refreshes are deferred until the VT parser is at a ground boundary, so a
    // snapshot never pins a generation in the middle of an escape sequence (which
    // would leave an attaching client resuming on an orphaned sequence tail). The
    // live broadcast path is untouched — only refresh emission waits for ground.
    let mut pending_replies: Vec<oneshot::Sender<Result<RefreshData>>> = Vec::new();
    let mut pending_broadcast_refresh = false; // set by resize / screen switch

    // master_fd is only valid as long as master (the owning File) is alive
    let master_fd = master.as_raw_fd();
    let wakeup_fd = wakeup_read.as_raw_fd();

    // Emits the `Closed` metadata from its Drop — on a clean return below (after
    // exit_code is set) or on an unwind. A panic thus still tells attached clients the
    // PTY is gone (they key off StreamMetadataReason::Closed to detach) instead of
    // leaving them hung. utmp removal stays on the normal path only (leak on panic OK).
    let mut closed = ClosedNotifier {
        meta_tx:    meta_tx.clone(),
        generation: generation.clone(),
        pty_id,
        created_at,
        exit_code:  None,
    };

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
                // Defer the redraw to a ground boundary (serviced below / post-read).
                pending_broadcast_refresh = true;
            }
        }
        // Queue on-demand (attach) refreshes; service them only at a boundary.
        while let Ok(reply_tx) = refresh_rx.try_recv() {
            pending_replies.push(reply_tx);
        }
        // If the parser is already at a ground boundary, service deferred refreshes
        // now; otherwise they wait for the batch that completes the open sequence.
        if (!pending_replies.is_empty() || pending_broadcast_refresh) && terminal.vt_at_boundary() {
            flush_refreshes(&terminal, &generation, &tx, &mut pending_replies, &mut pending_broadcast_refresh, false);
        }
        while let Ok(req) = scrollback_rx.try_recv() {
            let gen = generation.load(Ordering::Relaxed);
            let result = do_scrollback(
                &mut terminal, &mut scrollback_pins, &req.subscriber_id,
                req.op, req.amount, req.row_count, gen, current_cols,
            );
            let _ = req.reply.send(result);
        }

        // Poll both the PTY master and the wakeup pipe.  Writes to wakeup_write
        // (refresh or resize) unblock the poll so requests are handled promptly.
        let mut pfds = [
            libc::pollfd { fd: master_fd, events: libc::POLLIN, revents: 0 },
            libc::pollfd { fd: wakeup_fd, events: libc::POLLIN, revents: 0 },
        ];
        // Block indefinitely unless a refresh is waiting behind an unfinished
        // sequence (it didn't flush above, so the parser is mid-sequence) — then
        // cap the wait so an idle mid-sequence PTY can't stall the attach forever.
        let refresh_pending = !pending_replies.is_empty() || pending_broadcast_refresh;
        let poll_timeout = if refresh_pending { REFRESH_STALL_TIMEOUT_MS } else { -1 };
        let poll_ret = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, poll_timeout) };

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

        // Stall timeout: a refresh has been waiting for a ground boundary that
        // hasn't come (app went idle mid-sequence). Snapshot anyway rather than
        // block the attach — a degraded boundary that self-heals on the next repaint.
        if poll_ret == 0 {
            flush_refreshes(&terminal, &generation, &tx, &mut pending_replies, &mut pending_broadcast_refresh, true);
            continue;
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

        // Write and broadcast the read. If a refresh is pending, process_read splits
        // the read at a VT ground boundary and pins the snapshot there (see its doc);
        // otherwise it's the plain whole-read broadcast. `gen` is the last data chunk's
        // generation, so an accompanying TitleChanged below carries a matching gen.
        let gen = process_read(
            &mut terminal, Bytes::from(batch), &generation, &tx,
            &mut pending_replies, &mut pending_broadcast_refresh,
        );

        // Screen / title state is read after the full read has been applied.
        let current_screen = terminal.active_screen().unwrap_or(Screen::Primary);
        if current_screen != prev_screen {
            prev_screen = current_screen;
            // Defer the new-screen redraw to a ground boundary (serviced below).
            pending_broadcast_refresh = true;
        }
        let current_title = title.lock().unwrap().clone();
        if current_title != prev_title {
            prev_title = current_title.clone();
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
        // A refresh can have become pending *this* round (a screen switch above), and
        // process_read only splits for refreshes that were already pending on entry.
        // If the read left the parser at ground, service that fresh refresh now;
        // otherwise it waits for the read that completes the open sequence.
        if (!pending_replies.is_empty() || pending_broadcast_refresh) && terminal.vt_at_boundary() {
            flush_refreshes(&terminal, &generation, &tx, &mut pending_replies, &mut pending_broadcast_refresh, false);
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
    // Hand the real exit code to the notifier; its Drop emits the Closed event (here
    // on a normal return, or during unwind on a panic).
    closed.exit_code = status.as_ref().and_then(|s| s.code());

    // Service refreshes deferred at exit, plus any that arrived just before it.
    // The terminal is final now, so render directly without waiting for a boundary.
    for reply_tx in pending_replies.drain(..) {
        let gen = generation.load(Ordering::Relaxed);
        let _ = reply_tx.send(do_refresh(&terminal, gen));
    }
    while let Ok(reply_tx) = refresh_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_refresh(&terminal, gen);
        let _ = reply_tx.send(result);
    }
    while let Ok(req) = scrollback_rx.try_recv() {
        let gen = generation.load(Ordering::Relaxed);
        let result = do_scrollback(
            &mut terminal, &mut scrollback_pins, &req.subscriber_id,
            req.op, req.amount, req.row_count, gen, current_cols,
        );
        let _ = req.reply.send(result);
    }

    drop(wakeup_read); // closes read end; wakeup_write already closed (PtyHandle dropped)
}

#[cfg(test)]
mod boundary_tests {
    use super::*;

    fn make_terminal() -> Terminal<'static, 'static> {
        Terminal::new(TerminalOptions { cols: 80, rows: 24, max_scrollback: 1000 }).unwrap()
    }

    // The defer-refresh design rests on vt_at_boundary() reporting false exactly
    // while the parser holds an unfinished sequence, and true once it completes.

    #[test]
    fn fresh_and_plain_text_is_at_boundary() {
        let mut t = make_terminal();
        assert!(t.vt_at_boundary(), "fresh terminal should be at a boundary");
        t.vt_write(b"plain text");
        assert!(t.vt_at_boundary(), "printable text leaves the parser at ground");
    }

    #[test]
    fn mid_csi_is_not_at_boundary_until_completed() {
        let mut t = make_terminal();
        t.vt_write(b"abc\x1b[38;2;255"); // batch ends mid-CSI (truecolor SGR)
        assert!(!t.vt_at_boundary(), "unfinished CSI must report not-at-boundary");
        t.vt_write(b";0;0mxyz"); // completes the sequence
        assert!(t.vt_at_boundary(), "completed CSI returns to a boundary");
    }

    #[test]
    fn mid_osc_is_not_at_boundary_until_terminated() {
        let mut t = make_terminal();
        t.vt_write(b"\x1b]0;my titl"); // mid-OSC string (no ST/BEL yet)
        assert!(!t.vt_at_boundary(), "unterminated OSC must report not-at-boundary");
        t.vt_write(b"e\x07"); // BEL terminates the OSC
        assert!(t.vt_at_boundary(), "terminated OSC returns to a boundary");
    }

    #[test]
    fn mid_utf8_is_not_at_boundary_until_completed() {
        let mut t = make_terminal();
        // '€' is E2 82 AC; feed only the lead byte.
        t.vt_write(&[0xE2]);
        assert!(!t.vt_at_boundary(), "partial multi-byte UTF-8 must report not-at-boundary");
        t.vt_write(&[0x82, 0xAC]);
        assert!(t.vt_at_boundary(), "completed codepoint returns to a boundary");
    }

    #[test]
    fn esc_anywhere_recovers_boundary_reporting() {
        // A bare ESC opens a sequence; a following real sequence completes to ground.
        let mut t = make_terminal();
        t.vt_write(b"\x1b");
        assert!(!t.vt_at_boundary());
        t.vt_write(b"[0m");
        assert!(t.vt_at_boundary());
    }
}

#[cfg(test)]
mod process_read_tests {
    use super::*;

    fn make_terminal() -> Terminal<'static, 'static> {
        Terminal::new(TerminalOptions { cols: 80, rows: 24, max_scrollback: 1000 }).unwrap()
    }

    // Non-blocking drain of every event the broadcast currently holds.
    fn collect(rx: &mut broadcast::Receiver<PtyEvent>) -> Vec<PtyEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    // With no refresh waiting, a read is written and broadcast unchanged — one
    // Data chunk, no splitting, no Refresh.
    #[test]
    fn no_pending_refresh_emits_single_data_chunk() {
        let mut t = make_terminal();
        let (tx, mut rx) = broadcast::channel(16);
        let generation = AtomicU64::new(0);
        let mut replies = Vec::new();
        let mut pending = false;

        process_read(&mut t, Bytes::from_static(b"hello"), &generation, &tx, &mut replies, &mut pending);

        let events = collect(&mut rx);
        assert_eq!(events.len(), 1, "expected a single Data chunk");
        match &events[0] {
            PtyEvent::Data(c) => assert_eq!(&c.data[..], b"hello"),
            other => panic!("expected Data, got {other:?}"),
        }
    }

    // The core case: a refresh is pending and the parser is mid-sequence (the
    // previous read ended inside a CSI). The read is split at the first ground
    // boundary into head + tail, with the snapshot pinned strictly between them.
    #[test]
    fn pending_refresh_splits_read_at_boundary_into_head_refresh_tail() {
        let mut t = make_terminal();
        t.vt_write(b"\x1b[31"); // leave the parser mid-SGR
        assert!(!t.vt_at_boundary());

        let (tx, mut rx) = broadcast::channel(16);
        let generation = AtomicU64::new(0);
        let mut replies = Vec::new();
        let mut pending = true; // a resize/screen-switch broadcast refresh is waiting

        // 'm' completes the SGR (first boundary); "hello" is the clean tail.
        process_read(&mut t, Bytes::from_static(b"mhello"), &generation, &tx, &mut replies, &mut pending);

        let events = collect(&mut rx);
        assert_eq!(events.len(), 3, "expected head Data + Refresh + tail Data");
        let (head_gen, head) = match &events[0] {
            PtyEvent::Data(c) => (c.generation, c.data.clone()),
            other => panic!("expected head Data, got {other:?}"),
        };
        let refresh_gen = match &events[1] {
            PtyEvent::Refresh(r) => r.generation,
            other => panic!("expected Refresh, got {other:?}"),
        };
        let (tail_gen, tail) = match &events[2] {
            PtyEvent::Data(c) => (c.generation, c.data.clone()),
            other => panic!("expected tail Data, got {other:?}"),
        };

        assert_eq!(&head[..], b"m", "head completes the open sequence");
        assert_eq!(&tail[..], b"hello", "tail is the clean remainder");
        let mut joined = head.to_vec();
        joined.extend_from_slice(&tail);
        assert_eq!(joined, b"mhello", "head ++ tail reconstructs the read exactly");
        assert!(
            head_gen < refresh_gen && refresh_gen < tail_gen,
            "snapshot must sit between head and tail: head={head_gen} refresh={refresh_gen} tail={tail_gen}"
        );
        assert!(!pending, "the pending refresh was flushed at the boundary");
    }

    // When the entire read sits inside one unfinished sequence there is no boundary
    // to split on: broadcast it whole and keep the refresh pending for a later read.
    #[test]
    fn read_entirely_inside_sequence_emits_whole_and_keeps_pending() {
        let mut t = make_terminal();
        t.vt_write(b"\x1b]0;tit"); // mid-OSC, no terminator
        assert!(!t.vt_at_boundary());

        let (tx, mut rx) = broadcast::channel(16);
        let generation = AtomicU64::new(0);
        let mut replies = Vec::new();
        let mut pending = true;

        // More OSC body, still no ST/BEL — the read never reaches ground.
        process_read(&mut t, Bytes::from_static(b"le-more"), &generation, &tx, &mut replies, &mut pending);

        let events = collect(&mut rx);
        assert_eq!(events.len(), 1, "no boundary => single whole-read Data, no Refresh");
        match &events[0] {
            PtyEvent::Data(c) => assert_eq!(&c.data[..], b"le-more"),
            other => panic!("expected Data, got {other:?}"),
        }
        assert!(pending, "refresh stays pending until a boundary arrives");
    }

    // The boundary can fall at the very end of the read, leaving an empty tail —
    // we emit head + Refresh and skip the empty tail Data.
    #[test]
    fn split_with_empty_tail_emits_head_and_refresh_only() {
        let mut t = make_terminal();
        t.vt_write(b"\x1b[31"); // mid-SGR
        let (tx, mut rx) = broadcast::channel(16);
        let generation = AtomicU64::new(0);
        let mut replies = Vec::new();
        let mut pending = true;

        // 'm' completes the SGR and the read ends exactly at the boundary.
        process_read(&mut t, Bytes::from_static(b"m"), &generation, &tx, &mut replies, &mut pending);

        let events = collect(&mut rx);
        assert_eq!(events.len(), 2, "head + Refresh only; no empty tail Data");
        assert!(matches!(events[0], PtyEvent::Data(_)), "first event is head Data");
        assert!(matches!(events[1], PtyEvent::Refresh(_)), "second event is Refresh");
        assert!(!pending, "the pending refresh was flushed at the boundary");
    }

    // The reader's parser must mirror the real byte stream exactly, because
    // vt_at_boundary() (used to choose split points) reads that parser state. When a
    // split happens, the tail must still be applied to the terminal — otherwise the
    // reader desyncs from the stream and later reports false boundaries, pinning a
    // refresh mid-sequence and resuming a new attacher on an orphaned escape tail.
    #[test]
    fn split_applies_tail_to_terminal_so_parser_stays_in_sync() {
        let mut t = make_terminal();
        t.vt_write(b"\x1b[31"); // leave the parser mid-SGR
        assert!(!t.vt_at_boundary());

        let (tx, _rx) = broadcast::channel(16);
        let generation = AtomicU64::new(0);
        let mut replies = Vec::new();
        let mut pending = true;

        // 'm' completes the SGR (first boundary); the tail "hello\x1b[1" ends mid-CSI.
        process_read(&mut t, Bytes::from_static(b"mhello\x1b[1"), &generation, &tx, &mut replies, &mut pending);

        // The real stream is now mid-CSI, so the reader's parser must report the same.
        // If the tail was dropped, the parser sits at ground after 'm' and lies here.
        assert!(
            !t.vt_at_boundary(),
            "tail must be applied to the terminal; parser should still be mid-sequence"
        );
    }

    // The stall-timeout flush stamps degraded=true on both the on-demand reply and
    // the broadcast refresh; a boundary-clean flush leaves it false. This is the only
    // signal the client gets that a snapshot was forced out mid-sequence.
    #[test]
    fn flush_refreshes_marks_degraded_only_on_the_stall_path() {
        let t = make_terminal();
        let (tx, mut rx) = broadcast::channel(16);
        let generation = AtomicU64::new(0);

        // Stall flush: a waiting reply + a pending broadcast, degraded=true.
        let (reply_tx, mut reply_rx) = oneshot::channel();
        let mut replies = vec![reply_tx];
        let mut pending = true;
        flush_refreshes(&t, &generation, &tx, &mut replies, &mut pending, true);

        let reply = reply_rx.try_recv().expect("reply not sent").expect("refresh failed");
        assert!(reply.degraded, "stall reply must be marked degraded");
        match rx.try_recv().expect("no broadcast refresh") {
            PtyEvent::Refresh(r) => assert!(r.degraded, "stall broadcast must be degraded"),
            other => panic!("expected Refresh, got {other:?}"),
        }

        // Boundary-clean flush: degraded=false.
        let (reply_tx, mut reply_rx) = oneshot::channel();
        let mut replies = vec![reply_tx];
        let mut pending = true;
        flush_refreshes(&t, &generation, &tx, &mut replies, &mut pending, false);

        let reply = reply_rx.try_recv().expect("reply not sent").expect("refresh failed");
        assert!(!reply.degraded, "clean reply must not be degraded");
        match rx.try_recv().expect("no broadcast refresh") {
            PtyEvent::Refresh(r) => assert!(!r.degraded, "clean broadcast must not be degraded"),
            other => panic!("expected Refresh, got {other:?}"),
        }
    }
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

