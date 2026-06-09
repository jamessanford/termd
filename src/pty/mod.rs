use std::{
    collections::{HashMap, HashSet},
    fs::File,
    os::unix::io::{AsRawFd, FromRawFd},
    os::fd::OwnedFd,
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

mod reader;
mod snapshot;

use reader::{Reader, ReaderRequest};

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
    /// Synthesized per-subscriber by the forwarding task on broadcast lag —
    /// never emitted by the reader thread.
    DataLost,
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

/// State shared between the PtyHandle (tokio side) and the reader thread.
/// Everything here is cheap to read concurrently; the libghostty Terminal and
/// the master fd deliberately do NOT live here — they are exclusive to the
/// reader thread.
pub(crate) struct PtyShared {
    pub(crate) id:                 u64,
    pub(crate) hostname:           String,
    pub(crate) pts_name:           String,
    pub(crate) created_at:         SystemTime,
    pub(crate) cols:               AtomicU32,
    pub(crate) rows:               AtomicU32,
    pub(crate) title:              Mutex<String>,
    pub(crate) generation:         AtomicU64,
    pub(crate) subscribers:        RwLock<HashMap<String, SubscriberInfo>>,
    pub(crate) last_subscribed_at: Mutex<Option<SystemTime>>,
    // Assigned once at registration (next available slot, 0-based) for stable
    // list ordering; never changes for the life of the handle.
    pub(crate) sort_order:         AtomicU32,
}

impl PtyShared {
    pub(crate) fn info(&self) -> PtyInfo {
        let subscribers = {
            let map = self.subscribers.read().unwrap();
            let mut v: Vec<(String, SubscriberInfo)> =
                map.iter().map(|(id, s)| (id.clone(), s.clone())).collect();
            v.sort_by_key(|(_, s)| s.created_at);
            v
        };  // read-lock released here
        PtyInfo {
            id:                 self.id,
            hostname:           self.hostname.clone(),
            pts_name:           self.pts_name.clone(),
            cols:               self.cols.load(Ordering::Relaxed),
            rows:               self.rows.load(Ordering::Relaxed),
            title:              self.title.lock().unwrap().clone(),
            created_at:         self.created_at,
            last_subscribed_at: *self.last_subscribed_at.lock().unwrap(),
            subscribers:        Some(subscribers),
            sort_order:         self.sort_order.load(Ordering::Relaxed),
        }
    }
}

pub struct PtyHandle {
    shared: Arc<PtyShared>,
    tx: broadcast::Sender<PtyEvent>,
    req_tx: std::sync::mpsc::Sender<ReaderRequest>,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    child_pid: Pid,
    wakeup_write: OwnedFd,
}

impl PtyHandle {
    pub fn info(&self) -> PtyInfo {
        self.shared.info()
    }

    pub(crate) fn shared(&self) -> Arc<PtyShared> {
        self.shared.clone()
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

    /// Queue input for the PTY. Fire-and-forget: the reader thread delivers it
    /// (buffering on EAGAIN); Err only means the reader is gone, which clients
    /// also learn via the Closed metadata.
    pub fn write(&self, data: &[u8]) -> Result<()> {
        self.req_tx
            .send(ReaderRequest::Write(Bytes::copy_from_slice(data)))
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
        Ok(())
    }

    /// One-byte poke so a poll()-parked reader notices a queued request promptly.
    fn wake(&self) {
        let wfd = self.wakeup_write.as_raw_fd();
        let ret = unsafe { libc::write(wfd, [1u8].as_ptr() as *const libc::c_void, 1) };
        if ret < 0 {
            tracing::debug!("wakeup write failed: {}", std::io::Error::last_os_error());
        }
    }

    fn request_resize(&self, cols: u32, rows: u32) -> Result<oneshot::Receiver<Result<()>>> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReaderRequest::Resize { cols, rows, reply: tx })
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
        Ok(rx)
    }

    /// Resize the PTY. Executed on the reader thread (kernel ioctl, VT resize,
    /// and the Resize metadata broadcast happen there, in order); this awaits
    /// the reader's acknowledgement.
    pub async fn resize(&self, cols: u32, rows: u32) -> Result<()> {
        let rx = self.request_resize(cols, rows)?;
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped resize response"))?
    }

    pub fn set_title(&self, title: &str) {
        *self.shared.title.lock().unwrap() = title.to_string();
    }

    pub async fn refresh(&self) -> Result<RefreshData> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReaderRequest::Refresh { reply: tx })
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
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
        self.req_tx
            .send(ReaderRequest::Scrollback {
                subscriber_id: subscriber_id.to_owned(), op, amount, row_count, reply: tx,
            })
            .map_err(|_| anyhow!("PTY reader thread is dead"))?;
        self.wake();
        rx.await.map_err(|_| anyhow!("PTY reader thread dropped scrollback response"))?
    }

    /// Best-effort release of a subscriber's scrollback pin (teardown paths).
    pub fn close_scrollback(&self, subscriber_id: &str) {
        let (tx, _rx) = oneshot::channel();
        let _ = self.req_tx.send(ReaderRequest::Scrollback {
            subscriber_id: subscriber_id.to_owned(),
            op: ScrollbackOp::Close, amount: 0, row_count: 0, reply: tx,
        });
        self.wake();
    }

    pub fn id(&self) -> u64 {
        self.shared.id
    }

    pub fn current_generation(&self) -> u64 {
        self.shared.generation.load(Ordering::Relaxed)
    }

    pub fn touch_last_subscribed(&self) {
        *self.shared.last_subscribed_at.lock().unwrap() = Some(SystemTime::now());
    }

    pub fn upsert_subscriber(&self, subscriber_id: &str, info: SubscriberInfo) {
        let mut map = self.shared.subscribers.write().unwrap();
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
        self.shared.subscribers.write().unwrap().remove(subscriber_id);
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

        // Single master fd, owned by the reader thread. O_NONBLOCK so the reader
        // drains reads in a loop, and so PTY-input writes fail fast with EAGAIN
        // into the pending buffer instead of ever blocking the thread.
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error()).context("set O_NONBLOCK on master fd");
        }

        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};
        // dup slave so each Stdio owns its own fd; the child will have them as 0/1/2
        let slave_stdout = dup_fd(slave_fd.as_raw_fd()).context("dup slave fd for stdout")?;
        let slave_stderr = dup_fd(slave_fd.as_raw_fd()).context("dup slave fd for stderr")?;
        // Set FD_CLOEXEC on all master and slave fds so concurrent forks
        // don't inherit them. Rust's spawn dup2s them to 0/1/2 in the
        // child before exec, so the shell still gets them correctly.
        for fd in [master.as_raw_fd(), slave_fd.as_raw_fd(), slave_stdout.as_raw_fd(), slave_stderr.as_raw_fd()] {
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
        let (req_tx, req_rx) = std::sync::mpsc::channel::<ReaderRequest>();

        // Create wakeup pipe before spawning the child so that a failure here doesn't
        // leave a zombie process behind.  O_CLOEXEC and O_NONBLOCK are set atomically
        // (Linux) or via fcntl fallback (macOS) inside wakeup_pipe().
        let (wakeup_read, wakeup_write) = wakeup_pipe().context("wakeup pipe")?;

        let child = cmd.spawn().context("spawn shell")?;
        // slave fds are owned by the Stdio objects passed to Command and closed after fork;

        let child_pid = Pid::from_raw(child.id() as i32);
        crate::utmp::add_record(master.as_raw_fd(), &hostname);

        let shared = Arc::new(PtyShared {
            id,
            hostname,
            title: Mutex::new(pts_name.clone()),
            pts_name,
            created_at: SystemTime::now(),
            cols: AtomicU32::new(cols),
            rows: AtomicU32::new(rows),
            generation: AtomicU64::new(0),
            subscribers: RwLock::new(HashMap::new()),
            last_subscribed_at: Mutex::new(None),
            sort_order: AtomicU32::new(0), // real value assigned under the registry lock below
        });

        let handle = Arc::new(PtyHandle {
            shared: shared.clone(),
            tx: tx.clone(),
            req_tx,
            meta_tx: meta_tx.clone(),
            child_pid,
            wakeup_write,
        });

        // Spawn dedicated reader thread — owns libghostty state, the master fd,
        // and the child process
        let master = File::from(master);
        let meta_tx_spawn = meta_tx.clone();
        let shared_spawn = shared.clone();
        std::thread::Builder::new()
            .name(format!("pty-reader-{id:016x}"))
            .spawn(move || {
                match Reader::new(master, wakeup_read, req_rx, tx, meta_tx, shared, child) {
                    Ok(r) => r.run(),
                    Err(e) => {
                        // Startup failure: there is no Reader (and so no Drop) yet.
                        // Emit Closed ourselves so the PTY doesn't sit listed forever
                        // with a dead reader behind it.
                        tracing::warn!("PTY reader failed to start: {e}");
                        let _ = meta_tx_spawn.send(Arc::new(PtyMetadata {
                            reason:     MetadataReason::Closed,
                            exit_code:  None,
                            generation: 0,
                            info:       PtyInfo::closed(shared_spawn.id, shared_spawn.created_at),
                        }));
                    }
                }
            })
            .context("spawn reader thread")?;

        // Assign sort_order and insert atomically so concurrent creates can't
        // pick the same slot. Smallest unused 0-based value, reusing gaps left
        // by destroyed PTYs.
        {
            let mut map = self.ptys.write().unwrap();
            let used: HashSet<u32> =
                map.values().map(|h| h.shared.sort_order.load(Ordering::Relaxed)).collect();
            let order = (0u32..).find(|n| !used.contains(n)).unwrap();
            handle.shared.sort_order.store(order, Ordering::Relaxed);
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

    #[tokio::test]
    async fn resize_via_request_updates_state_and_broadcasts() {
        let reg = PtyRegistry::new();
        let h = reg.create(80, 24, None).unwrap();
        h.upsert_subscriber("sub-a", make_info("host1"));
        let mut meta_rx = h.meta_subscribe();

        h.resize(100, 30).await.unwrap();

        let info = h.info();
        assert_eq!((info.cols, info.rows), (100, 30));

        // The reader broadcast Resize metadata before replying, so it's queued.
        let meta = tokio::time::timeout(std::time::Duration::from_secs(5), meta_rx.recv())
            .await.expect("timed out").expect("meta channel closed");
        assert!(matches!(meta.reason, MetadataReason::Resize));
        assert_eq!((meta.info.cols, meta.info.rows), (100, 30));
        // Full info, not a fabricated stub: the subscriber map came through.
        assert_eq!(meta.info.subscribers.as_ref().unwrap().len(), 1);

        // The refresh path sees the new dimensions.
        let r = h.refresh().await.unwrap();
        assert_eq!((r.cols, r.rows), (100, 30));
        let _ = reg.destroy(h.id());
    }

    #[test]
    fn shared_info_reflects_title_and_dims() {
        let h = make_handle();
        h.set_title("new-title");
        let info = h.shared().info();
        assert_eq!(info.title, "new-title");
        assert_eq!((info.cols, info.rows), (80, 24));
        assert!(info.subscribers.is_some());
    }
}

