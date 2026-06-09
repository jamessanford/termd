use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    os::fd::OwnedFd,
    os::unix::io::AsRawFd,
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::SystemTime,
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use libghostty_vt::{Terminal, TerminalOptions};
use libghostty_vt::screen::Screen;
use libghostty_vt::screen::TrackedGridRef;
use tokio::sync::{broadcast, oneshot};

use super::snapshot::{do_refresh, do_scrollback};
use super::{
    MetadataReason, PtyChunk, PtyEvent, PtyInfo, PtyMetadata, PtyShared,
    RefreshData, ScrollbackData, ScrollbackOp,
};

/// One request to the reader thread. Sent on an unbounded std mpsc channel
/// (sends from the tokio thread never block), followed by a one-byte write to
/// the wakeup pipe so a poll()-parked reader services it promptly. Requests
/// are handled strictly in channel order.
pub(crate) enum ReaderRequest {
    /// PTY input. Fire-and-forget: the reader writes it through, buffering the
    /// unwritten tail on EAGAIN and draining it via POLLOUT.
    Write(Bytes),
    Resize {
        cols: u32,
        rows: u32,
        reply: oneshot::Sender<Result<()>>,
    },
    Refresh {
        reply: oneshot::Sender<Result<RefreshData>>,
    },
    Scrollback {
        subscriber_id: String,
        op: ScrollbackOp,
        amount: i32,
        row_count: u32,
        reply: oneshot::Sender<Result<ScrollbackData>>,
    },
}

/// Milliseconds to wait when a refresh is deferred but the parser is stuck
/// mid-sequence (the app wrote a partial escape sequence then went idle). Bounds
/// how long an attach/refresh can block before we give up and snapshot anyway.
const REFRESH_STALL_TIMEOUT_MS: libc::c_int = 1000;

/// Hard cap on buffered PTY input (~16x the kernel input queue). Past this the
/// child has stopped reading; dropping new input with a warning beats unbounded
/// growth — the loss now at least shows up in the logs, unlike the old silent
/// EAGAIN drop.
const MAX_PENDING_INPUT: usize = 1 << 20;

/// Write as much as the PTY accepts without blocking. Ok(n) may be short of
/// data.len() — that's EAGAIN, and the caller buffers the tail. Err is a real
/// error (PTY torn down), never WouldBlock.
fn write_some(master: &File, data: &[u8]) -> std::io::Result<usize> {
    use std::io::Write;
    let mut written = 0;
    while written < data.len() {
        match (&*master).write(&data[written..]) {
            Ok(0) => break,
            Ok(n) => written += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(written)
}

/// Queue PTY input: write immediately when nothing is pending (preserving
/// order), buffer the unwritten tail for the POLLOUT path.
fn queue_write(master: &File, pending_out: &mut Vec<u8>, data: &[u8], cap: usize) {
    let mut tail: &[u8] = data;
    if pending_out.is_empty() {
        match write_some(master, data) {
            Ok(n) => tail = &data[n..],
            Err(e) => {
                tracing::debug!("PTY write failed: {e}");
                return;
            }
        }
    }
    if tail.is_empty() {
        return;
    }
    if pending_out.len() + tail.len() > cap {
        tracing::warn!(
            pending = pending_out.len(),
            dropped = tail.len(),
            "PTY input buffer full; dropping write"
        );
        return;
    }
    pending_out.extend_from_slice(tail);
}

/// Drain as much pending input as the PTY accepts. On a real error the
/// remainder is discarded — the PTY is gone and the read side will surface
/// the same failure to the main loop.
fn flush_pending(master: &File, pending_out: &mut Vec<u8>) {
    match write_some(master, pending_out) {
        Ok(n) => { pending_out.drain(..n); }
        Err(e) => {
            tracing::debug!("PTY write failed: {e}");
            pending_out.clear();
        }
    }
}

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
    pub(super) fn closed(id: u64, created_at: SystemTime) -> Self {
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

/// Owns everything single-threaded about a PTY: the libghostty Terminal, the
/// master fd, scrollback pins, and the deferred-refresh state. Constructed and
/// driven on the dedicated pty-reader thread (Terminal is !Send).
pub(super) struct Reader {
    terminal: Terminal<'static, 'static>,
    master: File,
    wakeup_read: OwnedFd,
    req_rx: std::sync::mpsc::Receiver<ReaderRequest>,
    tx: broadcast::Sender<PtyEvent>,
    meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
    shared: Arc<PtyShared>,
    child: std::process::Child,
    // One scrollback pin per subscriber; the pin marks the viewport's top row and
    // libghostty keeps it on its content across appends and eviction.
    pins: HashMap<String, TrackedGridRef>,
    // PTY input the kernel wouldn't accept yet (EAGAIN); drained via POLLOUT.
    pending_out: Vec<u8>,
    // Refreshes are deferred until the VT parser is at a ground boundary, so a
    // snapshot never pins a generation in the middle of an escape sequence (which
    // would leave an attaching client resuming on an orphaned sequence tail). The
    // live broadcast path is untouched — only refresh emission waits for ground.
    pending_replies: Vec<oneshot::Sender<Result<RefreshData>>>,
    pending_broadcast_refresh: bool,
    prev_title: String,
    prev_screen: Screen,
    // Set by cleanup() on a normal exit; Drop reports it (None after a panic).
    exit_code: Option<i32>,
}

impl Reader {
    pub(super) fn new(
        master: File,
        wakeup_read: OwnedFd,
        req_rx: std::sync::mpsc::Receiver<ReaderRequest>,
        tx: broadcast::Sender<PtyEvent>,
        meta_tx: broadcast::Sender<Arc<PtyMetadata>>,
        shared: Arc<PtyShared>,
        child: std::process::Child,
    ) -> Result<Self> {
        let mut terminal = Terminal::new(TerminalOptions {
            cols: shared.cols.load(Ordering::Relaxed) as u16,
            rows: shared.rows.load(Ordering::Relaxed) as u16,
            // Byte budget for scrollback page memory, NOT a line count. libghostty
            // allocates whole ~0.5 MB pages (a page is sized for a 215x215 grid; each
            // Cell is 8 bytes), and every row costs cols*8 bytes regardless of how few
            // glyphs it holds. Rough rule at 80 cols: ~2 KB/line, so 16 MB ≈ ~8k lines
            // (proportionally fewer on wider terminals).
            max_scrollback: 16_000_000,
        })?;
        let shared_cb = shared.clone();
        terminal.on_title_changed(move |term| {
            if let Ok(t) = term.title() {
                *shared_cb.title.lock().unwrap() = t.to_string();
            }
        })?;
        // Initialize prev_title to match the initial title mutex value (pts_name),
        // so we don't emit a spurious TitleChanged before the shell sets any title.
        let prev_title = shared.pts_name.clone();
        Ok(Self {
            terminal,
            master,
            wakeup_read,
            req_rx,
            tx,
            meta_tx,
            shared,
            child,
            pins: HashMap::new(),
            pending_out: Vec::new(),
            pending_replies: Vec::new(),
            pending_broadcast_refresh: false,
            prev_title,
            prev_screen: Screen::Primary,
            exit_code: None,
        })
    }

    pub(super) fn run(mut self) {
        // master_fd is only valid as long as self.master (the owning File) is alive
        let master_fd = self.master.as_raw_fd();
        let wakeup_fd = self.wakeup_read.as_raw_fd();
        let mut buf = [0u8; 4096];

        'main: loop {
            // Drain the wakeup pipe, then queued requests, before waiting for PTY data.
            let mut wake_byte = [0u8; 64];
            unsafe { libc::read(wakeup_fd, wake_byte.as_mut_ptr() as *mut libc::c_void, wake_byte.len()) };
            while let Ok(req) = self.req_rx.try_recv() {
                self.handle_request(req);
            }
            self.flush_refreshes_if_at_boundary();
            if !self.pending_out.is_empty() {
                flush_pending(&self.master, &mut self.pending_out);
            }

            // Poll both the PTY master and the wakeup pipe.  Writes to wakeup_write
            // (any queued request) unblock the poll so requests are handled promptly.
            // While PTY input is buffered, also watch for the master becoming writable.
            let mut master_events = libc::POLLIN;
            if !self.pending_out.is_empty() {
                master_events |= libc::POLLOUT;
            }
            let mut pfds = [
                libc::pollfd { fd: master_fd, events: master_events, revents: 0 },
                libc::pollfd { fd: wakeup_fd, events: libc::POLLIN, revents: 0 },
            ];
            // Block indefinitely unless a refresh is waiting behind an unfinished
            // sequence (it didn't flush above, so the parser is mid-sequence) — then
            // cap the wait so an idle mid-sequence PTY can't stall the attach forever.
            let refresh_pending = !self.pending_replies.is_empty() || self.pending_broadcast_refresh;
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
                flush_refreshes(
                    &self.terminal, &self.shared.generation, &self.tx,
                    &mut self.pending_replies, &mut self.pending_broadcast_refresh, true,
                );
                continue;
            }

            // Master writable again: drain buffered PTY input.
            if pfds[0].revents & libc::POLLOUT != 0 {
                flush_pending(&self.master, &mut self.pending_out);
            }

            // Only read from PTY master if it actually has data ready.
            // poll_ret > 0 may be due to the wakeup pipe alone; reading master
            // when it is not ready would block indefinitely.
            if pfds[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
                // Only the wakeup pipe fired — loop back to handle queued requests.
                continue;
            }

            // Data available (or HUP/ERR) on PTY master — drain all buffered bytes.
            let mut batch: Vec<u8> = Vec::new();
            loop {
                match self.master.read(&mut buf) {
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
                &mut self.terminal, Bytes::from(batch), &self.shared.generation, &self.tx,
                &mut self.pending_replies, &mut self.pending_broadcast_refresh,
            );

            // Screen / title state is read after the full read has been applied.
            let current_screen = self.terminal.active_screen().unwrap_or(Screen::Primary);
            if current_screen != self.prev_screen {
                self.prev_screen = current_screen;
                // Defer the new-screen redraw to a ground boundary (serviced below).
                self.pending_broadcast_refresh = true;
            }
            let current_title = self.shared.title.lock().unwrap().clone();
            if current_title != self.prev_title {
                self.prev_title = current_title;
                let _ = self.meta_tx.send(Arc::new(PtyMetadata {
                    reason: MetadataReason::TitleChanged,
                    exit_code: None,
                    generation: gen,
                    info: self.shared.info(),
                }));
            }
            // A refresh can have become pending *this* round (a screen switch above), and
            // process_read only splits for refreshes that were already pending on entry.
            // If the read left the parser at ground, service that fresh refresh now;
            // otherwise it waits for the read that completes the open sequence.
            self.flush_refreshes_if_at_boundary();
        }

        self.cleanup();
        // `self` drops here: Drop emits the Closed metadata.
    }

    /// If the parser is at a ground boundary, service deferred refreshes now;
    /// otherwise they wait for the batch that completes the open sequence.
    fn flush_refreshes_if_at_boundary(&mut self) {
        if (!self.pending_replies.is_empty() || self.pending_broadcast_refresh)
            && self.terminal.vt_at_boundary()
        {
            flush_refreshes(
                &self.terminal, &self.shared.generation, &self.tx,
                &mut self.pending_replies, &mut self.pending_broadcast_refresh, false,
            );
        }
    }

    fn handle_request(&mut self, req: ReaderRequest) {
        match req {
            ReaderRequest::Write(data) => {
                queue_write(&self.master, &mut self.pending_out, &data, MAX_PENDING_INPUT);
            }
            ReaderRequest::Resize { cols, rows, reply } => {
                let _ = reply.send(self.apply_resize(cols, rows));
            }
            ReaderRequest::Refresh { reply } => {
                // Queued; serviced only at a VT ground boundary (or stall timeout).
                self.pending_replies.push(reply);
            }
            ReaderRequest::Scrollback { subscriber_id, op, amount, row_count, reply } => {
                let gen = self.shared.generation.load(Ordering::Relaxed);
                let cols = self.shared.cols.load(Ordering::Relaxed);
                let _ = reply.send(do_scrollback(
                    &mut self.terminal, &mut self.pins, &subscriber_id,
                    op, amount, row_count, gen, cols,
                ));
            }
        }
    }

    /// Kernel resize, VT resize, shared-dims update, and the Resize broadcast all
    /// happen here, on one thread, in one order — there is no window where the
    /// libghostty Terminal disagrees with the real PTY size.
    fn apply_resize(&mut self, cols: u32, rows: u32) -> Result<()> {
        use nix::pty::Winsize;
        let ws = Winsize {
            ws_col: cols as u16,
            ws_row: rows as u16,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let ret = unsafe { libc::ioctl(self.master.as_raw_fd(), libc::TIOCSWINSZ, &ws as *const Winsize) };
        if ret < 0 {
            return Err(anyhow!("TIOCSWINSZ failed: {}", std::io::Error::last_os_error()));
        }
        self.terminal.resize(cols as u16, rows as u16, 0, 0)?;
        self.shared.cols.store(cols, Ordering::Relaxed);
        self.shared.rows.store(rows, Ordering::Relaxed);
        // Defer the redraw to a ground boundary (serviced by the run loop).
        self.pending_broadcast_refresh = true;
        let _ = self.meta_tx.send(Arc::new(PtyMetadata {
            reason: MetadataReason::Resize,
            exit_code: None,
            generation: self.shared.generation.load(Ordering::Relaxed),
            info: self.shared.info(),
        }));
        Ok(())
    }

    /// Post-loop teardown: reap the child, announce the exit on the data stream,
    /// then serve everything still queued against the final terminal state.
    fn cleanup(&mut self) {
        let status = self.child.try_wait().ok().flatten().or_else(|| self.child.wait().ok());
        crate::utmp::remove_record(self.master.as_raw_fd());
        let exit_msg = {
            let title = self.shared.title.lock().unwrap().clone();
            match &status {
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
        let gen = self.shared.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.tx.send(PtyEvent::Data(PtyChunk {
            generation: gen,
            data: Bytes::from(exit_msg.into_bytes()),
        }));
        self.exit_code = status.as_ref().and_then(|s| s.code());

        // Refreshes deferred at exit: the terminal is final, render directly
        // without waiting for a boundary.
        for reply in self.pending_replies.drain(..) {
            let gen = self.shared.generation.load(Ordering::Relaxed);
            let _ = reply.send(do_refresh(&self.terminal, gen));
        }
        // One drain for everything still queued — replaces the per-channel loops.
        while let Ok(req) = self.req_rx.try_recv() {
            let gen = self.shared.generation.load(Ordering::Relaxed);
            match req {
                ReaderRequest::Refresh { reply } => {
                    let _ = reply.send(do_refresh(&self.terminal, gen));
                }
                ReaderRequest::Scrollback { subscriber_id, op, amount, row_count, reply } => {
                    let cols = self.shared.cols.load(Ordering::Relaxed);
                    let _ = reply.send(do_scrollback(
                        &mut self.terminal, &mut self.pins, &subscriber_id,
                        op, amount, row_count, gen, cols,
                    ));
                }
                ReaderRequest::Resize { reply, .. } => {
                    let _ = reply.send(Err(anyhow!("PTY closed")));
                }
                ReaderRequest::Write(_) => {} // child is gone; input has nowhere to go
            }
        }
    }
}

/// Sole emitter of the Closed metadata — fires on a clean return from run()
/// (after cleanup() set exit_code) and on an unwind (exit_code None), so a
/// panicking reader still tells attached clients the PTY is gone (they key off
/// StreamMetadataReason::Closed to detach) instead of leaving them hung.
/// Replaces the old ClosedNotifier mirror struct. Deliberately touches neither
/// the libghostty Terminal (unsafe mid-panic) nor utmp (a leaked record on a
/// reader panic is tolerated; the normal path removes it in cleanup()).
impl Drop for Reader {
    fn drop(&mut self) {
        let _ = self.meta_tx.send(Arc::new(PtyMetadata {
            reason:     MetadataReason::Closed,
            exit_code:  self.exit_code,
            generation: self.shared.generation.load(Ordering::Relaxed),
            info:       PtyInfo::closed(self.shared.id, self.shared.created_at),
        }));
    }
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
mod write_buffer_tests {
    use super::*;
    use std::io::Read;

    // Non-blocking pipe pair standing in for the PTY master: identical EAGAIN
    // semantics when the kernel buffer fills.
    fn pipe_pair() -> (File, File) {
        let (r, w) = super::super::wakeup_pipe().unwrap();
        (File::from(r), File::from(w))
    }

    fn drain(read_end: &mut File) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match read_end.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&buf[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("pipe read failed: {e}"),
            }
        }
        out
    }

    #[test]
    fn queue_write_writes_directly_when_buffer_empty() {
        let (mut r, w) = pipe_pair();
        let mut pending = Vec::new();
        queue_write(&w, &mut pending, b"hello", MAX_PENDING_INPUT);
        assert!(pending.is_empty(), "small write must go straight through");
        assert_eq!(drain(&mut r), b"hello");
    }

    #[test]
    fn queue_write_buffers_tail_on_full_pipe_and_flush_preserves_order() {
        let (mut r, w) = pipe_pair();
        let mut pending = Vec::new();
        // Fill the pipe until a tail lands in the buffer.
        let chunk = [b'a'; 4096];
        while pending.is_empty() {
            queue_write(&w, &mut pending, &chunk, MAX_PENDING_INPUT);
        }
        // With bytes pending, later writes append instead of jumping the queue.
        queue_write(&w, &mut pending, b"WORLD", MAX_PENDING_INPUT);
        assert!(pending.ends_with(b"WORLD"));

        // Drain the pipe, flush, repeat: every byte arrives, in order.
        let mut received = drain(&mut r);
        while !pending.is_empty() {
            flush_pending(&w, &mut pending);
            received.extend_from_slice(&drain(&mut r));
        }
        assert!(received.ends_with(b"WORLD"), "buffered tail must arrive last");
        let body = &received[..received.len() - 5];
        assert!(body.iter().all(|&b| b == b'a'), "no reordering or corruption");
    }

    #[test]
    fn queue_write_drops_beyond_cap_keeps_buffer_intact() {
        let (_r, w) = pipe_pair();
        let mut pending = vec![b'x'; 10]; // non-empty => no direct-write path
        queue_write(&w, &mut pending, b"toolarge", 12); // 10 + 8 > 12: dropped
        assert_eq!(pending.len(), 10);
        queue_write(&w, &mut pending, b"ok", 12);       // 10 + 2 <= 12: appended
        assert_eq!(pending.len(), 12);
        assert!(pending.ends_with(b"ok"));
    }
}
