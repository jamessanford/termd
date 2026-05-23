# Attach Handler Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple gRPC stream reading from render modes by making the main loop a long-lived demux that dispatches typed `PtyEvent`s to `RenderModeHandler` trait implementations via direct callback.

**Architecture:** The main loop in `mod.rs::run()` permanently owns `resp_rx`, handles all pty_id filtering and generation checking in one place, and calls synchronous handler methods on the hot path. Render modes become stateful structs implementing a trait, rather than async functions with their own select loops. Control-plane operations (subscribe, refresh, list, create, destroy) remain as helper functions called by the main loop.

**Tech Stack:** Rust, tokio, tonic (gRPC), libghostty-vt

**Spec:** `docs/superpowers/specs/2026-05-23-attach-refactor-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/attach/mod.rs` | Modify | Add `PtyEvent`, `EventResult`, `RenderModeHandler` trait, `create_handler()`. Restructure `run()` to be the long-lived demux loop. Remove `RunContext`, simplify `RunOutcome`. |
| `src/attach/raw.rs` | Rewrite | `RawHandler` struct implementing `RenderModeHandler`. Remove async `run()`. |
| `src/attach/cell.rs` | Rewrite | `CellHandler` struct implementing `RenderModeHandler`. Remove async `run()`. Keep `render_dirty()` and its tests. |
| `src/attach/region.rs` | Rewrite | `RegionHandler` struct implementing `RenderModeHandler`. Remove async `run()`. Keep `VtFilter` and all its tests. |
| `src/attach/input.rs` | Unchanged | |
| `src/attach/scrollback.rs` | Unchanged | |

---

### Task 1: Define core types

**Files:**
- Modify: `src/attach/mod.rs`

- [ ] **Step 1: Add PtyEvent, EventResult, and RenderModeHandler trait to mod.rs**

Add these types after the existing `RenderMode` enum (around line 33):

```rust
pub(super) enum PtyEvent<'a> {
    Stream { gen: u64, data: &'a [u8] },
    Refresh { gen: u64, cols: u32, rows: u32, data: &'a [u8] },
    Resize { cols: u32, rows: u32 },
    Closed,
}

pub(super) enum EventResult {
    Continue,
    ChangeRenderMode(RenderMode),
    RequestRefresh,
}

pub(super) trait RenderModeHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> anyhow::Result<EventResult>;
    fn on_pty_event(&mut self, event: PtyEvent, out: &mut Vec<u8>) -> anyhow::Result<EventResult>;
    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> anyhow::Result<EventResult>;
    fn cleanup(&mut self, _out: &mut Vec<u8>) {}
}
```

- [ ] **Step 2: Add create_handler helper function**

Add after the trait definition:

```rust
fn create_handler(
    mode: RenderMode,
    server_cols: u32,
    server_rows: u32,
    allow_upgrade: bool,
) -> anyhow::Result<Box<dyn RenderModeHandler>> {
    Ok(match mode {
        RenderMode::Cell => Box::new(cell::CellHandler::new(server_cols, server_rows, allow_upgrade)?),
        RenderMode::Raw => Box::new(raw::RawHandler::new()),
        RenderMode::Region => {
            let (client_cols, client_rows) = get_terminal_size();
            Box::new(region::RegionHandler::new(server_rows, server_cols, client_rows, client_cols))
        }
    })
}
```

This will not compile yet — the handler structs don't exist. That's expected.

- [ ] **Step 3: Verify the types are syntactically correct**

Comment out `create_handler` temporarily (it references types that don't exist yet). Run:

```bash
cargo check 2>&1 | head -20
```

Expected: compiles with no errors related to PtyEvent/EventResult/RenderModeHandler (there may be "unused" warnings).

- [ ] **Step 4: Commit**

```bash
git add src/attach/mod.rs
git commit -m "Add PtyEvent, EventResult, and RenderModeHandler trait"
```

---

### Task 2: Implement RawHandler

**Files:**
- Rewrite: `src/attach/raw.rs`

The simplest handler — has no state at all. Just copies data to the output buffer.

- [ ] **Step 1: Write tests for RawHandler**

Replace the entire contents of `src/attach/raw.rs` with the handler implementation and tests. Start with the tests (the implementation will follow in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{PtyEvent, EventResult, RenderMode};

    #[test]
    fn init_writes_refresh_data() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.init(b"hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"hello");
    }

    #[test]
    fn init_replays_buffered_chunks() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let buffered = vec![(2, b"world".to_vec())];
        let result = h.init(b"hello", &buffered, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"helloworld");
    }

    #[test]
    fn stream_copies_data_to_output() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_pty_event(PtyEvent::Stream { gen: 1, data: b"test" }, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"test");
    }

    #[test]
    fn refresh_copies_data_to_output() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 5, cols: 80, rows: 24, data: b"screen" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"screen");
    }

    #[test]
    fn resize_clears_screen() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        h.on_pty_event(PtyEvent::Resize { cols: 80, rows: 24 }, &mut out).unwrap();
        assert_eq!(out, b"\x1b[2J");
    }

    #[test]
    fn sigwinch_requests_refresh() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_sigwinch(100, 40, &mut out).unwrap();
        assert!(matches!(result, EventResult::RequestRefresh));
        assert!(out.is_empty());
    }

    #[test]
    fn closed_is_noop() {
        let mut h = RawHandler::new();
        let mut out = Vec::new();
        let result = h.on_pty_event(PtyEvent::Closed, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(out.is_empty());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test --lib attach::raw -- 2>&1 | tail -10
```

Expected: compilation errors — `RawHandler` not defined yet.

- [ ] **Step 3: Write the RawHandler implementation**

Add above the tests in `src/attach/raw.rs`:

```rust
use anyhow::Result;

pub(super) struct RawHandler;

impl RawHandler {
    pub(super) fn new() -> Self {
        Self
    }
}

impl super::RenderModeHandler for RawHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        out.extend_from_slice(refresh_data);
        for (_gen, data) in buffered {
            out.extend_from_slice(data);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                out.extend_from_slice(data);
            }
            super::PtyEvent::Refresh { data, .. } => {
                out.extend_from_slice(data);
            }
            super::PtyEvent::Resize { .. } => {
                out.extend_from_slice(b"\x1b[2J");
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, _cols: u32, _rows: u32, _out: &mut Vec<u8>) -> Result<super::EventResult> {
        Ok(super::EventResult::RequestRefresh)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test --lib attach::raw -- -v 2>&1 | tail -15
```

Expected: all 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/attach/raw.rs
git commit -m "Implement RawHandler with RenderModeHandler trait"
```

---

### Task 3: Implement CellHandler

**Files:**
- Modify: `src/attach/cell.rs`

CellHandler wraps `LocalTerminal` and the cell-by-cell rendering logic. The existing `render_dirty()` function and its tests are kept as-is — CellHandler calls `render_dirty()` from its trait methods.

- [ ] **Step 1: Write tests for CellHandler**

Add these tests to the existing `#[cfg(test)] mod tests` block in `cell.rs`, alongside the existing `render_dirty` tests:

```rust
    use super::super::{PtyEvent, EventResult, RenderMode};

    #[test]
    fn cell_init_renders_content() {
        let mut h = CellHandler::new(80, 24, false).unwrap();
        let mut out = Vec::new();
        let result = h.init(b"Hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("Hello"), "init should render refresh data");
    }

    #[test]
    fn cell_init_replays_buffered() {
        let mut h = CellHandler::new(80, 24, false).unwrap();
        let mut out = Vec::new();
        let buffered = vec![(2, b"World".to_vec())];
        h.init(b"Hello", &buffered, &mut out).unwrap();
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("World"), "init should replay buffered chunks");
    }

    #[test]
    fn cell_stream_renders_dirty() {
        let mut h = CellHandler::new(80, 24, false).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(PtyEvent::Stream { gen: 1, data: b"Test" }, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(!out.is_empty(), "stream data should produce render output");
    }

    #[test]
    fn cell_refresh_resizes_and_renders() {
        let mut h = CellHandler::new(80, 24, false).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 2, cols: 100, rows: 30, data: b"New" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert!(!out.is_empty());
    }

    #[test]
    fn cell_refresh_upgrades_to_region_when_allowed() {
        let mut h = CellHandler::new(80, 24, true).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        // Refresh with dimensions that fit — should upgrade
        // Note: this test depends on the real terminal size via get_terminal_size().
        // If the test terminal is large enough (>=80x24), it will upgrade.
        // In CI with no terminal, get_terminal_size() returns (0,0) so this won't upgrade.
        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 2, cols: 80, rows: 24, data: b"content" },
            &mut out,
        ).unwrap();
        // We can't assert the exact result because it depends on the terminal size,
        // but we can assert it doesn't error.
        assert!(matches!(result, EventResult::Continue | EventResult::ChangeRenderMode(RenderMode::Region)));
    }

    #[test]
    fn cell_no_upgrade_when_not_allowed() {
        let mut h = CellHandler::new(80, 24, false).unwrap();
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 2, cols: 80, rows: 24, data: b"content" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
    }

    #[test]
    fn cell_sigwinch_re_renders() {
        let mut h = CellHandler::new(80, 24, false).unwrap();
        let mut out = Vec::new();
        h.init(b"Hello", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_sigwinch(120, 40, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        // force_full render produces output even when nothing is dirty
        assert!(!out.is_empty());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test --lib attach::cell -- 2>&1 | tail -10
```

Expected: compilation errors — `CellHandler` not defined yet.

- [ ] **Step 3: Write the CellHandler implementation**

Add the struct and trait impl above `render_dirty()` in `cell.rs`. Remove the old `use` imports that referenced gRPC/protobuf types (`terminal_response::Response`, `StreamMetadataReason`) and the old `pub(super) async fn run(...)`. Keep `render_dirty()` and its existing tests.

```rust
use std::io::Write as IoWrite;

use anyhow::Result;
use libghostty_vt::render::{Dirty, RowIterator, CellIterator};
use libghostty_vt::style::Underline;

pub(super) struct CellHandler {
    lt: super::LocalTerminal,
    allow_upgrade: bool,
    server_cols: u32,
    server_rows: u32,
}

impl CellHandler {
    pub(super) fn new(cols: u32, rows: u32, allow_upgrade: bool) -> Result<Self> {
        Ok(Self {
            lt: super::LocalTerminal::new(cols, rows)?,
            allow_upgrade,
            server_cols: cols,
            server_rows: rows,
        })
    }
}

impl super::RenderModeHandler for CellHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> Result<super::EventResult> {
        self.lt.terminal.vt_write(refresh_data);
        render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, true, out)?;
        for (_gen, data) in buffered {
            self.lt.terminal.vt_write(data);
            render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, false, out)?;
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                self.lt.terminal.vt_write(data);
                render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, false, out)?;
            }
            super::PtyEvent::Refresh { cols, rows, data, .. } => {
                self.server_cols = cols;
                self.server_rows = rows;
                let (client_cols, client_rows) = super::get_terminal_size();
                if self.allow_upgrade && super::server_fits_client(cols, rows, client_cols, client_rows) {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Region));
                }
                self.lt.resize(cols, rows)?;
                self.lt.terminal.vt_write(data);
                render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, true, out)?;
            }
            super::PtyEvent::Resize { cols, rows } => {
                self.server_cols = cols;
                self.server_rows = rows;
                let (client_cols, client_rows) = super::get_terminal_size();
                if self.allow_upgrade && super::server_fits_client(cols, rows, client_cols, client_rows) {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Region));
                }
                self.lt.resize(cols, rows)?;
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> Result<super::EventResult> {
        if self.allow_upgrade && super::server_fits_client(self.server_cols, self.server_rows, cols, rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Region));
        }
        render_dirty(&self.lt.terminal, &mut self.lt.render_state, &mut self.lt.row_iter, &mut self.lt.cell_iter, true, out)?;
        Ok(super::EventResult::Continue)
    }
}
```

Keep the existing `render_dirty()` function exactly as it is (lines 149–258 of the current file). Keep the existing `render_dirty` tests. Remove the old `pub(super) async fn run(...)` function and its gRPC-specific imports (`terminal_response::Response`, `StreamMetadataReason`, `tokio::io::AsyncWriteExt`, `tokio::signal::unix::*`).

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test --lib attach::cell -- -v 2>&1 | tail -20
```

Expected: all tests pass — both the new CellHandler tests and the existing `render_dirty` tests.

- [ ] **Step 5: Commit**

```bash
git add src/attach/cell.rs
git commit -m "Implement CellHandler with RenderModeHandler trait"
```

---

### Task 4: Implement RegionHandler

**Files:**
- Modify: `src/attach/region.rs`

RegionHandler wraps `VtFilter` and handles the DECSTBM scroll region logic. The `VtFilter` struct and all its tests are kept completely unchanged. The old `pub(super) async fn run(...)` and `LoopExit` enum are removed.

- [ ] **Step 1: Write tests for RegionHandler**

Add these tests to the existing `#[cfg(test)] mod tests` block in `region.rs`, alongside the existing `VtFilter` tests:

```rust
    use super::super::{PtyEvent, EventResult, RenderMode, RenderModeHandler};

    #[test]
    fn region_init_emits_setup_and_data() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        let result = h.init(b"hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;24r"), "should emit DECSTBM");
        assert!(s.contains("hello"), "should include refresh data");
    }

    #[test]
    fn region_init_too_small_returns_change_mode() {
        let mut h = RegionHandler::new(24, 80, 20, 60);
        let mut out = Vec::new();
        let result = h.init(b"hello", &[], &mut out).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_init_replays_buffered() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        let buffered = vec![(2, b"world".to_vec())];
        h.init(b"hello", &buffered, &mut out).unwrap();
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("hello"));
        assert!(s.contains("world"));
    }

    #[test]
    fn region_stream_filters_data() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(PtyEvent::Stream { gen: 1, data: b"test" }, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        assert_eq!(out, b"test");
    }

    #[test]
    fn region_refresh_updates_filter_and_re_emits_setup() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 2, cols: 80, rows: 30, data: b"new" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::Continue));
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;30r"), "should emit updated DECSTBM");
        assert!(s.contains("new"));
    }

    #[test]
    fn region_refresh_too_large_switches_to_cell() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Refresh { gen: 2, cols: 200, rows: 50, data: b"big" },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_resize_too_large_switches_to_cell() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_pty_event(
            PtyEvent::Resize { cols: 200, rows: 50 },
            &mut out,
        ).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_sigwinch_too_small_switches_to_cell() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_sigwinch(60, 20, &mut out).unwrap();
        assert!(matches!(result, EventResult::ChangeRenderMode(RenderMode::Cell)));
    }

    #[test]
    fn region_sigwinch_ok_updates_client_size() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        let result = h.on_sigwinch(200, 50, &mut out).unwrap();
        assert!(matches!(result, EventResult::Continue));
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[1;24r"), "should re-emit DECSTBM on resize");
    }

    #[test]
    fn region_cleanup_resets_margins() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        h.cleanup(&mut out);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\x1b[r"), "should reset DECSTBM");
    }

    #[test]
    fn region_cleanup_disables_declrmm_when_active() {
        let mut h = RegionHandler::new(24, 80, 40, 120);
        let mut out = Vec::new();
        h.init(b"", &[], &mut out).unwrap();
        out.clear();

        h.cleanup(&mut out);
        let s = String::from_utf8_lossy(&out);
        // client_cols (120) > server_cols (80) so DECLRMM was enabled during init
        assert!(s.contains("\x1b[?69l"), "should disable DECLRMM");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test --lib attach::region -- 2>&1 | tail -10
```

Expected: compilation errors — `RegionHandler` not defined yet.

- [ ] **Step 3: Write the RegionHandler implementation**

Add the struct and trait impl between `VtFilter` and the old `run()` function. Remove the old `pub(super) async fn run(...)` function and the `LoopExit` enum. Remove gRPC-specific imports (`terminal_response::Response`, `StreamMetadataReason`, `tokio::io::AsyncWriteExt`, `tokio::signal::unix::*`). Keep `VtFilter` and all its internals exactly as they are.

```rust
pub(super) struct RegionHandler {
    filter: VtFilter,
}

impl RegionHandler {
    pub(super) fn new(server_rows: u32, server_cols: u32, client_rows: u32, client_cols: u32) -> Self {
        Self {
            filter: VtFilter::new(server_rows, server_cols, client_rows, client_cols),
        }
    }
}

impl super::RenderModeHandler for RegionHandler {
    fn init(&mut self, refresh_data: &[u8], buffered: &[(u64, Vec<u8>)], out: &mut Vec<u8>) -> anyhow::Result<super::EventResult> {
        if !super::server_fits_client(self.filter.server_cols, self.filter.server_rows, self.filter.client_cols, self.filter.client_rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        self.filter.emit_region_setup(out);
        self.filter.filter(refresh_data, out);
        for (_gen, data) in buffered {
            self.filter.filter(data, out);
        }
        Ok(super::EventResult::Continue)
    }

    fn on_pty_event(&mut self, event: super::PtyEvent, out: &mut Vec<u8>) -> anyhow::Result<super::EventResult> {
        match event {
            super::PtyEvent::Stream { data, .. } => {
                self.filter.filter(data, out);
            }
            super::PtyEvent::Refresh { cols, rows, data, .. } => {
                if !super::server_fits_client(cols, rows, self.filter.client_cols, self.filter.client_rows) {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                self.filter.update_region(rows, cols);
                self.filter.emit_region_setup(out);
                self.filter.filter(data, out);
            }
            super::PtyEvent::Resize { cols, rows } => {
                if !super::server_fits_client(cols, rows, self.filter.client_cols, self.filter.client_rows) {
                    return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
                }
                self.filter.update_region(rows, cols);
                self.filter.emit_region_setup(out);
            }
            super::PtyEvent::Closed => {}
        }
        Ok(super::EventResult::Continue)
    }

    fn on_sigwinch(&mut self, cols: u32, rows: u32, out: &mut Vec<u8>) -> anyhow::Result<super::EventResult> {
        if !super::server_fits_client(self.filter.server_cols, self.filter.server_rows, cols, rows) {
            return Ok(super::EventResult::ChangeRenderMode(super::RenderMode::Cell));
        }
        self.filter.update_client_size(rows, cols);
        self.filter.emit_region_setup(out);
        Ok(super::EventResult::Continue)
    }

    fn cleanup(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"\x1b[r");
        if self.filter.declrmm_active {
            out.extend_from_slice(b"\x1b[?69l");
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test --lib attach::region -- -v 2>&1 | tail -30
```

Expected: all tests pass — both the new RegionHandler tests and the existing VtFilter tests.

- [ ] **Step 5: Commit**

```bash
git add src/attach/region.rs
git commit -m "Implement RegionHandler with RenderModeHandler trait"
```

---

### Task 5: Restructure mod.rs main loop

**Files:**
- Modify: `src/attach/mod.rs`

This is the core refactor: restructure `run()` so that `resp_rx` never leaves the function, all pty_id filtering and generation checking happen in one place, and render modes are dispatched via the handler trait.

- [ ] **Step 1: Remove RunContext and simplify RunOutcome**

Delete the `RunContext` struct. Change `RunOutcome` to:

```rust
enum RunOutcome {
    ServerClosed,
    Action(InputAction),
}
```

This is now private to `run()` — remove the `pub(super)` visibility.

- [ ] **Step 2: Uncomment create_handler**

Uncomment the `create_handler` function added in Task 1 (now that all handler types exist).

- [ ] **Step 3: Rewrite run() — setup and session loop structure**

Replace the body of `run()` with the new structure. The signature stays the same. Key changes:

- `resp_rx` stays in `run()` for the entire session lifetime
- `current_refresh_gen` and `pty_closed` tracked in the main loop
- Handler created via `create_handler()`, initialized with `handler.init()`
- A `change_mode` variable after the select handles all `ChangeRenderMode` results in one place

```rust
pub async fn run(
    client: &mut AuthedClient,
    item: PtyItem,
    debug: bool,
    mode: RenderMode,
) -> Result<()> {
    if debug {
        return run_debug(client, item.pty_id).await;
    }

    let (cmd_tx, cmd_rx) = mpsc::channel::<TerminalCommand>(64);
    let mut resp_rx = client
        .stream(ReceiverStream::new(cmd_rx))
        .await?
        .into_inner();

    let _guard = setup_raw_mode()?;

    let mut current_pty_id = item.pty_id.clone();
    let mut current_item = item;
    let mut pty_list: Vec<PtyItem> = Vec::new();
    let mut previous_pty_id: Option<String> = None;
    let mut subscribed_pty_id: Option<String> = None;
    let allow_upgrade = mode == RenderMode::Region;
    let mut dispatch_mode = mode;
    let mut current_refresh_gen: u64 = 0;
    let mut stdout = tokio::io::stdout();
    let mut out = Vec::new();

    'session: loop {
        // === Phase: Subscribe ===
        let subscribe_ok = if subscribed_pty_id.as_deref() != Some(current_pty_id.as_str()) {
            let ok = subscribe(&cmd_tx, &mut resp_rx, &current_pty_id).await?;
            if ok { subscribed_pty_id = Some(current_pty_id.clone()); }
            ok
        } else {
            true
        };

        // === Phase: Request Refresh ===
        let refresh_result = if subscribe_ok {
            request_refresh(&cmd_tx, &mut resp_rx, &current_pty_id).await?
        } else {
            None
        };

        let (refresh_gen, refresh_bytes, buffered) = match refresh_result {
            Some(triple) => triple,
            None => {
                clear_screen();
                eprint!("\r\n[PTY closed]\r\n");
                (0, vec![], vec![])
            }
        };
        current_refresh_gen = refresh_gen;

        // Pre-filter buffered chunks by generation
        let buffered: Vec<_> = buffered.into_iter()
            .filter(|(gen, _)| *gen > current_refresh_gen)
            .collect();

        // === Phase: Create Handler & Init ===
        let mut handler: Box<dyn RenderModeHandler> = create_handler(
            dispatch_mode, current_item.cols, current_item.rows, allow_upgrade,
        )?;

        out.clear();
        if let EventResult::ChangeRenderMode(new_mode) = handler.init(&refresh_bytes, &buffered, &mut out)? {
            // Handler rejected (e.g., region: client too small) — fall back
            handler.cleanup(&mut out);
            if !out.is_empty() {
                stdout.write_all(&out).await?;
                out.clear();
            }
            dispatch_mode = new_mode;
            handler = create_handler(dispatch_mode, current_item.cols, current_item.rows, allow_upgrade)?;
            handler.init(&refresh_bytes, &buffered, &mut out)?;
        }
        if !out.is_empty() {
            stdout.write_all(&out).await?;
            stdout.flush().await?;
        }

        // === Phase: Spawn Input Task ===
        let (action_tx, mut action_rx) = mpsc::channel::<InputAction>(4);
        let input_task = tokio::spawn(input::run_stdin(
            cmd_tx.clone(),
            action_tx,
            current_pty_id.clone(),
        ));

        // === Phase: Event Loop ===
        let mut sigwinch = signal(SignalKind::window_change())?;
        let mut pty_closed = false;
        let mut refresh_debounce = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(86400)));
        let mut debounce_active = false;

        let outcome: RunOutcome = loop {
            out.clear();
            let mut change_mode: Option<(RenderMode, Vec<u8>)> = None;

            tokio::select! {
                msg = resp_rx.message() => {
                    match msg {
                        Ok(Some(r)) => match r.response {
                            Some(Response::Stream(s)) if s.pty_id == current_pty_id && s.generation > current_refresh_gen => {
                                let result = handler.on_pty_event(PtyEvent::Stream { gen: s.generation, data: &s.data }, &mut out)?;
                                if let EventResult::ChangeRenderMode(m) = result {
                                    change_mode = Some((m, vec![]));
                                }
                            }
                            Some(Response::Refresh(rf)) if rf.pty_id == current_pty_id => {
                                current_refresh_gen = rf.generation;
                                current_item.cols = rf.cols;
                                current_item.rows = rf.rows;
                                let result = handler.on_pty_event(
                                    PtyEvent::Refresh { gen: rf.generation, cols: rf.cols, rows: rf.rows, data: &rf.data },
                                    &mut out,
                                )?;
                                if let EventResult::ChangeRenderMode(m) = result {
                                    change_mode = Some((m, rf.data));
                                }
                            }
                            Some(Response::Metadata(m)) if m.pty_id == current_pty_id => {
                                if m.reason == StreamMetadataReason::Resize as i32 {
                                    if let Some(ref mi) = m.item {
                                        if mi.cols > 0 && mi.rows > 0 {
                                            current_item.cols = mi.cols;
                                            current_item.rows = mi.rows;
                                            let result = handler.on_pty_event(
                                                PtyEvent::Resize { cols: mi.cols, rows: mi.rows },
                                                &mut out,
                                            )?;
                                            if let EventResult::ChangeRenderMode(m) = result {
                                                change_mode = Some((m, vec![]));
                                            }
                                        }
                                    }
                                } else if m.reason == StreamMetadataReason::Closed as i32 {
                                    if !pty_closed {
                                        pty_closed = true;
                                        handler.on_pty_event(PtyEvent::Closed, &mut out)?;
                                        move_terminal_end();
                                        eprint!("\r\n[PTY closed]\r\n");
                                    }
                                }
                            }
                            _ => {}
                        },
                        _ => { break RunOutcome::ServerClosed; }
                    }
                }
                action = action_rx.recv() => {
                    break RunOutcome::Action(action.unwrap_or(InputAction::Detach));
                }
                _ = sigwinch.recv() => {
                    let (cols, rows) = get_terminal_size();
                    match handler.on_sigwinch(cols, rows, &mut out)? {
                        EventResult::ChangeRenderMode(m) => {
                            change_mode = Some((m, vec![]));
                        }
                        EventResult::RequestRefresh => {
                            refresh_debounce.as_mut().reset(
                                tokio::time::Instant::now() + std::time::Duration::from_secs(1)
                            );
                            debounce_active = true;
                        }
                        EventResult::Continue => {}
                    }
                }
                _ = &mut refresh_debounce, if debounce_active => {
                    debounce_active = false;
                    let _ = cmd_tx.send(TerminalCommand {
                        command: Some(Command::Refresh(RefreshRequest {
                            pty_id: current_pty_id.clone(),
                        })),
                    }).await;
                }
            }

            // Handle mode change (consolidated — one place for all event sources)
            if let Some((new_mode, refresh_data)) = change_mode {
                handler.cleanup(&mut out);
                dispatch_mode = new_mode;
                handler = create_handler(dispatch_mode, current_item.cols, current_item.rows, allow_upgrade)?;
                let init_result = handler.init(&refresh_data, &[], &mut out)?;
                if let EventResult::ChangeRenderMode(fallback) = init_result {
                    handler.cleanup(&mut out);
                    dispatch_mode = fallback;
                    handler = create_handler(dispatch_mode, current_item.cols, current_item.rows, allow_upgrade)?;
                    handler.init(&refresh_data, &[], &mut out)?;
                }
            }

            if !out.is_empty() {
                if stdout.write_all(&out).await.is_err() { break RunOutcome::ServerClosed; }
                let _ = stdout.flush().await;
            }
        };

        input_task.abort();
        let _ = input_task.await;

        match outcome {
            RunOutcome::ServerClosed => {
                reset_terminal_modes();
                move_terminal_end();
                eprintln!("[Connection closed]");
                break 'session;
            }
            RunOutcome::Action(action) => {
                reset_terminal_modes();
                match action {
                    InputAction::Detach => break 'session,

                    InputAction::Destroy => {
                        cmd_tx.send(TerminalCommand {
                            command: Some(Command::Destroy(DestroyRequest {
                                pty_id: current_pty_id.clone(),
                            })),
                        }).await?;
                        loop {
                            match resp_rx.message().await? {
                                None => { eprintln!("[server disconnected]"); break 'session; }
                                Some(r) => if let Some(Response::Command(c)) = r.response {
                                    if c.pty_id != current_pty_id { continue; }
                                    if !c.success {
                                        show_error(&format!("[Failed to destroy PTY: {}]", c.error.unwrap_or_default())).await;
                                        continue 'session;
                                    }
                                    break;
                                }
                            }
                        }
                        pty_list.clear();
                        if ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            if let Some(target) = recent_pty(&pty_list, &previous_pty_id).cloned() {
                                if target.pty_id != current_pty_id {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                }
                            }
                        }
                    }

                    InputAction::ForceResize => {
                        let (cols, rows) = get_terminal_size();
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Resize(ResizeRequest {
                                pty_id: current_pty_id.clone(), cols, rows,
                            })),
                        }).await;
                    }

                    InputAction::ForceRefresh => {
                        let _ = cmd_tx.send(TerminalCommand {
                            command: Some(Command::Refresh(RefreshRequest {
                                pty_id: current_pty_id.clone(),
                            })),
                        }).await;
                    }

                    InputAction::Create => {
                        let (cols, rows) = get_terminal_size();
                        cmd_tx.send(TerminalCommand {
                            command: Some(Command::Create(CreateRequest {
                                cols, rows, command: None,
                            })),
                        }).await?;
                        'create: loop {
                            match resp_rx.message().await? {
                                None => { move_terminal_end(); eprintln!("[server disconnected]"); break 'session; }
                                Some(r) => if let Some(Response::Create(cr)) = r.response {
                                    match cr.item {
                                        Some(new_item) => {
                                            switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, new_item).await;
                                            break 'create;
                                        }
                                        None => {
                                            show_error("[Failed to create new PTY]").await;
                                            pty_list.clear();
                                            continue 'session;
                                        }
                                    }
                                }
                            }
                        }
                        pty_list.clear();
                    }

                    InputAction::SwitchNext => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = next_pty(&pty_list, &current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::SwitchPrevious => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = prev_pty(&pty_list, &current_pty_id).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::SwitchRecent => {
                        if ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            if let Some(target) = recent_pty(&pty_list, &previous_pty_id).cloned() {
                                if target.pty_id != current_pty_id {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                }
                            }
                        }
                    }

                    InputAction::SwitchIndex(n) => {
                        if !ensure_list(&cmd_tx, &mut resp_rx, &mut pty_list).await {
                            continue 'session;
                        }
                        if let Some(target) = pty_list.get(n as usize).cloned() {
                            if target.pty_id != current_pty_id {
                                switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                            }
                        }
                    }

                    InputAction::ShowList => {
                        match show_list(&cmd_tx, &mut resp_rx, &mut pty_list, &current_pty_id).await? {
                            Some(new_id) if new_id != current_pty_id => {
                                if let Some(target) = pty_list.iter().find(|p| p.pty_id == new_id).cloned() {
                                    switch_pty(&cmd_tx, &mut current_pty_id, &mut current_item, &mut previous_pty_id, target).await;
                                    pty_list.clear();
                                }
                            }
                            _ => {}
                        }
                    }

                    InputAction::ShowInfo => {
                        show_error(&format!(
                            "requested={mode:?} actual={dispatch_mode:?} pty={current_pty_id}"
                        )).await;
                    }

                    InputAction::ShowScrollback => {
                        scrollback::show_scrollback(
                            &cmd_tx,
                            &mut resp_rx,
                            &current_pty_id,
                            current_item.rows,
                        ).await?;
                    }
                }
            }
        }
    }

    move_terminal_end();
    drop(_guard);
    Ok(())
}
```

- [ ] **Step 4: Update imports in mod.rs**

Add the necessary imports at the top of `mod.rs`:

```rust
use std::pin::Pin;
use tokio::signal::unix::{signal, SignalKind};
use tokio::io::AsyncWriteExt;
```

And add `Response` and `StreamMetadataReason` to the existing proto import block:

```rust
use termd::proto::{
    terminal_command::Command, terminal_response::Response,
    CreateRequest, DestroyRequest, ListRequest, PtyItem, RefreshRequest, ResizeRequest,
    SubscribeRequest, UnsubscribeRequest,
    TerminalCommand, StreamMetadataReason,
    terminal_service_client::TerminalServiceClient,
};
```

- [ ] **Step 5: Verify compilation**

```bash
cargo check 2>&1 | tail -20
```

Expected: compiles with no errors. There may be warnings about unused imports in the old handler files if any remnants remain.

- [ ] **Step 6: Run all tests**

```bash
cargo test 2>&1 | tail -20
```

Expected: all tests pass — handler tests, VtFilter tests, render_dirty tests, input tests, scrollback tests, integration tests.

- [ ] **Step 7: Commit**

```bash
git add src/attach/mod.rs src/attach/cell.rs src/attach/raw.rs src/attach/region.rs
git commit -m "Restructure attach main loop as long-lived demux with handler dispatch"
```

---

### Task 6: Clean up dead code and verify

**Files:**
- Modify: `src/attach/mod.rs`
- Verify: `src/attach/cell.rs`, `src/attach/raw.rs`, `src/attach/region.rs`

- [ ] **Step 1: Remove any remaining dead code**

Check for and remove:
- Any remaining references to the old `RunContext` type
- Any unused imports across all attach files
- The old `RunOutcome` variants if they still exist

```bash
cargo check 2>&1 | grep "warning.*unused"
```

Fix any warnings.

- [ ] **Step 2: Run the full test suite**

```bash
cargo test 2>&1
```

Expected: all tests pass, no warnings.

- [ ] **Step 3: Manual smoke test**

Build and run the daemon + client to verify the refactored attach works end-to-end:

```bash
cargo build 2>&1 | tail -5
```

Verify with all three render modes (`--render-mode cell`, `--render-mode raw`, `--render-mode region`) that:
- Attaching works
- Typing produces output
- `^A c` creates a new PTY
- `^A space` switches PTYs
- `^A "` shows the list
- `^A d` detaches
- Window resize is handled (for region mode: verify cell↔region transitions)

- [ ] **Step 4: Commit final cleanup**

```bash
git add -A
git commit -m "Clean up unused imports and dead code after handler refactor"
```
