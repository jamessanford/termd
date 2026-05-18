# utmp Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write utmp/wtmp entries when PTY sessions open and close so shells spawned by `termd` appear in `who`, `w`, and `last`.

**Architecture:** A new `src/utmp.rs` module wraps the `utempter_add_record` / `utempter_remove_record` C functions behind unconditional Rust helpers. A `build.rs` probe emits `cfg(has_utempter)` when `libutempter` is present; without it the helpers compile as no-ops. `pty.rs` gains exactly two call sites.

**Tech Stack:** `libutempter` (system library, optional), `pkg-config` crate (build-time only), `libc` (already a dependency)

---

## File Map

| Action | Path | Purpose |
|--------|------|---------|
| Modify | `Cargo.toml` | Add `pkg-config` to `[build-dependencies]` |
| Modify | `build.rs` | Add libutempter probe after existing tonic build step |
| Create | `src/utmp.rs` | `add_record` / `remove_record` with cfg'd `extern "C"` calls |
| Modify | `src/lib.rs` | Declare `pub mod utmp;` |
| Modify | `src/pty.rs` | Call `utmp::add_record` after spawn; `utmp::remove_record` on exit |

---

## Task 1: Extend build infrastructure for libutempter detection

**Files:**
- Modify: `Cargo.toml`
- Modify: `build.rs`

- [ ] **Step 1: Add `pkg-config` to build-dependencies in `Cargo.toml`**

In the `[build-dependencies]` section (currently has `tonic-build`), add:

```toml
[build-dependencies]
tonic-build = "0.12"
pkg-config = "0.3"
```

- [ ] **Step 2: Add the libutempter probe to `build.rs`**

`build.rs` currently compiles the proto. Add the probe after the `tonic_build` call:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/terminal.proto"], &["proto"])?;

    if pkg_config::probe_library("libutempter").is_ok() {
        println!("cargo:rustc-cfg=has_utempter");
    }

    Ok(())
}
```

`probe_library` automatically emits `cargo:rustc-link-lib=utempter` on success, so
no manual link directive is needed.

- [ ] **Step 3: Verify the build succeeds**

```bash
cargo build 2>&1
```

Expected: compiles cleanly. If `libutempter` is installed, the build output will
include a `cargo:rustc-cfg=has_utempter` line from the probe. Confirm:

```bash
cargo build -v 2>&1 | grep has_utempter
```

Expected on a system with libutempter: one matching line.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml build.rs
git commit -m "build: probe for libutempter and emit cfg(has_utempter)"
```

---

## Task 2: Create `src/utmp.rs`

**Files:**
- Create: `src/utmp.rs`

- [ ] **Step 1: Write the failing test first**

Create `src/utmp.rs` with the test module only — no implementation yet:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_do_not_panic() {
        // stdin fd 0 is not a PTY master — utempter will fail internally,
        // but the wrapper must not panic regardless.
        add_record(0, "localhost");
        remove_record(0);
    }
}
```

- [ ] **Step 2: Run the test to confirm it fails to compile**

```bash
cargo test --lib utmp 2>&1
```

Expected: compile error — `add_record` and `remove_record` are not defined.

- [ ] **Step 3: Implement `src/utmp.rs`**

Replace the file content with the full implementation:

```rust
use std::os::unix::io::RawFd;

#[cfg(has_utempter)]
extern "C" {
    fn utempter_add_record(master_fd: libc::c_int, host: *const libc::c_char) -> libc::c_int;
    fn utempter_remove_record(master_fd: libc::c_int) -> libc::c_int;
}

/// Write a USER_PROCESS utmp entry for the PTY identified by `master_fd`.
/// `host` is placed in the ut_host field; use the local hostname for local sessions.
/// No-op if termd was built without libutempter.
pub fn add_record(master_fd: RawFd, host: &str) {
    #[cfg(has_utempter)]
    {
        use std::ffi::CString;
        if let Ok(c_host) = CString::new(host) {
            unsafe { utempter_add_record(master_fd, c_host.as_ptr()); }
        }
    }
    #[cfg(not(has_utempter))]
    { let _ = (master_fd, host); }
}

/// Write a DEAD_PROCESS utmp entry, closing out the session for `master_fd`.
/// No-op if termd was built without libutempter.
pub fn remove_record(master_fd: RawFd) {
    #[cfg(has_utempter)]
    unsafe { utempter_remove_record(master_fd); }
    #[cfg(not(has_utempter))]
    { let _ = master_fd; }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_do_not_panic() {
        // stdin fd 0 is not a PTY master — utempter will fail internally,
        // but the wrapper must not panic regardless.
        add_record(0, "localhost");
        remove_record(0);
    }
}
```

- [ ] **Step 4: Run the test to confirm it passes**

```bash
cargo test --lib utmp 2>&1
```

Expected:
```
test utmp::tests::add_and_remove_do_not_panic ... ok
```

- [ ] **Step 5: Commit**

```bash
git add src/utmp.rs
git commit -m "feat(utmp): add add_record/remove_record helpers with cfg(has_utempter) gate"
```

---

## Task 3: Wire up call sites in `src/pty.rs`

**Files:**
- Modify: `src/lib.rs`
- Modify: `src/pty.rs`

- [ ] **Step 1: Declare the module in `src/lib.rs`**

`src/lib.rs` currently declares:

```rust
pub mod commands;
pub mod pty;
pub mod server;
```

Add `utmp` in the same style:

```rust
pub mod commands;
pub mod pty;
pub mod server;
pub mod utmp;
```

- [ ] **Step 2: Add `add_record` call in `create()`**

In `src/pty.rs`, inside `PtyRegistry::create()`, find the lines after `cmd.spawn()`:

```rust
        let child = cmd.spawn().context("spawn shell")?;
        let child_pid = Pid::from_raw(child.id() as i32);
        let created_at = SystemTime::now();
```

Add the call immediately after `child_pid` is set:

```rust
        let child = cmd.spawn().context("spawn shell")?;
        let child_pid = Pid::from_raw(child.id() as i32);
        crate::utmp::add_record(master_reader_fd, &hostname);
        let created_at = SystemTime::now();
```

`master_reader_fd` is still a raw `i32` at this point (it gets wrapped in a `File`
later when the reader thread is spawned). `hostname` is already in scope.

- [ ] **Step 3: Add `remove_record` call in `reader_thread()` cleanup**

In `reader_thread`, find the child-reap block near the bottom of the function:

```rust
    // Reap child and broadcast exit notification
    let status = child.try_wait().ok().flatten().or_else(|| child.wait().ok());
    let exit_msg = {
```

Add the remove call immediately after the reap, before building the exit message:

```rust
    // Reap child and broadcast exit notification
    let status = child.try_wait().ok().flatten().or_else(|| child.wait().ok());
    crate::utmp::remove_record(master.as_raw_fd());
    let exit_msg = {
```

`master` is the `File` wrapping the reader's copy of the PTY master fd — the same
device utempter registered, so `ptsname()` returns the same pts path.

- [ ] **Step 4: Run the full test suite**

```bash
cargo test 2>&1
```

Expected: all tests pass, no new warnings.

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/pty.rs
git commit -m "feat(pty): register utmp entries on PTY open and close"
```
