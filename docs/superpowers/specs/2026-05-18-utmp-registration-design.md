# utmp Registration for PTY Sessions

**Date:** 2026-05-18
**Status:** Approved

## Problem

PTY sessions created by `termd` are invisible to standard Unix session accounting
tools (`who`, `w`, `last`). When a shell is spawned via `PtyRegistry::create`, no
utmp/wtmp entry is written, so the session does not appear to the system.

## Goals

- Shell sessions show up in `who`, `w`, and `last`
- No new required dependencies — build succeeds on systems without `libutempter`
- No PAM, no systemd, no elevated privileges in `termd` itself
- Minimal changes to `src/pty.rs`

## Design

### Auto-detection via `build.rs`

A `build.rs` at the crate root probes for `libutempter` using the `pkg-config`
build-dependency:

```rust
fn main() {
    if pkg_config::probe_library("libutempter").is_ok() {
        println!("cargo:rustc-cfg=has_utempter");
    }
    // pkg-config emits cargo:rustc-link-lib=utempter automatically on success
}
```

If `libutempter` is absent, neither the cfg flag nor the link directive is emitted
and the build succeeds with utmp support compiled out. No feature flag — detection
is fully transparent.

### `src/utmp.rs` (new module)

Owns all utempter interaction. Exports two functions unconditionally so call sites
in `pty.rs` require no cfg guards:

```rust
pub fn add_record(master_fd: RawFd, host: &str)
pub fn remove_record(master_fd: RawFd)
```

The `#[cfg(has_utempter)]` block is inside each function. When the flag is absent
both functions are no-ops and the extern declarations are not compiled.

The `extern "C"` declarations are declared directly in `utmp.rs` — no wrapper
crate needed since `pkg-config` handles linking:

```rust
#[cfg(has_utempter)]
extern "C" {
    fn utempter_add_record(master_fd: libc::c_int, host: *const libc::c_char) -> libc::c_int;
    fn utempter_remove_record(master_fd: libc::c_int) -> libc::c_int;
}
```

`utempter_add_record` calls `ptsname()` internally to resolve the device name, so
passing either dup of the master fd is correct.

The hostname in the utmp entry is the local machine hostname. If `termd` is later
extended to accept remote clients, the call site in `create()` can pass the client
hostname instead — that change stays in `pty.rs`, not `utmp.rs`.

### `src/pty.rs` changes

Two call sites added, no other changes:

**In `create()`**, immediately after `cmd.spawn()`:
```rust
crate::utmp::add_record(master_reader_fd, &hostname);
```
`master_reader_fd` is the dup allocated for the reader thread. Using it mirrors
the fd that `reader_thread` will later pass to `remove_record`, keeping the same
fd consistent across the lifetime of the utmp entry.

**In `reader_thread()`**, in the exit cleanup block after the child is reaped:
```rust
crate::utmp::remove_record(master.as_raw_fd());
```
`master` is the `File` wrapping `master_reader_fd` — same PTY device, so
`ptsname()` returns the same result.

### `Cargo.toml` change

```toml
[build-dependencies]
pkg-config = "0.3"
```

No new runtime dependencies.

## What is not in scope

- PAM session management (`pam_open_session` / `pam_close_session`)
- systemd-logind `CreateSession` D-Bus call
- `XDG_SESSION_ID` / `XDG_RUNTIME_DIR` setup
- Per-client hostname in utmp entries (local hostname is used)
