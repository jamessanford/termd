---
name: attach design
description: Design spec for termd attach — subscribe+refresh+stream attach command with full ANSI refresh output
type: project
---

# termd `attach` Feature Spec

## Overview

Adds a `termd attach <pty-id>` subcommand that connects to a running PTY on the daemon, paints the current screen state, then streams live output — forwarding local stdin back to the PTY. Requires two coordinated changes: the server-side `do_refresh` must emit real ANSI terminal sequences (not bare UTF-8), and the CLI must manage a long-lived bidi gRPC stream with raw-mode terminal handling.

---

## Part 1: Server-side — `do_refresh` ANSI output

### File: `src/pty.rs`

Replace the current UTF-8 cell dump in `do_refresh` with a sequence of ANSI escape codes that reconstruct the full screen when replayed into any terminal emulator (including libghostty).

### Output sequence

1. **Soft reset** `\x1b[!p` (DECSTR) — resets SGR and most mode state
2. **Clear + home** `\x1b[2J\x1b[H`
3. **Hide cursor** `\x1b[?25l` — prevents flicker during repaint
4. **Per row** (0..rows): move cursor `\x1b[{row+1};1H`, then per cell:
   - Track "current SGR state"; emit `\x1b[0m` + fresh SGR only when style changes from previous cell
   - **Foreground**: `cell.fg_color()` → `Some(RgbColor{r,g,b})` → `\x1b[38;2;{r};{g};{b}m`; `None` → omit (terminal default)
   - **Background**: same pattern with `\x1b[48;2;{r};{g};{b}m`
   - **Flags** from `cell.style()`: bold→`1`, faint→`2`, italic→`3`, underline(single)→`4`, blink→`5`, inverse→`7`, invisible→`8`, strikethrough→`9`, overline→`53`, double-underline→`21`; curly/dotted/dashed underline → emit `4` (single)
   - **Graphemes**: emit UTF-8 text; if cell is empty, emit `' '`
5. **Reset SGR** `\x1b[0m`
6. **Cursor visibility**: emit `\x1b[?25h` if `snapshot.cursor_visible()` is true, else `\x1b[?25l`
7. **Cursor position**: `\x1b[{cursor_y+1};{cursor_x+1}H` using the values already stored in `RefreshData`

### Color source

Use `CellIteration::fg_color()` and `bg_color()` (which resolve palette indices already) rather than `cell.style().fg_color`. The `Style` struct is used only for boolean flags.

### Omissions (PoC)

- Kitty graphics protocol / sixel pixel data — skip
- Mouse mode state, alternate screen mode, bracketed paste mode, scrolling region — not emitted (TODO for future)
- Palette-based color fallback: if `fg_color()`/`bg_color()` return `Err`, treat as `None`

### Proto / test impact

No proto changes. `RefreshData.data` still carries `Bytes`; `cursor_x`/`cursor_y` carry the cursor position as before. The existing `test_refresh_returns_screen_data` assertion `!data.is_empty()` continues to pass; add a check that `data` starts with `b"\x1b["` to make it meaningful.

---

## Part 2: Client-side — `attach` subcommand

### File: `src/main.rs`

### Invocation

```
termd attach <pty-id> [--socket PATH]
```

### Startup sequence

1. Connect to daemon Unix socket with auth token (same as other client commands)
2. Open a **long-lived** bidi gRPC stream (do not close the send side after the first command)
3. Send `SubscribeRequest { pty_id }` → wait for `CommandResponse { success: true }`; exit on failure
4. Read local terminal size via `libc::ioctl(STDOUT_FILENO, TIOCGWINSZ, ...)` and send `ResizeRequest` to sync the PTY dimensions to the client window
5. Send `RefreshRequest { pty_id }`; begin buffering any `StreamData` messages that arrive before the response
6. Receive `RefreshResponse`: record `refresh_gen = response.generation`, write `response.data` to stdout, replay buffered `StreamData` with `generation > refresh_gen` to stdout

   > **Buffering**: steps 3–6 poll the gRPC receive side directly in the main task (before any tasks are spawned). Any `StreamData` arriving before the `RefreshResponse` is pushed onto a `Vec`; after the refresh is received, that vec is drained and filtered by generation.

7. Enter raw mode (see below)
8. Spawn `forwarder_task`, `stdin_task`, `sigwinch_task` (in that order — forwarder must exist before others can send), then enter main select loop

### Terminal raw mode

Use `nix::sys::termios::{tcgetattr, tcsetattr}` (the `term` feature is already enabled in `Cargo.toml`).

Save original settings before modification. Wrap in a **drop guard** (`TerminalGuard`) that calls `tcsetattr(STDIN_FILENO, TCSAFLUSH, &original)` on drop — ensures restoration on any exit path including panics.

Raw mode settings applied:
- Clear `ICANON`, `ECHO`, `ISIG`, `IEXTEN` from `local_flags`
- Clear `IXON`, `ICRNL`, `BRKINT`, `INPCK`, `ISTRIP` from `input_flags`
- `VMIN = 1`, `VTIME = 0`

### Task structure

Three concurrent actors share a single `mpsc::Sender<TerminalCommand>` for outbound commands to the server. The gRPC stream's send side is driven by a dedicated forwarder task that reads from this channel.

```
stdin_task    ──cmd_tx──┐
sigwinch_task ──cmd_tx──┼──▶ forwarder_task ──▶ gRPC stream ──▶ server
main_task     ──cmd_tx──┘

server ──▶ gRPC stream ──▶ main_task ──▶ stdout
```

**`stdin_task`**: reads raw bytes from `tokio::io::stdin()`, runs the escape state machine (see below), sends `WriteRequest { pty_id, data }` via `cmd_tx`; on `\n~.` detected, sends shutdown via a `oneshot::Sender<()>` and exits.

**`sigwinch_task`**: listens on `tokio::signal::unix::signal(SignalKind::window_change())`, reads updated terminal dimensions via `TIOCGWINSZ`, sends `ResizeRequest { pty_id, cols, rows }` via `cmd_tx`.

**`forwarder_task`**: owns the gRPC sink; reads from `cmd_rx`, writes each command to the stream; exits when `cmd_rx` closes (all senders dropped).

**`main_task`**: owns the gRPC receive side. `tokio::select!` over:
- Next `StreamData` message → write `data` bytes to stdout
- `shutdown_rx` fires → break
- Stream ends (`None`) → break

On exit from main loop: abort `stdin_task` and `sigwinch_task`, drop `cmd_tx` (signals forwarder that no more commands are coming), await `forwarder_task` (ensures gRPC stream closes cleanly), then `TerminalGuard` drops and restores terminal, process exits.

### `\n~.` escape state machine

Initial state: `AfterNewline` (treat start of session as post-newline).

| State | Byte | Action | Next state |
|---|---|---|---|
| `AfterNewline` | `~` | hold `~` | `AfterTilde` |
| `AfterNewline` | `\n` | flush `\n` as WriteRequest | `AfterNewline` |
| `AfterNewline` | other | flush byte | `Normal` |
| `Normal` | `\n` | flush `\n` | `AfterNewline` |
| `Normal` | other | flush byte | `Normal` |
| `AfterTilde` | `.` | send shutdown, exit task | — |
| `AfterTilde` | `\n` | flush `~\n` | `AfterNewline` |
| `AfterTilde` | other | flush `~` + byte | `Normal` |

"Flush" = send bytes as `WriteRequest { pty_id, data }` via `cmd_tx`.

### Edge cases

- **Server disconnect**: gRPC receive side returns `None` → main_task breaks → terminal restored, process exits 0
- **Ctrl-C in raw mode**: `ISIG` is cleared, so `^C` (0x03) is forwarded as a `WriteRequest` byte to the PTY — the PTY shell handles it. `\n~.` is the only local exit path.
- **stdout write error**: treated as shutdown (same as server disconnect)
- **Subscribe failure**: print error to stderr, exit 1 before entering raw mode
- **Stale StreamData** (generation ≤ refresh_gen): silently drop; these are already baked into the refresh screen

### No new proto changes

Uses existing `SubscribeRequest`, `RefreshRequest`, `WriteRequest`, `ResizeRequest` and their response types.

---

## Testing

- **`do_refresh` unit**: update `test_refresh_returns_screen_data` to assert `data.starts_with(b"\x1b[")` in addition to non-empty
- **`attach` command**: requires a real PTY + terminal, not unit-testable; manual smoke test after implementation
