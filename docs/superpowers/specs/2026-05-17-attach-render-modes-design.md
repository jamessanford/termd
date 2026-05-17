# Design: `--render-mode` flag for `termd attach`

**Date:** 2026-05-17  
**Status:** Approved  
**Branch:** main

---

## Context

`termd attach` currently uses a single cell-by-cell rendering strategy backed by
libghostty-vt. An experimental branch (`experimental/vt-formatter-full-dirty`)
tried a different strategy for `Dirty::Full` repaints. Rather than managing
strategies via branches, we want a `--render-mode` flag so all strategies can be
compared at runtime during this research phase.

A separate document (`RENDERING_MODES.md`) captures longer-term directions
(region passthrough, upstream libghostty contributions). This spec covers only
what is being implemented now.

---

## Scope

Implement three render modes accessible via `--render-mode` on `termd attach`:

| Mode | Description |
|---|---|
| `cell` | Cell-by-cell render state for all dirty states. **Default.** |
| `formatter` | Cell-by-cell for `Dirty::Partial`; libghostty VT formatter for `Dirty::Full`. |
| `raw` | Direct PTY byte passthrough. No libghostty on the client render path. |

`cell` is the default (preserves existing main-branch behavior).

---

## File Structure

`src/attach.rs` is converted to a module directory. `mod attach;` in `main.rs`
is unchanged — Rust resolves both `attach.rs` and `attach/mod.rs`.

```
src/
  main.rs              -- adds RenderMode to Attach subcommand
  attach/
    mod.rs             -- RenderMode enum, pub run(), shared helpers
    raw.rs             -- raw mode select loop
    cell.rs            -- cell mode select loop + render_dirty
    formatter.rs       -- formatter mode select loop + render_dirty
```

Each mode file contains a complete, self-contained implementation of its render
loop, including its own copy of `render_dirty` where applicable. This deliberate
duplication makes it easy to read, diff, and iterate each approach independently,
and makes adding future variants straightforward.

---

## `RenderMode` enum

Defined in `attach/mod.rs`, derived with `clap::ValueEnum`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum RenderMode {
    Cell,
    Formatter,
    Raw,
}
```

Imported in `main.rs`. Added to the `Attach` variant:

```rust
#[arg(long, value_enum, default_value_t = RenderMode::Cell)]
render_mode: RenderMode,
```

`attach::run` gains a `mode: RenderMode` parameter.

---

## `attach/mod.rs` — shared preamble and helpers

### Shared helpers (unchanged from current `attach.rs`)

- `TerminalGuard` + `setup_raw_mode`
- `get_terminal_size`
- `run_stdin` (the `~.` escape sequence handler)
- `LocalTerminal` struct + `impl` (used by cell and formatter; not raw)

### `RunContext` struct

Bundles the arguments passed to each mode's `run` function, avoiding a long
argument list at the dispatch site and in each mode file:

```rust
pub(super) struct RunContext {
    pub resp_rx: Streaming<TerminalResponse>,
    pub cmd_tx:  mpsc::Sender<TerminalCommand>,
    pub pty_id:  String,
    pub item:    PtyItem,     // server PTY metadata; raw ignores cols/rows
    pub refresh_gen:   u64,
    pub refresh_bytes: Vec<u8>,
    pub buffered:      Vec<(u64, Vec<u8>)>,
    pub debug:         bool,
}
```

### `pub async fn run` — preamble then dispatch

```
1. Open gRPC stream, send Subscribe command, await Command(success)
2. Sync local terminal size to server via Resize command
3. Send Refresh command
4. Await Refresh response, buffering any Stream chunks that arrive first
5. Enter raw terminal mode (TerminalGuard)
6. Spawn stdin task
7. Build RunContext; dispatch:
     Raw       → raw::run(ctx).await
     Cell      → cell::run(ctx).await
     Formatter → formatter::run(ctx).await
8. Abort stdin task, drop TerminalGuard, print close message if server closed
```

Steps 1–6 and 8 are identical for all three modes.

---

## `attach/raw.rs`

No libghostty imports. Manages no local terminal state.

Signature: `pub(super) async fn run(ctx: RunContext) -> Result<()>`

**Startup:** Write `ctx.refresh_bytes` directly to stdout. Replay any buffered
stream chunks that post-date `ctx.refresh_gen` directly to stdout.

**Select loop:**

| Event | Action |
|---|---|
| `Stream(s)` where `s.generation > refresh_gen` | `stdout.write_all(&s.data)` |
| `Metadata(Resize)` | `stdout.write_all(b"\x1b[2J")` — clears screen; server broadcasts a Refresh after resize |
| `Metadata(Closed)` | break |
| `Refresh(rf)` | `stdout.write_all(&rf.data)` — handles SIGWINCH-triggered refresh response; update `refresh_gen` to `rf.generation` |
| SIGWINCH | `cmd_tx.send(Command::Refresh(...))` |
| shutdown (stdin `~.`) | break |

SIGWINCH does **not** resize the server PTY. It requests a Refresh so the client
repaints from the server's current state. The Refresh response arrives as
`Response::Refresh` in the same select loop.

`refresh_gen` is updated when a new Refresh response arrives, so subsequent
stream chunks are filtered correctly.

---

## `attach/cell.rs`

Uses libghostty-vt render state API. Cell-by-cell rendering for all dirty states.

Signature: `pub(super) async fn run(ctx: RunContext) -> Result<()>`

**Startup:** Create `LocalTerminal::new(ctx.item.cols, ctx.item.rows)`. Feed `ctx.refresh_bytes`
into the terminal; call `render_dirty(force_full=true)` to paint. Replay buffered
chunks: feed each into the terminal and call `render_dirty`.

**Select loop:**

| Event | Action |
|---|---|
| `Stream(s)` where `s.generation > refresh_gen` | Feed to terminal; `render_dirty(force_full=false)` |
| `Metadata(Resize)` | `lt.resize(cols, rows)`; `stdout.write_all(b"\x1b[2J")` |
| `Metadata(Closed)` | break |
| SIGWINCH | `render_dirty(force_full=true)` |
| shutdown | break |

**`render_dirty`** in this file: cell-by-cell iteration for both `Dirty::Partial`
and `Dirty::Full`. Uses row iterator + cell iterator; emits cursor-goto per row,
per-cell SGR + graphemes. (Current `attach.rs` implementation, unchanged.)

---

## `attach/formatter.rs`

Identical structure to `cell.rs`. Signature: `pub(super) async fn run(ctx: RunContext) -> Result<()>`.
The only difference from `cell.rs` is in `render_dirty`.

**`render_dirty`** in this file:
- `Dirty::Partial` → cell-by-cell loop (same as cell.rs)
- `Dirty::Full` → `\x1b[2J\x1b[H` + libghostty `Formatter::new(..., Format::Vt).format_alloc()`

This is the implementation from `experimental/vt-formatter-full-dirty`, ported
into this file.

---

## What is not in scope

- `region` mode (DECSTBM passthrough with stream rewriting) — documented in
  `RENDERING_MODES.md` as a future direction
- Mode 2026 synchronized output wrapping — noted in `RENDERING_MODES.md`
- Any server-side changes
- Performance profiling of the new modes (separate activity)
