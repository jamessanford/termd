# Performance Notes

## Profiling setup

```sh
# Attach perf to a running termd process
perf record -g -F 999 -p <PID> -o perf.data -- sleep 10 &
PERF_PID=$!
sleep 1
LD_LIBRARY_PATH=target/debug/build/libghostty-vt-sys-b3b8792ea556457c/out/ghostty-install/lib \
  ./target/debug/termd send <PTY_PREFIX> 'du'
wait $PERF_PID

# Generate flamegraph
perf script -i perf.data | inferno-collapse-perf | inferno-flamegraph > flamegraph.svg

# Top symbols
perf report -i perf.data --no-children --sort comm,dso,sym --percent-limit 0.3 --stdio
```

## May 2026 — client render_dirty hot paths

Profiled `termd attach` client (debug build) while running `du` through a 70×20 terminal.

### Server-side (pid 193389)

The server's dominant cost was **libghostty-vt VT processing** of the `du` output:

| Symbol | % |
|---|---|
| `terminal.Terminal.print` (libghostty-vt) | 5.38% |
| `ghostty_terminal_vt_write` | 1.07% |
| `h2::proto::streams::prioritize::Prioritize::pop_frame` | 1.05% |
| `server::TerminalServiceImpl::stream` closure | 0.82% |
| `h2::codec::framed_write::Encoder::buffer` | 0.82% |

Server time is dominated by the ghostty VT parser and gRPC/H2 streaming overhead. Not a bottleneck worth optimizing further unless output volume increases significantly.

### Client-side — before optimization (pid 193433)

| Symbol | % |
|---|---|
| `termd::attach::render_dirty` | 6.66% |
| `StyleColor::try_from` (FFI conversion) | 5.04% |
| `Style::try_from` (FFI conversion) | 4.54% |
| `ptr::is_aligned_to` (debug checks) | 4.34% + 3.32% |
| `Result::branch` (try trait, debug) | 3.79% |
| `ghostty_render_state_row_cells_get` | 3.69% |
| `CellIteration::get` | 2.70% |
| `CellIteration::fg_color` | 2.32% |
| `CellIteration::bg_color` | 2.04% |
| `CellIteration::graphemes` | 1.82% |
| Vec alloc/dealloc churn | ~4% scattered |

**Root causes:**
- `style()`, `fg_color()`, and `bg_color()` were called unconditionally for every cell, including empty ones. On a typical terminal most cells per row are empty, so the majority of the FFI type conversions (`Style::try_from`, `StyleColor::try_from`) were wasted.
- `graphemes()` allocated a fresh `Vec<char>` per cell.
- The output `Vec<u8>` was re-allocated on every received stream message.

### Optimizations applied (commit ee8cfdb)

1. **Skip `style()` and `fg_color()` for empty cells.** Check `graphemes_len()` first; for cells with no text only call `bg_color()` (background color still applies). Eliminates the dominant FFI conversion cost for the majority of cells.

2. **Reuse grapheme buffer.** Single `Vec<char>` allocated once per `render_dirty` call, resized only when a cell has more graphemes than previously seen. Uses `graphemes_buf(&mut slice)` instead of `graphemes()`.

3. **Reuse output buffer.** Moved `Vec<u8>` outside the receive loop and `clear()` it each iteration instead of reallocating per message.

### Client-side — after optimization (pid 203824)

Sample count dropped from ~7200 to ~657 over the same 10s window, suggesting `du` output rendered substantially faster. Percentages are noisier at lower sample counts.

| Symbol | % | vs. before |
|---|---|---|
| `StyleColor::try_from` | 5.17% | ≈ same (now driven by `bg_color` on all cells) |
| `render_dirty` | 5.73% | ↓ from 6.66% |
| `Style::try_from` | 5.00% | ≈ same (only non-empty cells now) |
| `CellIteration::bg_color` | 4.44% | ↑ from 2.04% (relative share grew as other costs fell) |
| `CellIteration::fg_color` | — | ✓ eliminated from hot path |
| `CellIteration::graphemes` | — | ✓ eliminated from hot path |
| Vec alloc/dealloc | — | ✓ eliminated |
| `CellIteration::style` | 0.39% | ↓ from significant |

## TODO — remaining hot paths

- **`bg_color` on every cell (4.44%)** — now the top cell accessor. Worth checking if there's a cheap way to skip the FFI call for cells with default background (e.g. a row-level flag or a cheaper `has_bg` predicate).
- **`StyleColor::try_from` and `Style::try_from`** — still present because `bg_color` uses `StyleColor::try_from` and styled text cells still pay full conversion cost. Caching or a cheaper comparison path could help.
- **Debug-mode pointer checks** (`ptr::is_aligned_to`, `copy_nonoverlapping::precondition_check`, `Result::branch`) — significant in debug builds, will largely disappear in release. Profile a release build before optimizing further.
