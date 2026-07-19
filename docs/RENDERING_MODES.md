## Rendering modes for `termd attach`

### Background

Rendering the server VT data stream to a local terminal is not necessarily
straightforward, especially when the client window columns and rows do not
match the server side.

We've settled on two primary render modes to help this:

### `--render-mode` flag for `termd attach`

`--render-mode <mode>` flag (or `--renderer`) to the `Attach` subcommand
in `src/main.rs`. Possible values:

- **`cell`** — cell-by-cell render state iteration.
  Dirty tracking at row level; `Dirty::Partial` repaints only changed rows,
  `Dirty::Full` repaints everything row-by-row with cursor-goto per row.
  Has reduced performance, but allows a "viewport" into the server window.
  Clients typically do not want to stay in this mode for long, because they
  cannot see the entire screen.

- **`autowrap`** (current default) — raw passthrough with libghostty-driven
  explicit wrap injection; the output is valid for any client at least as wide
  as the server PTY. See [AUTOWRAP.md](AUTOWRAP.md).

- **`raw`** — original approach, code path now exists only for debugging/testing.
  forward the raw PTY byte stream from the server
  directly to stdout. Works perfectly when server and client are the same size;
  breaks on size mismatch because cursor positions and scroll regions are
  absolute. Fast, zero allocation, zero parsing on the client render path.

The `attach::run` function in `src/attach.rs` would take a `RenderMode` enum
argument. Each mode is a different code path; the shared infrastructure
(gRPC streaming, stdin forwarding, SIGWINCH handling) stays the same.

There was also a `region` mode (raw passthrough confined to a DECSTBM scroll
region plus DECSLRM horizontal margins, with in-stream rewriting of conflicting
escape sequences). The horizontal-margin state proved too contended to share
with server apps and it was removed in favor of `autowrap` — see
[AUTOWRAP.md](AUTOWRAP.md) for the full rationale.

---

## libghostty upstream gap

libghostty-vt already has a complete, battle-tested VT parser (`Parser`,
`Stream`, `TerminalStream`, `StreamAction`) exposed in the Zig public API
(`lib_vt.zig`). However, **none of this is in the C API** — there is no
`ghostty_parser_*` or `ghostty_stream_*` in the C exports (`terminal/c/`).

Exposing the parser in the C API would let C/Rust embedders build stream
filters without reimplementing a VT parser. A minimal addition:

```c
GhosttyParser ghostty_parser_new(void);
void ghostty_parser_free(GhosttyParser parser);
// Feed bytes; fills actions_out[0..actions_written] with parsed actions.
GhosttyResult ghostty_parser_feed(
    GhosttyParser parser,
    const uint8_t* bytes, size_t len,
    GhosttyParserAction* actions_out, size_t actions_cap,
    size_t* actions_written);
```

This is generic enough to be a clean upstream contribution — it enables stream
filters, protocol analyzers, and similar tools without baking proxy logic into
the library. The DECSTBM/DECLRMM rewriting logic itself stays in our code.

Worth filing or discussing upstream when there's appetite for it. The
formatter (state→bytes) and a hypothetical stream filter (bytes→modified bytes)
are complementary; the library already has both halves internally.

---

## Mode 2026 (synchronized output) for full repaints

The `xterm-ghostty` terminfo includes:
```
Sync=\E[?2026%?%p1%{1}%-%tl%eh%;
```
i.e., `\E[?2026h` / `\E[?2026l` for synchronized output. Wrapping the VT
formatter output (in `formatter` mode) with these sequences would suppress
rendering during the clear+repaint, eliminating the flash on scroll. Low-effort
win to try once the formatter mode is validated.

---

## Performance baseline

See `PERFORMANCE.md` for profiling methodology and results. Profile in release
mode before drawing conclusions — debug builds carry significant overhead from
alignment checks and `Result::branch` that disappear in release. The formatter
mode has not yet been profiled.
