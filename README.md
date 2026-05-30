### Experimental

termd is a terminal multiplexer experiment.  The daemon renders PTYs internally to libghostty instances and serves them over [gRPC](proto/terminal.proto).  The included client displays one PTY at a time (no splits/panes yet).

termd is written using Claude Sonnet 4.6, Claude Opus 4.7, and Claude Code.

### Build/Run

Try `cargo build --release`.  You need Zig 0.15 installed.

```
./target/release/termd start &
./target/release/termd attach
```

Then use "screen"-like command keys, use `C-a ?` for help.

### Caveats

- This is experimental and does NOT correctly support all terminal codes and modes, it may fail in interesting ways.
- It expects your terminal to be a ghostty variant.  If you're not using ghostty, you could try `--render-mode=cell` for a slower fallback.

### Internals

Internals are subject to change.

- `termd start` starts a gRPC server serving [proto/terminal.proto](proto/terminal.proto)
- `termd attach` is a sample client ([src/attach/*](src/attach/))

The included client has two modes, one which renders to a libghostty within the client and repaints dirty lines, and one which mostly dumps raw data, intercepting a few things such as scroll regions when your terminal is larger than the remote terminal size.  It switches between the modes dynamically.  It is not fully featured and has bugs.
