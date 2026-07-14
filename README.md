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

The included client's default mode (`autowrap`) forwards raw PTY data, using a client-side libghostty to inject explicit line breaks where the server would have soft-wrapped when your terminal is larger than the remote terminal size (see [AUTOWRAP.md](docs/AUTOWRAP.md)).  A `cell` mode renders to a libghostty within the client and repaints dirty lines, and is the automatic fallback when your terminal is smaller than the remote's.  The older `region` mode (raw forwarding confined by scroll regions and horizontal margins) remains available but is slated for removal.  The client switches between modes dynamically.  It is not fully featured and has bugs.
