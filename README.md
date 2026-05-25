### Experimental

termd is a terminal multiplexer experiment.  A daemon renders PTYs to libghostty instances and provides a [gRPC server](proto/terminal.proto) to access and stream the PTYs.  The included client displays one PTY at a time (doesn't do splits/panes yet)

The included client can be run remotely (ssh into remote machine and run "termd attach") or locally to speak gRPC with a remote server (try forwarding the gRPC port over SSH).  In the future the client might use HTTP/3 and connection migration.  A client might even connect to multiple remote servers.

### Build/Run

Try `cargo release-static` to build a version with a static libghostty linked in.  You need Zig 0.15 installed.

```
./target/release/termd start &
./target/release/termd attach
```

Then use "screen"-like command keys, use <Control-A ?> for help.

### Caveats

- This is experimental and does NOT correctly support all terminal codes and modes, it may fail in interesting ways.
- It expects your terminal to be a ghostty variant.  If you're not using ghostty, you could try `--render-mode=cell` for a slower fallback.

### Internals

Internals are subject to change.

- `termd start` starts a gRPC server serving [proto/terminal.proto](proto/terminal.proto)
- `termd attach` is a sample client ([src/attach/*](src/attach/))

The idea is that a "termd" daemon could be running on many servers, that your terminal program could incorporate
the gRPC protocol, and that remote windows could then appear fully native.

The included client has two modes, one which renders to a libghostty within the client and repaints dirty lines, and one
which mostly dumps raw data, intercepting a few things such as scroll regions when your terminal is larger than the
remote terminal size.  It switches between the modes dynamically.  It is not fully featured and has bugs.

It has [hardcoded](src/attach/input.rs) "screen"-like keybindings, see <Control-A ?>
