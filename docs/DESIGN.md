Let's create a program that runs as a gRPC bidi stream server, and
implements those RPCs to manage terminals (PTYs) that are hooked up
to libghostty-rs instances.

Reference source of libghostty-rs is in examples/libghostty-rs, an example of a program that uses libghostty in a similar way to our program is in examples/zmx, an example of a similar subset of the functionality we want is in examples/hauntty

Our program will:
  - be written in Rust, use libghostty-rs 
  - have a CLI with a "start" arg to start it as a daemon (and in the future act as a client to the daemon, but not yet)
  - expose gRPC bidi endpoint over a listening socket and HTTP
  - have a hardcoded secret for authentication during development
  - manage a set of underlying PTYs and libghostty-rs instances attached to each PTY.
  - design a nice extendible struct to hold all of these active PTYs/libghostty-rs instances
  - new PTYs must be allocated and destroyed in a systemd friendly way (in the future we'll want to support macOS)
  - wire up the gRPC streaming to libghostty-rs following our protobuf spec
  - keep the "main CLI", "gRPC server", "implementations of commands", "pty management" in separate files
  - this is a proof of concept, it's OK to have a skeleton implementation
  - once we've done this, add "list", "create", and "destroy" args to the CLI to begin basic validation of the daemon


Here's a sketch of what the gRPC prptobuf might look like.  I'm happy to accept suggestions and iterations.


service TerminalService {
  rpc Stream(stream TerminalCommand) returns (stream TerminalResponse);
}

TerminalCommand
  - list ptys
  - create new pty
  - destroy pty
  - subscribe to pty
  - unsubscribe to pty

  Commands that apply to subscribed PTYs (these will include PTY ID)
    - send characters to pty
    - send new resolution
    - send new title string
    - request full screen refresh
    - FUTURE: Maybe support OSCs more directly

TerminalResponse will have a bunch of things it might include:

TerminalCreateResponse
  PTYItem
    PTY ID
    hostname and underlying real pty name
    resolution
    reference title string (default to real pty name like /dev/pts/30)

TerminalListResponse
  repeated PTYItem
  would be nice to also mark which ones you are subscribed to already

# For generic commands like destroy/subscribe/unsubscribe
TerminalCommandResponse
  PTY ID
  success bool
  optional error string


TerminalStream
  - PTY ID
  - streamed ASCII/ANSI/UTF data (to dump directly into a local libghostty)
    - which should include a "generation ID" that is monotonically increasing
    - may have occasional confirmation of cursor location (prevent desyncs) (ok to leave unimplemented here)

TerminalRefreshResponse
  - PTY ID
  - full screen refresh data (OK to use raw data here, like the streamed response above)
    - also includes cursor location
    - we may need to include "state" information in the future (the "state of the libghostty instance") but don't worry about it yet
  - include a "generation ID" here so we know exactly when it happened


