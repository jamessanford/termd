# TODO

## Attach: server-side "keep on exit" flag

The attach client's on-exit behavior (switch to the most recent PTY vs. keep
the dead PTY on screen until explicitly destroyed) is currently a client-side
setting, toggled with `C-a o`. Two clients with different settings attached to
the same PTY conflict: a Switch-mode client destroys the exited PTY out from
under a Keep-mode client. Once we've lived with the behavior and like it,
consider making "keep" a property of the PTY itself (set at create time and/or
toggled via RPC) so the server, not whichever client notices first, decides
whether an exited PTY lingers.

## Attach: settings/options

`C-a o` is a stopgap for flipping one option. Replace with a real mechanism
(config file and/or an in-client settings UI) as more options appear; the
client's `Settings` struct is the place they're meant to land.
