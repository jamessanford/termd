# Known gaps: client terminal state across PTY switch / refresh / exit

The refresh path (`src/pty/snapshot.rs:do_refresh`) and the client reset
(`RESET_TERMINAL_MODES` in `src/attach/mod.rs`) follow a clear-then-restore
pattern: the reset clears client state to a known baseline, and the formatter
re-emits whatever the target PTY has set. The formatter emits nothing for
default/unset values (`dynamicColorOverride`, the mode table, keyboard flags all
share this emit-nothing-when-unset asymmetry), so every piece of state an app
can change needs an explicit clear on our side or it leaks across PTY switches,
into the client UI screens, and past session exit.

Fixed so far: mouse/keyboard modes, kitty keyboard + modifyOtherKeys, DECSCUSR,
charsets, DECSTBM/DECLRMM, dynamic colors (OSC 10/11/12), title (OSC 0, with an
xterm title-stack push/pop around the session), hyperlinks (OSC 8), palette
redefinitions (OSC 104), synchronized output (?2026).

Still open, roughly by impact:

## Tab stops

`tabstops: false` in `do_refresh` is deliberate on the restore side (replaying
tab stops moves the cursor, corrupting the final position), but there is no
clear either: an app that runs `TBC 3` (clear all) or sets custom stops via HTS
leaves the client's tabs broken for every later PTY and after exit.

Clear-side fix is feasible: DECST8C (`CSI ? 5 W`) resets stops to every 8 and is
implemented by ghostty (`stream.zig` → `tab_reset`) and xterm. Restore-side
(re-establishing a PTY's custom stops on attach) stays a known limitation until
there is a formatter-level mechanism that doesn't disturb the cursor.

## OSC 7 working directory

`pwd: false` and no clear: the host terminal keeps believing the cwd is the
last PTY's (affects "open new tab in same directory"). An empty `OSC 7` is the
usual clear, but terminal support for the empty form varies — verify before
shipping, and consider restore-side (`pwd: true`) at the same time.

## Preamble vs RESET_TERMINAL_MODES asymmetry

The `do_refresh` preamble explicitly disables only mouse 1000/1002/1003/1006
plus the keyboard protocols before relying on `modes: true` to restore; the
client-side `RESET_TERMINAL_MODES` additionally covers 1004/1015/1016/2004/1049
and more. A mid-attach refresh (e.g. DataLost resync) applies only the preamble,
so a client/server mode desync there can leave stale focus-reporting or
bracketed paste. In steady state the client tracked the same PTY's stream, so
mismatches need a cut/lossy stream to arise. Also: neither list disables `?9`
(X10 mouse) or `?1005` (UTF-8 mouse).

## pending_wrap

Existing TODO in `do_refresh`: the formatter can't express "cursor at last
column with pending wrap", and CUP clears the flag, so after a switch the next
print overwrites the last cell instead of wrapping. Needs a formatter-level
mechanism (e.g. print+backspace at the last column).

## Kitty graphics / sixel images

Images drawn by one PTY may survive the refresh's `2J` on some terminals
(kitty-protocol placements have their own lifetime rules). Unverified. If image
protocols are in scope, the reset likely needs a kitty delete-all
(`ESC _ G a=d,d=A ESC \`) and there is no restore story at all.

## Error-path session exits

`run()` in `src/attach/mod.rs` reaches its title-stack pop (`CSI 23;0t`) and
`move_terminal_end()` only on the normal exit path; a `?` early-return (e.g.
handler creation failure) skips both, leaving the host title cleared. The
pre-existing `move_terminal_end` skip has the same shape; both belong in a
guard/Drop if this is worth fixing.
