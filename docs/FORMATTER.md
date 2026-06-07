# Terminal State Serialization via VT Formatter

## Overview

The `TerminalFormatter` with `Extra::all` is already designed as a state
reconstruction mechanism — the comment on `Extra::all` in `formatter.zig` says
*"Emit everything. This reconstructs the terminal state as closely as
possible."* The migration path today is:

```
serialize:  Formatter { extra: all flags true } → format_alloc → bytes
restore:    Terminal::new(...) + terminal.vt_write(&bytes)
```

This is already wired end-to-end through the C API (`c/formatter.zig`) and
exposed in Rust via `libghostty-vt/src/fmt.rs`.

## What the formatter already emits

**Terminal-level** (`TerminalFormatter.Extra`):
- Color palette: OSC 4 for all 256 entries
- Modes differing from defaults: CSI h/l, CSI ? h/l
- Scrolling region: DECSTBM + DECSLRM
- Tabstops: CSI 3g + HTS
- PWD: OSC 7
- Keyboard: `modify_other_keys_2` via CSI > 4;2m

**Screen-level** (`ScreenFormatter.Extra`):
- Cell content, styles, hyperlinks, wide chars, graphemes, wrapping
- Cursor position: CUP
- Cursor SGR style
- Hyperlink cursor state: OSC 8
- Protection: DECSCA
- Kitty keyboard flags: CSI = u
- Charsets G0-G3 + GL/GR invocations
- Saved cursor position *(position only — not style/charset/modes; see gaps below)*

## Gaps between `Extra::all` and full migration fidelity

| State | Status | VT sequence | Effort |
|---|---|---|---|
| Window title | **Done** (`extra.title`) | `OSC 0 ; <title> ST` | Trivial |
| Dynamic FG/BG/cursor color overrides (OSC 10/11/12) | **Done** (`extra.colors`) | OSC 10, 11, 12 | Easy |
| Alternate screen when primary is active (and vice versa) | Structural gap | SMCUP/RMCUP wrap around second ScreenFormatter | Medium |
| Saved cursor: style, charset, origin, wrap, protection | Acknowledged gap | temp-set each field before DECSC, restore after | Medium |
| Viewport scroll position | Missing | no VT sequence | Medium |
| Kitty graphics image data | Not feasible via VT | APC re-upload would be required | Hard |
| Mouse shape | Missing | no standard escape | Minor / debatable |

## Task list

### Easy wins (additive changes to `formatter.zig`, following existing patterns)

- [x] **Title** — added `title: bool` to `TerminalFormatter.Extra`; emits
  `\x1b]0;<title>\x1b\\` via `terminal.getTitle()` when a title is set.
  Wired through `Extra.all`, the C struct in `c/formatter.zig`, the C header,
  and the Rust bindings.

- [x] **Dynamic color overrides** — added `colors: bool` to
  `TerminalFormatter.Extra`; emits OSC 10/11/12 for
  `terminal.colors.foreground`, `.background`, `.cursor`.  The actual field on
  `DynamicRGB` is `override` (not `current`); a color is emitted only when
  `override` is set *and* differs from `default` (see `dynamicColorOverride` in
  `formatter.zig`), so an override equal to the default — or a reset — emits
  nothing and won't clobber the receiving terminal's own default.
  Wired through `Extra.all`, the C struct, the C header, and the Rust bindings.

  **Enabled in `do_refresh` (pty.rs).**  The reconnect/refresh path sets both
  `title` and `colors` to `true`, so a reconnecting client gets the server
  app's window title and any app-set fg/bg/cursor overrides.  Unlike `palette`
  (kept `false` to avoid clobbering the client's theme), `colors` only emits an
  OSC 10/11/12 when the app changed a color *from its default*, so it won't
  override the client's own default fg/bg/cursor.

### Medium effort

- [ ] **Alternate screen** — add `alternate_screen: bool` to
  `TerminalFormatter.Extra`.  When set, format the *inactive* screen's
  `ScreenFormatter` wrapped in the appropriate enter/exit sequences so the
  receiving terminal ends up with both screens populated.  The active screen
  is formatted last so the terminal lands in the right mode.
  Concretely: if currently on primary, emit SMCUP + alternate screen content +
  RMCUP before the active-screen content; if on alternate, emit the primary
  screen content first, then SMCUP + active screen content.

- [ ] **Saved cursor completeness** — the existing `saved_cursor` extra emits
  only position.  Fix it to also temporarily set SGR style, charset state,
  origin mode, wrap flag, and protection attribute to their saved values before
  issuing DECSC, then restore each to its real value afterward.  The comment in
  `formatter.zig` at the `saved_cursor` field already describes this exactly.

- [ ] **Viewport scroll position** — if the viewport is not at the bottom
  (i.e. the user has scrolled up), record the offset.  There is no standard VT
  sequence; options are: (a) emit a private OSC and handle it in termd on
  replay, or (b) always scroll to bottom on restore and accept the loss of
  scroll position.  Option (b) is acceptable for now.

### Hard / deferred

- [ ] **Kitty graphics image data** — image pixel data lives in-memory only.
  No VT replay path can recover it.  The viable approach is an
  `on_kitty_image_export` effect callback that fires during serialization and
  lets the embedder stash image data out-of-band, paired with a
  `on_kitty_image_import` on the restore side.  Accept loss for now.

### Not needed

- **Mouse shape** — the pty process will reset this when it redraws.

## In-process fast path (future)

`Screen.clone()` (Screen.zig:442) is an in-memory deep-copy used for
resize/reflow.  It is not exposed at the C API level.  For same-process
migration (e.g. moving a pty between server threads), exposing this would skip
VT parsing entirely and be much faster for large scrollback buffers.  Not
needed for the cross-process case termd cares about now.

## Code locations

| Component | File |
|---|---|
| Formatter types and VT emission logic | `examples/ghostty/src/terminal/formatter.zig` |
| C API wrapper | `examples/ghostty/src/terminal/c/formatter.zig` |
| Rust wrapper | `examples/libghostty-rs/crates/libghostty-vt/src/fmt.rs` |
| Terminal struct (title, colors, modes, scrolling region) | `examples/ghostty/src/terminal/Terminal.zig` |
| Screen struct (cursor, saved cursor, charset, kitty kb) | `examples/ghostty/src/terminal/Screen.zig` |
