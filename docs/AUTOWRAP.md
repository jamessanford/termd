# render-mode=autowrap

Status: **proposal / not started.** This describes a new render mode. It does not
modify `render-mode=cell` or `render-mode=region`, which keep their own trade-offs.

## The problem

term attaches a client terminal to a server PTY and has to reconcile two screens
that may differ in size. Today there are two render modes, each with a hard limit:

- **`render-mode=cell`** runs libghostty over the PTY and re-renders cells to the
  client. Exact, but it fundamentally draws "the characters on the screen," so it
  loses passthrough: OSC sequences, hyperlinks, true color edge cases, sixel/kitty
  graphics, and query/response round-trips don't flow through. We want *most*
  escape codes and OSCs to pass through untouched, intercepting only a few.

- **`render-mode=region`** forwards raw PTY bytes confined to a box via DECSTBM
  (vertical scroll region) and DECSLRM/DECLRMM (horizontal margins), filtering a
  small set of sequences. It preserves passthrough, but it is a leaky abstraction:
  it multiplexes a *single* terminal's global modal state between two owners — term
  (framing the PTY into a box) and the app (which assumes it owns a full screen of
  its declared size).

The leak rate is proportional to how hard term and the app contend for the same
global state, and that contention is **not** evenly distributed:

- **Vertical (DECSTBM scroll region):** low contention. Term owns a top/bottom
  region; apps that touch it are clamped; worst case is a slightly-wrong scroll.
  Robust enough.
- **Horizontal (DECSLRM / DECLRMM):** high contention, and it is a **singleton**.
  There is exactly one set of left/right margins, and both term and the app want it.

Every fix region mode has accumulated — DECSLRM clamping, DECLRMM ownership, the
`?69l` restore, the DECSTBM re-emit, the sigwinch refresh-on-crossing, the recent
app-margin passthrough — is the *same* horizontal-margin leak surfacing through a
different sequence. It is whack-a-mole against a contended singleton, and the
contended set is not closed (save/restore cursor, DECRQM on mode 69, origin mode,
set-then-read margins, the disable/emit/re-enable dance racing across buffer
boundaries, terminals that reset margins on re-enable, ...).

We cannot sidestep this by forcing `client_cols == server_cols`: multiple clients
of differing widths can attach to one PTY (which also raises its own issues, e.g.
where OSC/query *responses* are routed when one shared stream feeds many clients).

### Key reframe: what do horizontal margins actually buy us?

Exactly **one** thing: the *implicit autowrap column*. term frames the PTY at the
top-left, so the box's left edge is always client column 1 and the server↔client
horizontal coordinate map is the identity. Absolute positioning (CUP), the scroll
region, and relative moves already work without DECSLRM when `client_cols >
server_cols`, because column N means column N on both sides. The *only* thing that
breaks without margins is that a glyph printed at server column `server_cols`
doesn't wrap — the wider client keeps going into its own column `server_cols + 1`.

So DECSLRM is a narrow autowrap-boundary hack, and we pay for it with contention
over a global singleton the app also wants. That asymmetry is the whole story.

## The proposed solution

Introduce **`render-mode=autowrap`**: forward raw PTY bytes (preserving passthrough,
like region mode) but **stop using DECSLRM entirely**. Instead, run a libghostty
over the forwarded stream to track the cursor, and **inject an explicit line break**
(`\r\n` / cursor move to next row, column 1) at the exact byte boundary where the
server *would* have soft-wrapped.

Because the client is wider than the server, it never autowraps inside the box on
its own at `server_cols`; our injected break is the only wrap. The resulting stream
uses **only absolute/explicit positioning plus injected breaks**, so it is
**client-width-agnostic**: one transformed stream is correct on *any* client of
width ≥ `server_cols`. term then retains only the cheap, uncontended vertical scroll
region (DECSTBM), and the entire horizontal-margin contention surface disappears —
deleted, not patched. The app keeps DECSLRM, `\e[s`/`\e[u` save-cursor, etc.

When `client_cols == server_cols`, no injection is needed (the client's own autowrap
lands exactly right), and the transformed stream is identical to passthrough — a
consistent special case, not a separate path.

### Why not the alternatives

- **Keep patching region mode** (e.g. disable DECSLRM / emit save-restore /
  re-enable): a treadmill against a singleton with a non-closed contended set. Only
  acceptable if region mode stays experimental and lightly used.
- **Diffing the screen to decide what to send:** an optimization of *cell* mode's
  wire protocol. It presupposes you've decided what every cell should be, i.e. you've
  already given up passthrough (graphics, hyperlinks, OSC responses) unless you model
  all of it — at which point you've rebuilt a full terminal. Optimizes bytes, not the
  property we care about.

## Feasibility: libghostty already exposes the hard parts

Verified in our fork at `examples/libghostty-rs`
(`crates/libghostty-vt/src/terminal.rs`):

- `is_cursor_pending_wrap()` — the **deferred-wrap flag** ("will the next printable
  soft-wrap?"). This is the fidelity-critical bit and it's a direct query, so we
  never re-derive deferred-wrap semantics ourselves.
- `cursor_x()` / `cursor_y()` — exact cursor cell, width-aware. The engine models
  wide chars and grapheme clustering (`GRAPHEME_CLUSTER`, mode 2027), so a 2-wide
  glyph that won't fit forces the wrap internally.
- `vt_write(&[u8])` — incremental feed. `vt_at_boundary()` — parser is in ground
  state (not mid-sequence / mid-UTF-8), i.e. when it's safe to splice in a break.
- The engine fully models DECLRMM (`LEFT_RIGHT_MARGIN`, mode 69), so its cursor
  accounting already respects margins if we ever set them.
- `on_pty_write` / `on_device_attributes` callbacks capture query responses (DA,
  DSR, ...), directly useful for routing responses per-client in the multi-client
  case instead of letting them race up one shared stream.

### The one gap (fork-shaped)

There is no "wrap happened here" / "bytes consumed until next wrap" event. The naive
implementation feeds bytes while polling `is_cursor_pending_wrap()` and injects a
break before the next printable — correct, but an FFI query per printable is chatty.
Since we own the fork, the clean fix is to **add a wrap hook upstream**: a callback
that fires with the byte offset at each soft-wrap, so the proxy splices instead of
polls. The model already computes the wrap; we're only surfacing it.

## Next steps

1. **Prototype the polling version first** (no fork changes): a standalone harness
   that feeds a recorded PTY stream through libghostty, polls
   `is_cursor_pending_wrap()` + cursor position, and produces an injected-break
   output stream. Validate against a corpus on a wider-than-server client.
2. **Build a test corpus** of wrap edge cases: exactly-`server_cols` glyphs then a
   control sequence (deferred wrap must NOT fire a spurious blank line), wide chars
   at the boundary, combining/zero-width sequences, tabs advancing past the edge,
   CR/LF interactions, and resequencing across `vt_write` chunk boundaries.
3. **Add the upstream wrap hook** to the fork (libghostty-vt + sys binding): a
   soft-wrap callback carrying the byte offset, plus whatever the polling prototype
   showed we were missing. Re-point the proxy at the hook.
4. **Implement `render-mode=autowrap`** as a new mode alongside cell/region (do not
   iterate on the existing modes). Reuse the vertical DECSTBM framing from region
   mode; drop all horizontal-margin handling.
5. **Wire multi-client response routing** via `on_pty_write` / `on_device_attributes`
   so query responses go to the right client rather than one shared stream.
6. **Decide the mode-selection policy**: when autowrap supersedes region, when cell
   is still required (e.g. truncation when a client is narrower than the server,
   which autowrap does not solve — it targets clients *wider* than the server).

## Final goal

A render mode that keeps near-total passthrough (OSC, hyperlinks, true color,
graphics, query/response round-trips) **and** is correct under width mismatch and
multiple attached clients, by reducing term's claim on global terminal state to the
uncontended vertical scroll region and replacing the contended horizontal-margin
mechanism with libghostty-driven explicit wrap injection. One transformed stream is
valid for every client of width ≥ `server_cols`; the open-ended horizontal-margin
contention surface is removed rather than patched.

## Scope / non-goals

- Does not change `render-mode=cell` or `render-mode=region`.
- Does not address clients **narrower** than the server (that needs truncation/reflow
  and remains cell mode's domain).
