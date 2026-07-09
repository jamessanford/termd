# Refresh Boundaries and Degraded Snapshots

## Overview

A refresh is a full screen redraw the daemon renders from its own libghostty
`Terminal` (`do_refresh` in `src/pty/snapshot.rs`) and hands to clients so they can paint
the current state without replaying the whole byte history. It reaches clients
two ways:

- **Per-subscriber (unary `Refresh` RPC)** — a client sends `RefreshRequest`; the
  reader renders the snapshot and emits it **inline on the data broadcast** as
  `PtyEvent::RefreshFor { subscriber_id, .. }` (`pty/mod.rs::deliver_refresh` →
  `flush_refreshes`). Each Subscribe forwarding task forwards it only if the id is
  its own. The unary RPC just acks; the snapshot arrives on the stream.
- **Broadcast** — on resize and primary↔alternate screen switch the reader pushes
  `PtyEvent::Refresh` to every subscriber (`server.rs`).

Ordering is the key invariant: the snapshot rides the **same ordered channel** as
the PTY data, sequenced by the single authority (the reader) at generation `G`. So
on the wire a subscriber sees `[…data ≤G…][snapshot][…data >G…]`. The client
therefore treats the snapshot as the authoritative baseline — it **discards
everything received before the snapshot** (subsumed by it) and resumes on the data
that follows. No generation is exposed on the wire and the client does no dedup;
`generation` stays internal to `src/pty/`, used only to pin the snapshot at a clean
boundary (below). Emitting the snapshot inline rather than on a side channel is
what guarantees this order — a separate channel could let `>G` data overtake the
snapshot. See the commit history around `RefreshFor` for the bug this fixed.

## Why refreshes are pinned at a VT ground boundary

The daemon forwards the raw PTY byte stream. A refresh snapshot effectively
*splits* that stream at a generation: everything `<= G` is folded into the
snapshot, everything `> G` is replayed live on top of it. If `G` lands in the
**middle of an escape sequence**, the client renders the snapshot (parser → VT
ground) and then resumes on the *continuation* of a sequence whose prefix it
never saw — e.g. the app wrote `\x1b[38;2;100`, the snapshot is pinned, then
`;200;50mfoo` arrives and prints as literal garbage.

So `flush_refreshes` is gated on `terminal.vt_at_boundary()`: the snapshot is
only pinned when the parser sits at ground (not mid-sequence, not mid multi-byte
UTF-8). `process_read` cooperates by splitting a read at the first ground
boundary when a refresh is pending. See `FORMATTER.md` for what the snapshot
bytes contain.

## The stall timeout and `degraded`

Pinning at a boundary assumes a boundary eventually arrives. If an app writes a
partial sequence and then goes **idle mid-sequence**, no boundary comes and a
blocked attach would hang forever. To bound that, the reader's `poll()` uses a
timeout (`REFRESH_STALL_TIMEOUT_MS`, 1s) when a refresh is pending; on timeout it
flushes the refresh **anyway**, mid-sequence.

That forced snapshot is the one case where `G` is not at a boundary. As of the
server-side change, `flush_refreshes(..., degraded)` stamps `RefreshData.degraded
= true` on exactly that path (all boundary-clean flushes pass `false`), and the
flag is carried to the client on `RefreshResponse.degraded` (proto field 8) for
both the reply and the broadcast.

**This is purely a signal.** The daemon still sends a best-effort render so the
user can *see why* the app is stuck — that was the whole point of the timeout.
The daemon does not currently do anything else about it (no automatic re-emit,
no per-client tracking). What to do with the signal is left to the client.

## The client-side problem (unsolved)

`degraded` is plumbed end-to-end but **no client consumes it yet**. The hard part
is that a degraded refresh can arrive in two very different situations, and the
right response differs:

- **On initial attach** the client has *no* state. A degraded snapshot is
  strictly better than a blank screen → it should display it.
- **Mid-stream** the client is *already in sync* via the byte stream. A degraded
  snapshot is strictly *worse* → applying it would clobber correct state and
  resume the client on an orphaned sequence tail.

And degraded refreshes really can show up mid-stream. The client requests
refreshes after attach — after a SIGWINCH (debounced) and after a SIGWINCH-driven
mode switch (`src/attach/mod.rs`, the `refresh_debounce` and `change_mode`
paths) — and those replies flow through the *same* `Response::Refresh` handler
that applies broadcast refreshes. On the wire a reply and a broadcast are
indistinguishable, and the handler currently applies every refresh
unconditionally.

So the open question is: **how should a client recover from a degraded refresh
without adding conditional "apply or ignore" logic to the steady-state path?**

## Candidate client-side solutions

These were explored during design; none is implemented. Listed roughly best →
weakest.

### 1. Scope degraded to the one blocked caller (`allow_degraded` on the request)

Key observation: **the stall timeout only needs to unblock a caller that is
*synchronously blocked*, and that is only ever the initial attach.** The
bootstrap `request_refresh` *awaits* its reply; nothing renders until it returns.
Every mid-stream refresh is fire-and-forget — the client keeps streaming (and
stays correct) while it waits, so its reply can wait for a true boundary
indefinitely.

Design:
- Add `allow_degraded: bool` to `RefreshRequest`; the client sets it `true` only
  for the bootstrap request, `false` everywhere else.
- The stall timeout flushes only the pending replies marked `allow_degraded`
  (broadcast refreshes and other replies keep waiting for ground).
- Result: `degraded` can appear on **exactly one reply in the system** — the
  blocking bootstrap one. The steady-state `Response::Refresh` handler needs zero
  new logic, and "degraded mid-stream" becomes impossible by construction.

Cost: one bool on the request, and the reader must track which pending replies
are degradable. (Deferred for now per the decision to keep the server simple and
"let the client worry about what comes back.")

### 2. Freeze-until-clean (display the degraded snapshot, then wait)

On a degraded attach: render the snapshot (user sees the stuck screen), but set
the sync baseline to a sentinel (`current_refresh_gen = u64::MAX`) so all
`Stream` is dropped. The daemon re-arms `pending_broadcast_refresh` after a
degraded stall, so at the next ground boundary it broadcasts a **clean** refresh;
the existing handler resets the baseline to a real generation and streaming
resumes. If the app is stuck forever, the client stays frozen on the preview —
which is correct, there's nothing valid to stream.

Pairs naturally with #1 (freeze only applies to the bootstrap path). On its own,
needs the daemon re-arm and one sentinel assignment.

### 3. Client re-requests a clean refresh later

Simplest reactive option: on seeing `degraded == true`, the client schedules its
own follow-up `RefreshRequest` (debounced) and ignores/freezes until a
non-degraded reply arrives. More client state (a timer, a pending flag) and a
round-trip, but no protocol change beyond the existing `degraded` flag. This is
the "let the client decide" path and is the most likely near-term direction.

### 4. Parser-state sync — REJECTED

Have the refresh carry the parser's unconsumed prefix bytes so the client can
replay them and resume seamlessly with no freeze. **Not viable cheaply:**
libghostty's `Parser.zig` is the vt100.net state machine and does *not* retain
raw bytes — it keeps parsed structure (`state`, `intermediates`, `params`/
`params_sep`, a separate `osc_parser` buffer, and UTF-8 state elsewhere).
Producing the prefix means either reconstructing canonical bytes from that
structure across every state (CSI/OSC/DCS-passthrough/APC/partial-UTF-8 — broad
surface, easy to get subtly wrong) or adding a raw accumulator threaded through
`stream.zig`'s SIMD fast paths (taxes the hot path). Either is a large libghostty
change with real correctness risk — nothing like the one-line `vt_at_boundary`
getter.

## Guiding principle

> A refresh may be answered degraded only when a client is *synchronously
> blocked* on it — which is only ever the initial attach. Everything else can
> afford to wait for a true ground boundary and should always be clean.

Keeping to that keeps `degraded` out of the steady-state path entirely, which is
what makes the client simple.

## Current status

- **Done (server):** `degraded` is computed on the stall path and carried to the
  client on `RefreshResponse` for both the reply and broadcast paths.
- **Not done (client):** nothing reads `degraded` yet. Pick a recovery strategy
  (likely #3, optionally hardened with #1) before relying on it.
