# StreamMetadata Design

**Date:** 2026-05-16
**Status:** Approved

## Overview

Add a `StreamMetadata` response type to the gRPC protocol. The server sends it
unprompted to all subscribers of a PTY whenever observable PTY state changes
(size, title, subscriber count, or lifecycle). This gives clients a structured,
machine-readable side-channel alongside the raw `StreamData` byte stream.

## Proto Changes

### New enum

```proto
enum StreamMetadataReason {
  RESIZE              = 0;
  CLOSED              = 1;
  TITLE_CHANGED       = 2;
  SUBSCRIBERS_CHANGED = 3;
}
```

### New message

```proto
message StreamMetadata {
  string               pty_id    = 1;
  // NOTE: item.subscribed is always true in this context — the recipient is
  // by definition subscribed. When PtyItem is refactored into a connection-
  // agnostic PtyState plus per-client fields, StreamMetadata should use
  // PtyState instead of PtyItem.
  PtyItem              item      = 2;
  StreamMetadataReason reason    = 3;
  optional int32       exit_code = 4;  // populated only when reason == CLOSED
}
```

### TerminalResponse oneof

Add field 6:

```proto
StreamMetadata metadata = 6;
```

## Server Emission Points

Each emission broadcasts to all current subscribers of the affected PTY using
the existing broadcast path (the same channel that carries `StreamData`).

| Reason               | Where emitted                                          | Notes                                      |
|----------------------|--------------------------------------------------------|--------------------------------------------|
| `RESIZE`             | `PtyHandle::resize()`, after `ioctl` succeeds          | Sends updated cols/rows in item            |
| `CLOSED`             | `reader_thread`, after exit notification chunk is sent | exit_code set from child exit status       |
| `TITLE_CHANGED`      | `reader_thread`, inside `on_title_changed` callback    | Sends updated title in item                |
| `SUBSCRIBERS_CHANGED`| `handle_subscribe` / `handle_unsubscribe` in commands  | Sent after the subscriber set is updated   |

## Data Flow

```
PTY state changes (resize / exit / title / subscribe)
        │
        ▼
server emits StreamMetadata on the per-PTY broadcast channel
        │
        ▼
all subscribers receive it interleaved with StreamData
```

## Client Handling

`attach` in `main.rs` currently ignores any `TerminalResponse` variants other
than `StreamData` and `RefreshResponse`. It should be updated to handle
`StreamMetadata`:

- `CLOSED`: print the human-readable message already in `StreamData` (no
  change needed there) and break the receive loop cleanly.
- `RESIZE` / `TITLE_CHANGED` / `SUBSCRIBERS_CHANGED`: no action required in
  the basic attach client; structured for future UI clients.

## Future Work

- Refactor `PtyItem` into a connection-agnostic `PtyState` plus per-client
  fields; switch `StreamMetadata.item` to `PtyState`.
- `SUBSCRIBERS_CHANGED` currently carries the same `subscribed: bool` as the
  list response. Once `PtyState` exists, replace this with per-subscriber
  information (e.g. each subscriber's native terminal dimensions), since the
  server will eventually need to track client-side cols/rows to make informed
  decisions (e.g. choosing a common render size).
