# Local SDK patch

This is the published `bullet-rust-sdk` 0.0.43 source, selected by the root
`[patch.crates-io]`. Only `src/ws/managed.rs` differs from that release.

`ManagedWebsocket::take_dropped_events()` exposes a take-and-reset counter for
server messages lost to overflow of the existing bounded event queue. Check it
after receiving an event and before interpreting that event. The private adapter
raises its existing accounting-reconciliation flag; data loss alone does not
turn a definite RPC rejection into an unknown outcome.

`Reconnecting` and `Disconnected` remain in the SAME FIFO, but cannot be dropped
due to a full data buffer: a fresh `futures::mpsc::Sender` has one guaranteed
slot (documented by `futures::channel::mpsc::channel`). Only these rare control
events use a fresh sender. No separate channel, waiting for capacity, or change
to the normal message bound is introduced.

`ManagedWebsocket::is_connected()` allows the adapter to choose a working peer
even before it drains the old socket's queued events. One atomic epoch advances
at the start and successful end of reconnect: odd means unavailable, even means
connected. `connected_epoch()` captures the current route when submitting a
command; `received_epoch()` advances by two when `recv()` returns each reliable
`Reconnecting`, identifying the connection that produced subsequent queued events.
The adapter replaces the mutable pending-RPC `interrupted` flag with its submission
epoch. Only an error reply from a LATER epoch is ambiguous. A queued old reconnect
cannot taint a new command, and an old reply preceding the break remains definite.
Readiness depends only on the actual transport, never on draining queued events:
protective commands can use a restored socket immediately.
Stopping the SDK leaves its epoch odd. This readiness snapshot does not guarantee
against a break AFTER a command was queued; original pending RPC ownership and
the SDK's unchanged exact-transaction retry handle that existing ambiguity.

No queue-size settings, reconnection backoff, protocol messages or configuration
options change. Loss accounting and connection readiness are independent for
each socket. No polling task or additional event type is needed. The adapter
uses SDK readiness instead of its own persistent `online` flags; ordinary
message loss preserves routing and definite rejects. A real reconnect still
clears subscription continuity; the existing request table identifies potentially
replayed replies without scanning/mutating pending requests on every break.

Regression tests exercise the actual SDK against a local WS server in
`crates/exchanges/bullet/src/ws_tests.rs`. On an upstream SDK update, keep this
patch only until an equivalent supported loss notification exists upstream.
