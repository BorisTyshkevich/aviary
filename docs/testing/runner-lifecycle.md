# Runner lifecycle and shutdown

Prompt submission returns an admission result. `accepted` carries a run ID and
means the runner owns a terminal callback or a reported startup error. A caller
must not submit that request again. `rejected_stopping` means the runner took no
ownership, created no checkpoint, and will not call its stream consumer. Channel
ingress revalidates the current route and runner once after this rejection; if
the retry is also rejected or the route disappeared, it attempts the fixed
“Restarting; please resend your request.” notice at the original target. MCP
and scheduler callers also finish their waiting request on rejection.

Runner cancellation records `user_stop` for an explicit stop or canceled
request context, and `runner_stop` for runner replacement or server shutdown.
The cause is attached to the stop event. Replayable non-Slack prompt
checkpoints survive `runner_stop`; user stops retire them. Scheduled jobs retain
their queue recovery policy. The run ID names the checkpoint, while the
original prompt message ID remains in the record for replay.

On SIGINT, SIGTERM, `aviary stop`, or a hard restart, the server closes Slack
Socket Mode ingress and waits for already acknowledged handlers to hand their
work to a runner. The dispatch gate serializes acknowledgement with closure:
after it closes, new envelopes are neither acknowledged nor routed. The server
then stops runner admission, requests runner cancellation, and drains callbacks
and checkpoint writes for up to 45 seconds. Outgoing Slack Web API access and
persistence remain available during that drain. A timeout is logged; it does
not declare surviving goroutines dead. The same-process recovery registry
blocks replay of their checkpoints until their callbacks and checkpoint
teardown actually finish. A hard restart waits for the old socket to close
before the replacement opens. Shared Slack connection replacement on reload
uses the same socket ordering. If the old socket cannot be confirmed closed
within the handoff deadline, the replacement is withheld and the error is
logged; a later reconcile or restart is needed to retry the handoff (tracked
as [issue #41](https://github.com/lsegal/aviary/issues/41)).

The scheduler closes its claim loop before channel ingress quiescence, without
canceling jobs already claimed. A job remains owned in this process until its
accepted run has delivered its terminal callback and the queue outcome is
written. Another Scheduler in the same process skips that job during startup
recovery and queue claiming, even if shutdown's drain deadline elapsed. A
process exit clears this ownership, so the file-backed queue still recovers
interrupted jobs on the next start.

The server owns separate channel and execution contexts during shutdown, so
root cancellation does not prematurely cancel admitted work or Signal transport
before the drain. A caller's own canceled request context still stops its run.
Signal has no Slack-style Socket Mode acknowledgement gate: its daemon and
transport remain available until the bounded runner drain ends, then channel
teardown begins. The server gate stops new non-Slack runner submissions after
Slack ingress handoff and attempts a fixed resend notice at the original target
while the transport is still available. The focused tests cover runner ownership, admission fallback,
checkpoint exclusion, and Slack socket ordering. They do not establish an
end-to-end guarantee for a live Signal daemon or for an OS-delivered process
signal; those require an external integration environment. A server-level root
cancellation test verifies that an admitted channel run keeps its terminal
callback and checkpoint owner through shutdown.

Slack Socket Mode is not a durable incoming queue. Closing ingress gives Slack
an opportunity to retry unacknowledged events, but no restart-length retry
window is guaranteed. An abrupt crash after acknowledgement and before the
first checkpoint write can still lose that event. Do not treat the in-memory
live-run registry as durable across process exit. Slack checkpoint disposition
and terminal delivery recovery are covered by the separate Slack recovery
change.

The Slack SDK can queue an acknowledgement for its websocket writer before the
bytes reach Slack. Socket closure can leave that receipt uncertain: a replacement
socket may see a retry after the old handler already admitted the run. The gate
prevents *new acknowledgement attempts* after closure; it does not guarantee
wire-confirmed acknowledgement or exactly-once execution.
