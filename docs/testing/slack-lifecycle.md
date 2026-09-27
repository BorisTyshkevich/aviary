# Slack status and runner verification

Slack `show_typing` follows the selected agent/channel route, including when
several routes share one Slack connection. On supported surfaces, native status
uses fixed `is thinking` text and refreshes while the run is active. It does not
include tool details, paths, arguments, results, or errors. Unsupported status
surfaces stop refresh attempts. Before terminal delivery, Aviary stops further
refreshes and waits for an in-flight status request to finish. Silence and other
outcomes without a confirmed new reply clear status explicitly. These status
calls are best effort; PR1 does not add tool progress messages or reliable
activity counting across concurrent runs.

Use fake Slack transports to check status ordering and fixed public failure
messages. A runner terminal event can arrive before deferred usage or checkpoint
teardown finishes. Tests that change the global store root must call
`AgentRunner.Wait()` after receiving a terminal event and before resetting the
root; otherwise the race detector can report a store-root read/write race.

```sh
go test -race ./internal/agent -run 'TestAgentRunner_ErrorCases|TestNoProviderDoesNotExposeModelToSlackDelivery|TestPreparationErrorDoesNotReachRegisteredSlackDelivery' -count=1
```
