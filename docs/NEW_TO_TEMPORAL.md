# New to Temporal?

Temporal is a durable workflow engine: it replays deterministic code from an event history and
guarantees progress even when workers crash or restart. This page is a crash course for people who
have not used Temporal before but want to build actors with this SDK.

## Why Temporal

- Durable execution: workflow code resumes after restarts; timers and retries survive outages.
- Strong ordering: each workflow instance processes events in order (signals, timers, activity
  completions).
- Clear blast radius: work is scoped to a workflow ID, and queries/signals target that scope.
- Built-in time travel: histories are immutable, so debugging is reproducing from history, not
  guessing.

## Core concepts

- **Workflows** are deterministic functions. They cannot do non-deterministic I/O directly; instead
  they call activities and react to delivered events. Temporal replays workflows to rebuild state.
- **Activities** are the side-effecting pieces (DB/API calls, email, etc.). They run outside
  workflow replay and are retried with exponential backoff by default.
- **Task queues** route work to workers. Temporal distinguishes workflow task queues and activity
  task queues; names are opaque strings and can be shared by many workers.
- **Workers** poll task queues and execute workflow and activity code. A worker can host multiple
  workflow/activity types.
- **Signals** are asynchronous messages delivered to a workflow; **queries** are read-only requests
  that should not mutate workflow state.
- **IDs and runs:** workflow IDs are stable across retries and Continue-As-New; run IDs change on
  each execution attempt.
- **Timers and sleeps** are deterministic; waking up is just another event in the history.
- **Data conversion** is pluggable serialization; every client and worker must agree on the same
  converter.

## How Tactors maps to Temporal

- Each actor description becomes a workflow type. Commands map to signals; queries map to Temporal
  queries. See [Temporal → Tactors mental model](TEMPORAL_MENTAL_MODEL.md) for a Rosetta stone.
- Ask/QueryActor are deterministic workflow calls, not activities; they execute inside a workflow
  task and keep ordering guarantees.
- Activities registered on the actor are standard Temporal activities—use them for all I/O.
- `WithSnapshot` and `actors.ContinueAsNew` rotate histories via Temporal Continue-As-New, optionally
  carrying pending signals and state.
- `actors.Patch` wraps Temporal GetVersion to gate upgrades without replay explosions.

## Local development and testing

- You do not need to run `temporal-server` locally: `go test` uses Temporal’s deterministic
  testsuite. See [Examples](EXAMPLES.md) for runnable scenarios.
- To inspect histories or send ad hoc signals/queries, install [`temporalio/cli`](https://github.com/temporalio/cli)
  and point it at the testsuite endpoint printed by the harness or your own cluster.
- Worker registration and queue conventions live in [Temporal runtime](RUNTIME_TEMPORAL.md); read it
  to understand how pollers are shared and how to tune retry/timeouts.

## Operational habits

- **Determinism first:** avoid random/time/UUID calls in workflows unless wrapped in
  `actors.Effect` or activities.
- **Retries and idempotency:** Temporal retries activities by default; ensure the code behind them is
  idempotent or handles deduplication.
- **Back-pressure:** per-command `WithTimeout` and `WithSignalTimeout` bound how long workflows wait
  and how big signal backlogs grow.
- **Observability:** the runtime emits spans/metrics via [Observability](OBSERVABILITY.md) hooks;
  they only fire on non-replay paths to keep data clean.
- **Versioning:** declare patches early, let both versions coexist until old runs finish, then remove
  the old branch.

## Where to go next

- Build your first actor with [Getting Started](GETTING_STARTED.md).
- Translate Temporal workflows with [Porting guide](PORTING_TEMPORAL.md).
- Dig into worker behavior with [Temporal runtime](RUNTIME_TEMPORAL.md).
- Keep the mapping handy via [Temporal mental model](TEMPORAL_MENTAL_MODEL.md).
