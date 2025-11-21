# Typed Actor Builder Guide

This page walks through the builder-based API that powers every actor in the SDK. Treat it as a
reference once you have skimmed the [Getting Started](GETTING_STARTED.md) guide.

## Core building blocks

1. Pick a kind and state factory:
   ```go
   actors.NewStateful("ticket", func() TicketState { return TicketState{} })
   ```
2. Declare typed commands, queries, and activities by embedding the helper message structs:
   ```go
   type AssignCommand struct {
       actors.CommandMsg[AssignResponse]
       Agent string
   }
   type TranscriptQuery struct {
       actors.QueryMsg[Transcript]
   }
   ```
3. Register handlers via `actors.Command`, `actors.Query`, or `actors.Activity` inside the fluent
   `With(...)` chain. Handlers receive `actors.Ctx`, the state pointer (or value for queries), and
   the typed message.
4. Finish with `.Build()` to produce an `actors.Actor`. Everything downstream (the Temporal runtime
   and the testsuite-backed testkit) works from that description—only `%T` formatting is used to
   label types; no other runtime reflection is needed.

### Temporal translation at a glance

- Signals → `actors.Command`; per-command timeouts/retries map to Temporal policies deterministically.
- Queries → `actors.Query`; add `actors.WithCache` to mirror memoization.
- GetVersion → `actors.Patch(ctx, "id")`; declare patches on the builder.
- Continue-As-New → `WithSnapshot` or `actors.ContinueAsNew`.
- Child workflow → `actors.Spawn` / `SpawnOneShot`; provide `WithChildKind` for one-shot.

## Per-route configuration

Attach options inline to keep behavior self-documenting:

```go
actors.NewStateful("ticket", newState).
    With(
        actors.Command(handleAssign,
            actors.WithTimeout(3*time.Second),
            actors.WithRetry(actors.RetryPolicy{MaximumAttempts: 5}),
        ),
        actors.Query(handleTranscript, actors.WithCache(time.Minute)),
    ).
    Build()
```

- `actors.WithTimeout`, `actors.WithSignalTimeout`, and `actors.WithRetry` configure command-level
  policies. Signal timeouts now translate into real deadlines: pending signals are dropped once the
  timeout elapses, so use this to bound backlog growth and unblock stuck workflows.
- `actors.WithCache(ttl)` enables a deterministic query cache for a specific handler.
- Validators registered via `actors.WithValidator` run before the handler executes and can reject the
  payload deterministically.

## Cross-cutting builder options

- `WithWorkflowQueue` and `WithActivityQueue` pin the default Temporal task queues. When omitted, the
  runtime falls back to `<actor-kind>-workflow` and `<actor-kind>-activity`.
- `WithSnapshot(every, buildInit)` rotates long-running workflows using Continue-As-New. Provide how
  often to rotate (in number of processed commands) and how to rebuild the init payload from state.
- Use `actors.Patch(ctx, "change-id")` within handlers to gate new logic. It wraps Temporal’s
  `GetVersion` so replays remain deterministic by default.

## Activities

- Use `actors.Activity` inside `With(...)` to register activity handlers alongside commands/queries.
  Activity handlers receive the real Temporal activity context when running under the Temporal
  runtime.
- `ctx.Activity` and `ctx.ActivityWithOptions` mirror Temporal’s workflow helpers so you can invoke
  activities from commands with familiar options.
- Attach per-activity defaults (schedule/start-to-close, heartbeat, retry, task queue) via
  `actors.WithActivityDefaults(...)` when registering; the runtime applies them to every call unless
  you override via per-call options.
- `actors.RunActivity` / `actors.RunActivityBackground` look up the activity name from the payload
  type, avoiding string constants when calling from workflows. Use
  `actors.RunActivityNamed` / `actors.BackgroundActivity` when you need to pin a specific route.
- `actors.RunActivity` / `actors.RunActivityNoResult` wrap `ctx.Activity` and decode typed responses.
  Embed `actors.ActivityMsg[Resp]` in each request struct so the helper can infer the expected type.
- `actors.BackgroundActivity` launches fire-and-forget work, logging failures without blocking the
  command loop.
- Responsiveness trade-off: waiting on activities, sleeps, or cross-actor asks inside a handler
  blocks the workflow’s signal loop—that’s allowed but serializes other signals/commands. When you
  want the loop free, schedule work (`ctx.Activity`, `ctx.BackgroundActivity`, child workflows) and
  return; expose progress via queries or callbacks when the work completes.

## Child workflows and cross-actor calls

- `actors.Spawn` starts a long-lived child workflow. Combine with `actors.WithChildName`,
  `actors.WithChildTaskQueue`, or `actors.WithChildWorkflowIDReusePolicy` for advanced routing.
- `actors.SpawnOneShot` launches a child, sends exactly one command, and returns the typed response.
  Provide `actors.WithChildKind` so the runtime knows which actor description to spin up.
- `actors.Tell` sends typed commands to other actors via signals; `actors.Ask` executes a command and
  waits synchronously for the typed result without creating a child workflow.
- `actors.QueryActor` runs a read-only ask/reply protocol inside a single workflow task. Useful when
  you need to inspect another actor’s state without waiting for signals.

Message metadata (available via `actors.Message(ctx)`) now enforces the optional `Deadline` and
`RetryBudget` fields. When you set these from callers, each hop decrements the budget and refuses to
process expired messages, making it easier to propagate TTLs and back-pressure across actors.

## Workflow utilities

- `actors.Effect(ctx, key, func() error)` deduplicates side effects across replays and
  Continue-As-New. Wrap non-deterministic work (activities, API calls) inside the effect callback.
- `actors.ContinueAsNew(ctx, init)` restarts the workflow with a new init payload and clears signal
  backlog.
- `actors.RequestContinueAsNew(ctx, ref, ...)` lets orchestrators ask other actors to snapshot and
  rotate immediately, optionally overriding the next init payload.
- `actors.Memo(ctx)`, `actors.UpsertSearchAttributes`, and `actors.SearchAttributes` expose platform
  metadata in a typed manner.

## Common recipes

- **Bound long handlers:** `actors.WithTimeout` on commands; combine with `actors.WithSignalTimeout`
  to drop stale signals instead of flooding backlogs.
- **Deterministic retries:** `actors.WithRetry` per command; non-retryable errors via
  `actors.NonRetryable(err)`.
- **Per-call queue override:** `actors.WithActivityTaskQueue` / `actors.WithChildTaskQueue` for a
  single call; use builder-level `WithActivityQueue` / `WithWorkflowQueue` for defaults.
- **Feature gates:** declare `builder.DeclarePatch("id", defaultOn)` and guard code with
  `actors.Patch(ctx, "id")`; inspect via diagnostics queries.

## Examples worth studying

- `examples/spawn` – long-lived and one-shot children, `Tell`, and `QueryActor` across workflows.
- `examples/telegram` – per-command retries, activities, ask/query plumbing, and a Temporal test.
- `examples/orders`, `examples/ticketing`, `examples/hello-system` – smaller actors focused on state
  mutation, caching, and typed clients.
- `cmd/worker` – a minimal worker binary that wires multiple actors into Temporal queues.

## Migration notes

- `actors.WithChildKind` is mandatory for `SpawnOneShot` so the runtime can look up the callee’s
  description.
- Cross-actor helpers rely on the shared description registry. Every Temporal runner automatically
  registers an actor kind, so other workflows can find it at runtime.
- Reserved signal names (`__actors_query_request`, `__actors_query_reply`, `__actors_ask_request`,
  `__actors_ask_reply`) power the ask/query protocol—avoid reusing them directly.

## Exporting metadata and manifests

- Every description exposes `desc.Metadata()` for tooling that needs to inspect handler names,
  request/response schemas, declared patches, and queue overrides. The metadata is immutable, so
  you can safely diff it between commits.
- `actors.MarshalDescription(desc)` emits a canonical JSON manifest with a schema hash and the full
  metadata surface. Use it to generate review artifacts or feed framework registries without
  touching reflection.
- See [`docs/DIAGNOSTICS.md`](DIAGNOSTICS.md) for details on metadata fields, manifest hashes, and
  how to consume them from CLIs.

More sections will land as we expand parent/child metrics and deeper Temporal tooling. See
[`docs/OBSERVABILITY.md`](OBSERVABILITY.md) for the current tracing/metrics hooks. Contributions are
very welcome!
