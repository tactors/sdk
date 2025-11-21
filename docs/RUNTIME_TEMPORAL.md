# Temporal Runtime Overview

The Temporal runtime consumes the same typed actor descriptions produced by the builder, registers
workflows/activities automatically, and drives the signal loop that powers commands. This guide
covers how it is wired together and what knobs you can use.

## Description-driven execution

- `runtime.NewRunner(actor)` clones the actor’s `actors.Description`, stores it in a registry keyed
  by actor kind, and returns a runner with `Workflow()` / `Activities()` helpers.
- When a runner registers a workflow, the actor kind becomes the Temporal workflow type. Activities
  declared via `actors.Activity` are registered verbatim so you can attach them to any worker.
- Because runners operate purely on descriptions, the exact same actor works with the Temporal
  runtime and the deterministic testsuite without runtime reflection beyond `%T` formatting used to
  label types. The SDK is intentionally Temporal-first.

## Worker registration and queues

- `runtime.NewWorkerSet(client, cfg)` maintains one workflow worker and one activity worker per
  queue. Calling `Register(actor, runtime.WorkerConfig{})` wires the actor into `<kind>-workflow`
  and `<kind>-activity` by default.
- Builders can override these defaults globally via `WithWorkflowQueue` / `WithActivityQueue`. Per
  call, `actors.WithChildTaskQueue` and `actors.WithActivityTaskQueue` still take precedence.
- Multiple actors can share queues; the worker set reuses pollers automatically.
- `WorkerSet.StartAll(ctx)` and `StopAll()` start/stop every registered worker. Cancelling the
  context passed to `StartAll` gracefully shuts everything down. Running the same binary multiple
  times is safe because Temporal load-balances across pollers.
- `WorkerConfig.WorkerOptions` passes through to Temporal worker options (sticky queues, interceptors,
  concurrency limits). Use it exactly as you would with `worker.New` in plain Temporal code.
- Queue conventions for multi-team setups: keep `<team>-<domain>-<kind>-workflow` / `-activity` or
  override per actor; the registry records the queue names so discovery tools can render them.
Example bootstrap:

```go
client, _ := client.Dial(client.Options{})
set := runtime.NewWorkerSet(client, runtime.WorkerConfig{
    WorkerOptions: &worker.Options{Identity: "actors-worker"},
})
set.Register(telegram.TelegramSupportActor(), runtime.WorkerConfig{
    WorkflowQueue: "support-wf",
    ActivityQueue: "support-act",
})
set.Register(orders.OrderActor(), runtime.WorkerConfig{})
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
if err := set.StartAll(ctx); err != nil {
    log.Fatal(err)
}
<-ctx.Done()
```

## Payload codecs and encryption

- The runtime ships with a deterministic CBOR data converter. Call `runtime.DataConverter()` to
  share the same converter with clients or gateways so payloads stay compatible across every entry
  point.
- Use `runtime.ConfigurePayloadCodecs(...)` to wrap that converter with Temporal payload codecs
  (compression, encryption, etc.). Passing no codecs resets the runtime back to plain CBOR.
- `runtime.NewEncryptionCodec(key)` returns an AES-GCM codec that encrypts every payload before it
  is handed to Temporal. Histories, visibility records, and built-in logs store only ciphertext so
  secrets never appear in plaintext outside the worker.
- Remember to hand the configured converter to your clients as well:

```go
codec, err := runtime.NewEncryptionCodec([]byte(os.Getenv("ACTOR_PAYLOAD_KEY")))
if err != nil { log.Fatal(err) }
runtime.ConfigurePayloadCodecs(codec)

cli, err := client.Dial(client.Options{
    HostPort:      "temporal:7233",
    DataConverter: runtime.DataConverter(),
})
if err != nil { log.Fatal(err) }
set := runtime.NewWorkerSet(cli)
```

## Workflow lifecycle

1. **Start envelope.** Every workflow begins with a deterministic envelope that carries the parent
   ref, init payload, and optional one-shot command. The Temporal data converter preserves it so
   replays decode the exact same struct.
2. **State initialization.** `StateFactory` runs before `OnStart`. If `OnStart` returns a value, it
   replaces the initial state pointer.
3. **Query registration.** Each query defined in the description becomes a Temporal query handler.
   Requests are decoded through typed factories so handlers only see Go structs.
4. **Signal loop.** Commands map to Temporal signals. The runtime listens on a selector, decodes the
   payload via the typed factory, and invokes the handler. Per-command timeouts and retries are
   enforced with workflow contexts and `workflow.Sleep`.
5. **One-shot mode.** When a `startEnvelope` includes a `oneShotCommand` (used by `SpawnOneShot`),
   the workflow executes exactly once without entering the signal loop and returns the typed result.

## Continue-As-New and snapshots

- `WithSnapshot` configures automatic rotation by serializing state plus the pending signal backlog
  into workflow memo and telling the runtime how to build the next init payload.
- Even without `WithSnapshot`, the runtime listens for Temporal’s `GetContinueAsNewSuggested` signal
  (Temporal’s own rotation hint). Before rotating it memoizes state/signals and restarts the workflow
  with the original init payload so no messages are lost.
- External orchestrators can call `actors.RequestContinueAsNew(ctx, ref, ...)` to trigger the same
  snapshot/Continue-As-New path on another actor, optionally overriding the next init payload.

## Child orchestration

- `actors.Spawn` spins up a child workflow using Temporal’s child APIs. The runtime derives workflow
  IDs or honors overrides provided through `actors.WithChildName`. The returned `actors.Ref` includes
  both the kind and instance ID for downstream `Tell`/`Ask` calls.
- `actors.SpawnOneShot` requires `actors.WithChildKind` so the runtime can find the child’s
  description. It starts a child, injects a one-shot command into the start envelope, waits for the
  workflow to finish, and unwraps the typed response.

## Cross-actor communication

- `actors.Tell` signals another actor by looking up its description in the registry and using the
  typed metadata stored on each command.
- `actors.Ask` sends a command, waits for the reply via the deterministic ask protocol
  (`__actors_ask_request` / `__actors_ask_reply` signals), and surfaces the typed result. No child
  workflow is spawned.
- `actors.QueryActor` follows a similar pattern for read-only handlers using
  `__actors_query_request` / `__actors_query_reply`.
- Each envelope carries the caller ref, correlation ID, and deadline. The runtime bounds the number
  of inflight exchanges and times out replies after one minute so a misbehaving callee cannot jam a
  workflow forever. Validation hooks fire before the handler executes; failures return deterministic
  errors to the caller.
- `actors.Effect` records successful keys in workflow memo (with optional TTL). Replays skip work for
  previously completed keys—wrap activities or external calls inside the effect callback to keep
  them effectively-once.
- Interceptors: use Temporal worker interceptors via `WorkerOptions` for logging/metrics you already
  rely on; Tactors observability hooks still fire separately and only outside replay.
- Error helpers keep intent explicit:
  - `actors.BusinessError` propagates typed failures without failing the workflow.
  - `actors.NonRetryable(err)` tells the runtime to stop retries immediately.
  - `actors.RetryAfter(err, d)` requests a durable delay before reprocessing the same message.

## Diagnostics queries

- Every actor automatically responds to two reserved Temporal queries:
  - `actors.DiagnosticsPatchesQuery` returns the patch metadata embedded in the description (ID,
    default on/off, optional note) via `actors.QueryPatchReport(ctx, ref)`.
  - `actors.DiagnosticsSnapshotQuery` reports the live snapshot/Continue-As-New counters by exposing
    the `actors.SnapshotInfo` captured on the workflow (`actors.QuerySnapshotReport(ctx, ref)` and
    `actors.Snapshot(ctx)` inside handlers).
- These routes are registered by the runtime, so you do not need to modify the builder or your
  actor’s query list; simply invoke them from tooling/CLIs using the helpers above.
- The diagnostics queries use reserved names (`actors_diag_patches` and
  `actors_diag_snapshot`)—avoid registering handlers under the same identifiers.

## Workflow utilities

- `actors.ContinueAsNew(ctx, init)` restarts the workflow manually as needed.
- `actors.Memo(ctx)` exposes typed workflow memo fields.
- `actors.UpsertSearchAttributes` / `actors.SearchAttributes` manage Temporal search attributes.
- `actors.Patch(ctx, changeID)` wraps `workflow.GetVersion` so upgrades stay deterministic by default.
- `actors.Snapshot(ctx)` exposes the current snapshot cadence, commands processed since the last
  rotation, and Continue-As-New counts for use in admin/observability handlers.

## Worker diagnostics

- `runtime.WorkerSet.HealthSnapshot()` returns the queues, running state, and actor kinds assigned
  to every workflow/activity worker so control planes can render fleet status without poking
  individual pollers.
- `observability.Listener` implementations can opt into worker lifecycle events by also satisfying
  `observability.WorkerLifecycleListener`. The runtime emits `WorkerEvent` callbacks whenever a
  worker starts, stops, or fails to boot for a given queue (including the set of actor kinds assigned
  to that worker).

## Testkit integration

`testkit.NewActorTemporalScenario(...)` wires the description into Temporal’s testsuite so you can
drive workflows deterministically:

- Registers workflows and activities automatically, assigning a deterministic workflow ID.
- Offers `WhenCommand` to enqueue typed signals, `Advance` to jump fake time, and `QueryWorkflow` to
  run typed queries mid-scenario.
- `Run(t)` executes the workflow and returns the workflow error plus the captured outcome for further
  inspection.

## Reserved signal names

The runtime owns these signals—avoid defining your own handlers under the same names:

- `__actors_query_request`
- `__actors_query_reply`
- `__actors_ask_request`
- `__actors_ask_reply`
- `__actors_continue_request`
- `__actors_continue_reply`

## Extensibility

Everything flows through `actors.Description`, so the Temporal runtime and testsuite reuse the same
typed schema without reflection beyond `%T` formatting for names. While the architecture could
support other backends, the project focuses exclusively on Temporal.
