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

## Cross-namespace routing (opt-in)

Temporal namespaces are isolation boundaries. The SDK can route actor calls across namespaces, but
it is deliberately opt-in and uses activities for cross-namespace hops.

Enable it by configuring a client pool + resolver + policy on the worker set:

```go
pool := runtime.StaticClientPool{
    Default: "accounts",
    Clients: map[string]runtime.TemporalClient{
        "accounts": accountsClient,
        "cards":    cardsClient,
        "providers": providersClient,
    },
}
resolver := runtime.KindNamespaceMap{
    "Account":  "accounts",
    "Card":     "cards",
    "Provider": "providers",
}
policy := runtime.CrossNamespacePolicy{
    Enabled: true,
    Allowlist: runtime.NormalizeAllowlist(map[string][]string{
        "accounts": {"cards", "providers"},
        "cards":    {"accounts"},
    }),
}

set := runtime.NewWorkerSet(accountsClient).
    Configure(
        runtime.WithClientPool(pool),
        runtime.WithNamespaceResolver(resolver),
        runtime.WithCrossNamespacePolicy(policy),
    )
```

Notes:

- `Namespace == ""` is normalized to the pool default before comparisons, so you do not
  accidentally bridge when both sides refer to the default namespace.
- Bridge activities execute in the caller namespace and invoke the target namespace client.
- Cross-namespace is disabled by default, even if a pool is present; enable it explicitly via
  `CrossNamespacePolicy.Enabled`.
- For a minimal end-to-end snippet, see [`docs/CROSS_NAMESPACE_EXAMPLE.md`](CROSS_NAMESPACE_EXAMPLE.md).

For non-actor processes (HTTP gateways, CLI tools), install the pooled client invoker so
`actors.InvokeAsk`/`InvokeTell`/`InvokeQuery` use the same routing rules:

```go
runtime.RegisterTemporalClientPool(pool, resolver, policy)
```

## Child orchestration
## Payload codecs and encryption

- The runtime ships with a deterministic CBOR data converter. Call `runtime.DataConverter()` to
  share the same converter with clients or gateways so payloads stay compatible across every entry
  point.
- Use `runtime.ConfigurePayloadCodecs(...)` to wrap that converter with Temporal payload codecs
  (compression, encryption, etc.). Passing no codecs resets the runtime back to plain CBOR.
- `runtime.NewEncryptionCodec(key)` returns an AES-GCM codec that encrypts every payload before it
  is handed to Temporal. Histories, visibility records, and built-in logs store only ciphertext so
  secrets never appear in plaintext outside the worker.
- `runtime.NewOffloadCodec(store, runtime.OffloadCodecOptions{ThresholdBytes: ...})` stores payloads
  larger than a threshold in an external store (Redis/S3/etc.) and replaces them with lightweight
  references. `ThresholdBytes` defaults to 256 KiB. Offloaded bytes must remain available for
  workflow replay, and the codec applies to every payload (signals, queries, activities, snapshots),
  so all workers and clients must share the same data converter. Temporal applies codecs last-to-
  first on encode, so earlier codecs wrap later ones. To keep offloaded blobs encrypted, configure
  the offload codec before the encryption codec. The codec derives deterministic keys (SHA-256 by
  default) and passes them to the store; stores must treat `Put(key, payload)` as idempotent so
  replays remain safe.
- Store contract:

```go
type OffloadStore interface {
    Put(key string, payload []byte) error
    Get(key string) ([]byte, error)
}
```
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

To combine encryption with offload, place the offload codec first so encryption happens before
offload storage:

```go
offload, err := runtime.NewOffloadCodec(store, runtime.OffloadCodecOptions{ThresholdBytes: 512 * 1024})
if err != nil { log.Fatal(err) }
codec, err := runtime.NewEncryptionCodec([]byte(os.Getenv("ACTOR_PAYLOAD_KEY")))
if err != nil { log.Fatal(err) }
runtime.ConfigurePayloadCodecs(offload, codec)
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
- `actors.SpawnRemote` starts a workflow in another namespace (not a child). Use
  `actors.WithSpawnNamespace` to target a specific namespace. Starts are idempotent by workflow ID.
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
- When a target is in a different namespace, the runtime schedules a bridge activity in the caller
  namespace and performs the call using the target namespace client.
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

## Waiting for external events

`ctx.WaitForEvent(name, timeout)` suspends a command handler until an event named `name` is
delivered to the actor, or until `timeout` elapses (`<= 0` means wait forever). It is the primitive
for human-approval pauses and webhook ingestion.

- Durable and deterministic: the Temporal implementation is a `GetSignalChannel` receive plus a
  workflow timer inside a `Selector`. It survives worker restarts and replays without divergence.
- Payloads use the same CBOR data converter as commands; `actors.WaitForEventAs[T]` decodes the
  value into a typed struct.
- Timeout returns an error that satisfies `errors.Is(err, actors.ErrEventTimeout)`. Handle it in the
  handler—an unhandled plain error is treated like any other command failure by the loop.
- If the command itself has `WithTimeout`, or the workflow is cancelled, the wait unblocks with the
  cancellation error instead.
- Namespacing: event `approve` travels on the signal `__actors_event:approve`
  (`actors.EventSignalName`). Names starting with `__actors_` are rejected on both sides: an
  event may not use the prefix, and a command may not be registered under it. Within those rules an
  event cannot collide with a command or with the runtime's own signals.
- Mailbox semantics, precisely: commands delivered as **signals** (`InvokeCommand`, `testkit.When*`)
  stay queued in their channels while a handler is suspended and run after it returns, in sorted
  command-name order. Commands that arrive through the **Tell/Ask request signals or Temporal
  Updates run in their own coroutine and are not held back**: they execute concurrently with the
  suspended handler on the same state, as they always have for any blocking handler. Queries likewise
  observe whatever the suspended handler has already written. `WaitForEvent` does not change this
  model; it makes the window it opens long enough to matter, so a handler that suspends should not
  leave state half-applied across the wait.
- Events delivered before a handler waits are buffered and returned immediately; an event that
  arrives after a wait timed out stays buffered for the next `WaitForEvent` on that name.
- Continue-As-New: buffered events are not carried across a snapshot -- the drain covers command
  channels only, and Temporal offers no way to re-inject a signal into the new run. A handler that
  is **suspended on the Tell/Ask/Update path when the loop continues-as-new is abandoned**: its
  command sits in no channel to drain, its partial state is what gets snapshotted, and it never
  returns. Signal-path handlers are safe because the loop cannot reach the snapshot while one is
  running.
- `timeout <= 0` is a wait without limit. Temporal itself is fine with that -- a blocked coroutine
  completes the workflow task and history does not grow while idle -- but the loop only considers
  Continue-As-New between commands, so an actor whose handler waits without bound cannot rotate
  while asks, tells and queries keep appending history. Prefer a finite timeout and re-wait in a
  loop on `ErrEventTimeout`.
- Delivery:
  - `actors.DeliverEvent(ctx, ref, name, payload)` from clients/gateways. With the runtime's
    invokers it only signals a running workflow and never signal-with-starts one; a third-party
    `ClientInvoker` that does not implement `EventDeliverer` falls back to `InvokeCommand`, whose
    contract does start the actor.
  - `actors.SendEvent(ctx, ref, name, payload)` from inside another actor (same namespace only).
  - `testkit`: `WhenEvent(name, payload)` on scenarios, and `testkit.NewMemoryCtx` for in-process
    handler unit tests with a channel-based `DeliverEvent`.
- Do not call `WaitForEvent` from query handlers; they must not block.

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
- Offers `WhenCommand` to enqueue typed signals, `WhenEvent` to deliver external events, `Advance`
  to jump fake time, and `QueryWorkflow` to run typed queries mid-scenario.
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
- `__actors_event:<name>` (one per event name used with `WaitForEvent`)

## Extensibility

Everything flows through `actors.Description`, so the Temporal runtime and testsuite reuse the same
typed schema without reflection beyond `%T` formatting for names. While the architecture could
support other backends, the project focuses exclusively on Temporal.
