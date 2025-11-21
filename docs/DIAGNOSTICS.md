# Diagnostics, Metadata, and Worker Health

This guide explains how to surface actor metadata, emit deterministic manifests, inspect runtime
registries, and query live workflows for patch/snapshot status. It also covers the new worker health
surfaces exposed by the Temporal runtime so control planes can see the state of every poller.

## Actor Metadata & Manifests

- Every `actors.Description` exposes `desc.Metadata()` which returns an `actors.ActorMetadata`
  snapshot. It contains handler names, request/response types, timeouts, retries, activity schemas,
  and the list of declared `actors.Patch` identifiers. The metadata struct is immutable so callers
  can diff it safely.
- `actors.MarshalDescription(desc)` produces a canonical JSON manifest containing the metadata plus a
  schema hash. The hash is stable across builds/Go versions and makes it trivial to check a manifest
  into Git or compare two revisions when reviewing a rollout.
- Use `actors.UnmarshalDescription(data)` to load the manifest back into a typed
  `actors.DescriptionManifest`. Calling `.Metadata()` on the manifest returns the same struct as
  `desc.Metadata()`, which allows CLIs to inspect manifests without touching application binaries.

Example (CLI extract):

```go
desc := actor.Spec()
payload, _ := actors.MarshalDescription(desc)
_ = os.WriteFile("diag/greeter.json", payload, 0o644)
```

## Runtime Registry Access

- The SDK now exposes a public registry in `actors/registry.go`. `actors.RegisterActorDescription`
  and `actors.RegisterDescription` install actors into the default registry, while `actors.LookupDescription`
  and `actors.RegisteredActors()` provide read-only views for tooling.
- The Temporal runtime calls `actors.RegisterDescription` automatically when a runner is created, so
  control planes can enumerate registered kinds without custom reflection (we only rely on `%T`
  formatting for type names). You can also swap the global registry by calling
  `actors.SetDefaultRegistry(customRegistry)` from tests or bespoke hosts.
- Use the registry to keep external discovery services in sync _without_ touching the runtime
  internals: register descriptions when a worker starts and remove them when the worker stops.

## Correlation & Message Headers

- Correlation metadata is modeled by `actors.CorrelationData`. Call `actors.Correlation(ctx)` inside a
  handler to read saga IDs or tracing span identifiers that were propagated with the message.
- To override the correlation state before calling another actor (for example, to start a new saga),
  call `actors.SetCorrelation(ctx, data)` and the runtime will propagate the mutation through
  `Ask`, `Tell`, `Spawn`, and `RequestContinueAsNew`.
- Client callers can also set `WithCorrelationID(...)` when using `actors.InvokeAsk`. Frameworks
  should favor deterministic IDs so traces line up with observability backends.

## Patch & Snapshot Reports

- Workflows automatically register two diagnostics queries:
  - `actors.DiagnosticsPatchesQuery` (string `actors_diag_patches`) returns every declared patch ID
    along with whether it defaults to on/off.
  - `actors.DiagnosticsSnapshotQuery` (string `actors_diag_snapshot`) returns the live
    `actors.SnapshotInfo` for the workflow (snapshot cadence, Continue-As-New count, commands
    processed since the last rotation, etc.).
- Tooling can query these routes from any process using the helpers:

```go
ref := actors.ARef("orders", "order-123")
patchReport, _ := actors.QueryPatchReport(ctx, ref)
snapReport, _ := actors.QuerySnapshotReport(ctx, ref)
```

- Inside handlers you can inspect snapshot state via `actors.Snapshot(ctx)` to gate long-running
  work or emit custom metrics.

## Worker Health & Lifecycle

- `temporal.WorkerSet.HealthSnapshot()` returns a `WorkerHealthSnapshot` struct describing every
  workflow/activity queue, which actor kinds are bound to it, and whether the worker is currently
  polling. Call this from control planes to render dashboards or power readiness probes.
- `observability.Listener` implementations can opt into worker lifecycle events by additionally
  implementing `observability.WorkerLifecycleListener`. The runtime emits `WorkerEvent` callbacks
  when a worker starts, stops, or fails to boot so orchestration services can react deterministically.
  The events include the queue name, worker role (workflow/activity), and the actor kinds assigned to
  that queue.
- Health snapshots and worker events are read-only and thread-safe; the worker set holds the
  necessary locks while capturing snapshots so callers do not need to add extra synchronization.

## Ops Playbook (Temporal Users)

- **Backlogs growing:** check `actors_diag_snapshot` for `CommandsSinceLastSnapshot` and
  `RequestedRotations` to see if Continue-As-New is stuck; ensure `WithSignalTimeout` is set on noisy
  commands so stale signals are dropped.
- **Handlers timing out:** correlate command spans/metrics with per-command timeout settings; use
  `actors.WithRetry` and `actors.NonRetryable` to align with Temporal retry policies rather than
  failing workflows.
- **Patches during rollout:** query `actors_diag_patches` to confirm which workers flipped a patch
  before enabling it by default; gate risky logic with `actors.Patch(ctx, id)`.
- **Worker health:** poll `WorkerSet.HealthSnapshot()` for missing pollers or queue/kind mismatches;
  alert when a queue shows no pollers or when activity/workflow queues diverge unexpectedly.
- **Suggested alerts:** snapshot lag (commands since last rotation) above your cadence target; worker
  poller count hitting zero; ask/query failure rate spikes on a specific actor kind.
