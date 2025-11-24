# Tactors SDK

Tactors is a typed, builder-first actor SDK for Go. You describe state, handlers, activities, and
runtime knobs in one fluent API and receive a deterministic actor description that every runtime can
consume—no code generation or `interface{}` plumbing. We still touch Go’s `%T` formatting to label
types, but handler routing stays strongly typed.

## What you get

- Typed actor builders: fix state with `actors.NewStateful`, get typed commands/queries/activities
  via helper embeds, and build once for every runtime consumer.
- Declarative knobs: retries, timeouts, caches, queue overrides, snapshots, and validators live on
  the builder so intent is obvious.
- Temporal-first runtime: one `actors.Description` powers worker registration, Ask/Query plumbing,
  and the deterministic testsuite—no alt runtimes or codegen.
- Ops hooks: spans/metrics around commands and cross-actor calls; rotation/versioning via
  `WithSnapshot` and `actors.Patch`.

## Quick start

1) New to the ecosystem? Read [`docs/NEW_TO_TEMPORAL.md`](docs/NEW_TO_TEMPORAL.md) and
   [`docs/NEW_TO_ACTORS.md`](docs/NEW_TO_ACTORS.md) for the quick primers.
2) Build the smallest actor in [`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md) and run it
   against the Temporal testsuite (`go test`).
3) Browse runnable samples in [`docs/EXAMPLES.md`](docs/EXAMPLES.md) (code lives at
   https://github.com/tactors/samples).

## Docs map

- Builder reference: [`docs/ACTOR_BUILDER.md`](docs/ACTOR_BUILDER.md).
- Runtime/worker behavior: [`docs/RUNTIME_TEMPORAL.md`](docs/RUNTIME_TEMPORAL.md).
- Temporal-first map: [`docs/INDEX.md`](docs/INDEX.md) and [`docs/TEMPORAL_MENTAL_MODEL.md`](docs/TEMPORAL_MENTAL_MODEL.md).
- Ops & maintenance: [`docs/DIAGNOSTICS.md`](docs/DIAGNOSTICS.md) and [`docs/CONTROL_WORKFLOWS.md`](docs/CONTROL_WORKFLOWS.md).
- Contributing: [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md).

## Testkit quick use

- `testkit.ActorTemporalScenario` runs actors against Temporal’s testsuite. Stub activities inline
  with `WhenActivity(...)`; it infers names from typed signatures or accepts explicit names.
- Inspect merged activity options with `ExpectActivityOptions` helpers before execution.

## Temporal runtime at a glance

- `runtime` registers workflows/activities from an actor description and wires Ask/Query/Spawn to
  Temporal signals, queries, and child workflows.
- `runtime.NewWorkerSet` reuses workers per queue and rotates histories automatically when
  `WithSnapshot` or `GetContinueAsNewSuggested()` triggers.

## Repository guide

- `actors/` – builder, typed helpers, and shared primitives.
- `runtime/` – Temporal runner plus typed client utilities.
- `testkit/` – deterministic harnesses (Temporal scenario and testing helpers).
- `observability/` – pluggable tracing/metrics hooks; `internal/` – supporting packages as we flesh
  out tooling.
- Samples now live in the sibling repo: https://github.com/tactors/samples.

## Testing

Run the full suite (unit tests and Temporal testsuite scenarios) with:

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

No external Temporal server is required—the testsuite handles registration, fake time, and cleanup.

## License

Released under the [MIT License](LICENSE).
