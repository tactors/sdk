# Temporal-First Entry Point

For teams already running Temporal. Start here if you know signals/queries/Continue-As-New and want fast adoption with typed actors.

## Quick Path

- Register one actor and run a worker: see [Getting Started](GETTING_STARTED.md).
- Exercise ask/query from CLI or a test: see [Runtime & Workers](RUNTIME_TEMPORAL.md) and [Examples](EXAMPLES.md).
- Reuse your existing activities and retry/timeouts: see [Porting a Workflow](PORTING_TEMPORAL.md).

## What to Read

- **Mental model:** [Temporal → Tactors mapping](TEMPORAL_MENTAL_MODEL.md) (workflow type → actor kind, signal → command, GetVersion → Patch, Continue-As-New → WithSnapshot).
- **Porting guide:** [Porting Temporal workflows](PORTING_TEMPORAL.md) (translate signals/queries/activities/retries/search attributes/memo).
- **Getting started for Temporal users:** [Getting Started](GETTING_STARTED.md) (minimal delta from standard worker code; quick ask/query).
- **Builder reference:** [Actor builder](ACTOR_BUILDER.md) (per-command retries/timeouts/cache, child workflows, asks/queries).
- **Runtime & workers:** [Temporal runtime](RUNTIME_TEMPORAL.md) (queue conventions, worker options/interceptors, data converter).
- **Ops & troubleshooting:** [Diagnostics](DIAGNOSTICS.md) (patch/snapshot queries, worker health), [Control workflows](CONTROL_WORKFLOWS.md) (durable cadences).
- **Telemetry:** [Observability](OBSERVABILITY.md) (span/metric fields, replay filtering, OTel stitching).
- **Runnable samples:** [Examples](EXAMPLES.md) grouped by Temporal task (fan-out asks, child orchestration, control cadence). More end-to-end apps live at https://github.com/tactors/samples.

## Fast Commands

```bash
GOCACHE=$(pwd)/.gocache go test ./examples/greeter   # smallest actor, Temporal testsuite
GOCACHE=$(pwd)/.gocache go test ./examples/spawn     # child workflows, Tell/Ask/QueryActor
GOCACHE=$(pwd)/.gocache go test ./examples/control   # durable cadence + snapshots
```
