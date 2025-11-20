# Temporal → Tactors Mental Model

Use this as a Rosetta stone if you already know Temporal primitives.

## Core Mapping

- Workflow type → **Actor kind** (`actors.NewStateful("kind", ...)`).
- Signal → **Command** (typed via `actors.CommandMsg[Resp]`; delivered through workflow selector).
- Query → **Query** (typed via `actors.QueryMsg[Resp]`; deterministic cache optional).
- Activity handler → **Activity handler** (same code, registered from the description).
- Child workflow → **actors.Spawn / SpawnOneShot** (long-lived vs one-shot + inline response).
- Continue-As-New → **WithSnapshot** or **ContinueAsNew** helper (preserves signal backlog + state).
- GetVersion → **actors.Patch(ctx, "id")** (declared on the builder).
- Workflow retries → **Per-command retries** plus Temporal retry policy on the worker if desired.
- Workflow ID / Run ID → **actors.Ref** (kind + ID; propagated through Ask/Tell/Spawn).

## Execution Model Differences

- Commands run on the workflow goroutine. If a handler blocks on activity/sleep/ask/query, the signal loop pauses; keep handlers short when responsiveness matters.
- Per-command timeouts: `actors.WithTimeout` bounds handler work; `actors.WithSignalTimeout` drops pending signals after the deadline to keep backlogs sane (Temporal signal timeouts are enforced deterministically).
- Ask/QueryActor are deterministic workflows, not activities—no extra child workflow unless you call `Spawn/SpawnOneShot`.
- Diagnostics queries (`actors_diag_patches`, `actors_diag_snapshot`) are registered automatically on every workflow.

## Data & Determinism

- Data converter defaults to deterministic CBOR (`runtime.DataConverter()`); share it with all clients so CLI/gateways match worker payloads.
- Effects and idempotency: wrap non-deterministic calls in `actors.Effect(ctx, key, ...)` to skip work on replay/Continue-As-New.
- Metadata: `actors.MarshalDescription` emits a stable manifest (hash + handler schemas) for reviews/registry sync.

## Deployment Glue

- Queue defaults: `<kind>-workflow` / `<kind>-activity`; override via `WithWorkflowQueue` / `WithActivityQueue` or per-call options.
- WorkerSet keeps one workflow and activity worker per queue, reusing pollers when queues overlap; `StartAll(ctx)` / `StopAll()` are the lifecycle.
- Observability hooks fire only when `workflow.IsReplaying` is false, so spans/metrics are once-per-real-execution.
