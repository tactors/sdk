# Porting an Existing Temporal Workflow

Checklist for moving a Temporal workflow to typed actors without losing existing behaviors.

## Translation Cheat Sheet

- **Signals → Commands.** Define a command struct with `actors.CommandMsg[Resp]` and register via `actors.Command`.
- **Queries → Queries.** Define `actors.QueryMsg[Resp]` structs; optional per-query cache via `actors.WithCache`.
- **Activities → Activities.** Reuse your existing activity funcs; register with `actors.Activity` inside the builder.
- **Continue-As-New → WithSnapshot.** Configure `WithSnapshot` (cadence + init rebuild) to rotate histories deterministically.
- **GetVersion → Patch.** Declare patches on the builder and gate code with `actors.Patch(ctx, "id")`.
- **Child workflows → Spawn / SpawnOneShot.** Long-lived vs one-shot; provide `WithChildKind` for one-shot.
- **Workflow options → Worker/command options.** Use `actors.WithTimeout`, `actors.WithSignalTimeout`, `actors.WithRetry` per command; set worker options on `runtime.WorkerSet`.

## Porting Steps

1) **Wrap state and kind.** Create `actors.NewStateful("kind", stateFactory).With(...).Build()`. Keep your existing init payload as the start envelope (the state factory runs before `OnStart`).

2) **Translate signals/queries.** For each Temporal signal/query handler, create a typed command/query struct and register handler functions. Keep names stable if you depend on external callers.

3) **Reuse activities.** Move existing activity funcs under `actors.Activity(...)`. From commands, call `ctx.Activity`/`RunActivity` just like `workflow.ExecuteActivity`; honor existing retry policies via `actors.WithRetry` or Temporal worker opts.

4) **Time-bound behavior.** If you used workflow/task timeouts, set per-command timeouts and signal timeouts to drop stuck backlogs. Long handlers block the signal loop—kick off activities/children and return when you need responsiveness.

5) **Apply Continue-As-New.** If histories grew large, wire `WithSnapshot(every, continueArgs)` so rotation captures state + pending signals. Compare with original `workflow.GetInfo().GetContinueAsNewSuggested` usage.

6) **Patches and launches.** Replace `workflow.GetVersion` gates with `actors.Patch(ctx, id)`. Declare patches on the builder; use diagnostics query for rollout audits.

7) **Search attributes & memo.** Replace direct Temporal calls with `actors.UpsertSearchAttributes` / `actors.Memo` helpers inside handlers. The runtime keeps memo/search attributes deterministic across Continue-As-New.

8) **Interceptors/headers.** Propagate correlation/saga IDs via `actors.SetCorrelation(ctx, data)` or via ask/tell options; implement `observability.Listener` for custom telemetry if you previously used workflow/worker interceptors.

9) **Clients and data converter.** Share `runtime.DataConverter()` with any CLI/gateway to keep payloads consistent. If you used payload codecs (compression/encryption), wrap the converter via `runtime.ConfigurePayloadCodecs`.

10) **Test before cutover.** Use `testkit.NewActorTemporalScenario` to mimic your signal/command sequences and fake time. Run `go test ./...` to exercise the same workflow/activities with deterministic Temporal testsuite.

## Gradual Cutover Tips

- Deploy with patches default-off, flip per worker/region via diagnostics query, then default-on in code once traffic stabilizes.
- Run old and new workers on separate queues; switch callers by queue/env var. When stable, decommission the old queue.
- Alert on snapshot lag and worker health (see `DIAGNOSTICS.md`) to catch stuck rotations or failed pollers early.
