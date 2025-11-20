# Tactors SDK

Tactors is a typed, builder-first actor SDK for Go. You describe state, handlers, activities, and
runtime knobs in one fluent API and receive a deterministic actor description that every runtime can
consume—no reflection, code generation, or `interface{}` plumbing.

## What you get

- **Typed actors front to back.** `actors.NewStateful("kind", func() MyState { ... })` fixes the state
  type; commands, queries, and activities embed helper structs (`actors.CommandMsg`, `actors.QueryMsg`,
  `actors.ActivityMsg`) so handlers receive real Go types with no manual decoding.
- **Declarative behavior.** Add retries, timeouts, caches, queue overrides, and snapshots with fluent
  helpers such as `actors.WithTimeout`, `actors.WithRetry`, `actors.WithCache`, `WithWorkflowQueue`,
  and `WithSnapshot`.
- **Temporal-first runtime.** Builders emit an `actors.Description` record that the Temporal runtime
  and testsuite harness consume directly. No additional runtimes are planned—the SDK is focused on
  Temporal end to end.
- **Built-in observability.** Command handlers plus cross-actor `Ask`/`Query` calls emit spans and
  metrics via the `observability` hooks so any framework can connect OpenTelemetry or other backends
  without custom adapters.
- **Cross-actor tooling.** Spawn long-lived or one-shot children, send typed commands across actors,
  force rotations via `actors.RequestContinueAsNew`, and gate upgrades via `actors.Patch` by default.

## Newcomer path

1. **Read the walkthrough.** [`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md) shows the minimum
   actor, how to exercise it inside the Temporal testsuite, and how to wire a Temporal worker.
2. **Explore the builder.** [`docs/ACTOR_BUILDER.md`](docs/ACTOR_BUILDER.md) explains every `With(...)`
   option plus helpers for activities, child workflows, and cross-actor calls.
3. **Launch workers.** [`docs/RUNTIME_TEMPORAL.md`](docs/RUNTIME_TEMPORAL.md) covers queue naming,
   worker registration, ask/query plumbing, and Continue-As-New behavior.
4. **Study real code.** [`docs/EXAMPLES.md`](docs/EXAMPLES.md) lists the runnable samples under
   `examples/`, each wired directly into the Temporal testsuite.
5. **Want to help?** [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) captures the development workflow
   and roadmap ideas.
6. **Diagnostics & control.** [`docs/DIAGNOSTICS.md`](docs/DIAGNOSTICS.md) explains metadata export,
   registry access, patch/snapshot queries, and worker health. [`docs/CONTROL_WORKFLOWS.md`](docs/CONTROL_WORKFLOWS.md)
   covers deterministic interval helpers for maintenance actors.
- **Already on Temporal?** Jump to [`docs/INDEX.md`](docs/INDEX.md) for a Temporal-first map,
  mental model, and porting guide.

## Temporal runtime at a glance

- `runtime` registers workflows and activities directly from an actor description, mapping
  commands to signals and queries to Temporal query handlers.
- `runtime.NewWorkerSet` keeps one workflow and one activity worker per queue, reuses workers when
  multiple actors share queues, and exposes `StartAll(ctx)` / `StopAll()` to manage them together.
- History rotation happens automatically. When you configure `WithSnapshot`, the runtime Continue-As-New
  payload carries state plus pending signals; without it, the runtime still obeys Temporal’s
  `GetContinueAsNewSuggested()` hook and restarts with the original init payload.
- The deterministic Temporal testsuite harness powers the `testkit` package, so code you write for
  production is exactly what you exercise in tests—no in-memory shortcut paths.

## Repository guide

- `actors/` – builder, typed helpers, and shared primitives.
- `runtime/` – Temporal runner plus typed client utilities.
- `testkit/` – deterministic harnesses (Temporal scenario and testing helpers).
- `examples/` – runnable samples referenced throughout the docs.
- `observability/` – pluggable tracing/metrics hooks; `internal/` – supporting packages as we flesh
  out tooling.
- More end-to-end samples live in the sibling repo: https://github.com/tactors/samples.

## Testing

Run the full suite (unit tests and Temporal testsuite scenarios) with:

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

No external Temporal server is required—the testsuite handles registration, fake time, and cleanup.

## License

Released under the [MIT License](LICENSE).
