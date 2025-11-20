# Examples Overview

Every sample under `examples/` is runnable via `go test`, which drives the Temporal testsuite
scenario described alongside the actor. Use them as Temporal task templates.

## By Temporal task

- **Smallest actor:** `examples/greeter` (command + query).
- **Key/value & façade:** `examples/hello-system` (typed queries, lightweight HTTP-ish front).
- **Retries/timeouts:** `examples/orders` (per-command retry/timeout overrides, activities w/ results).
- **Fan-out asks/children:** `examples/spawn` (long-lived children, one-shot `SpawnOneShot`, `Tell`, `QueryActor`).
- **Cross-actor workflows:** `examples/telegram` (multi-command workflow, ask/query between actors, fire-and-forget activities).
- **Caching pattern:** `examples/ticketing` (`actors.WithCache`, stop-command).
- **Durable cadence:** `examples/control` (`control.AwaitInterval`, snapshots, diagnostics queries).

## Run commands

```bash
GOCACHE=$(pwd)/.gocache go test ./examples/greeter
GOCACHE=$(pwd)/.gocache go test ./examples/spawn
GOCACHE=$(pwd)/.gocache go test ./examples/control
```

Feel free to copy/paste patterns from these directories—each one uses the same builder API and
Temporal testsuite harness described elsewhere in the docs. For real clusters, reuse the same actor
code with `runtime.NewWorkerSet` as shown in Getting Started. Additional end-to-end apps (HTTP façades,
gateways, CLIs) live in the samples repo: https://github.com/tactors/samples.
