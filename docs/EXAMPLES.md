# Examples Overview

All runnable examples live in the dedicated samples repo: https://github.com/tactors/samples.
They are still wired to the Temporal testsuite and use the same builder APIs described here.

## By Temporal task (in `github.com/tactors/samples/examples/`)

- **Smallest actor:** `greeter` (command + query).
- **Key/value & façade:** `hello-system` (typed queries, lightweight HTTP-ish front).
- **Retries/timeouts:** `orders` (per-command retry/timeout overrides, activities w/ results).
- **Fan-out asks/children:** `spawn` (long-lived children, one-shot `SpawnOneShot`, `Tell`, `QueryActor`).
- **Cross-actor workflows:** `telegram` (multi-command workflow, ask/query between actors, fire-and-forget activities).
- **Caching pattern:** `ticketing` (`actors.WithCache`, stop-command).
- **Durable cadence:** `control` (`control.AwaitInterval`, snapshots, diagnostics queries).

## Run commands (from the samples repo root)

```bash
git clone https://github.com/tactors/samples.git
cd samples
GOCACHE=$(pwd)/.gocache go test ./examples/greeter
GOCACHE=$(pwd)/.gocache go test ./examples/spawn
GOCACHE=$(pwd)/.gocache go test ./examples/control
```

You can copy/paste patterns from those directories—each one exercises the same builder API and
Temporal testsuite harness described in this SDK. For production workers, reuse the actor code with
`runtime.NewWorkerSet` as shown in Getting Started.
