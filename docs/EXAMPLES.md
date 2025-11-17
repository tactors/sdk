# Examples Overview

Every sample under `examples/` is runnable via `go test`, which drives the Temporal testsuite
scenario described alongside the actor. Use them as reference implementations while exploring the
SDK.

| Path | What it teaches |
|------|-----------------|
| `examples/greeter` | Smallest possible actor: one command, one query, and a Temporal scenario. |
| `examples/hello-system` | Simple key/value state, typed queries, and a lightweight HTTP-ish façade. |
| `examples/orders` | Activities that return typed results, retries, and per-command timeout overrides. |
| `examples/spawn` | Long-lived `actors.Spawn`, one-shot children (`actors.SpawnOneShot`), `Tell`, and `QueryActor`. |
| `examples/telegram` | Multi-command workflow w/ ask/query across actors plus fire-and-forget activities. |
| `examples/ticketing` | Composite state, caching with `actors.WithCache`, and a stop-command pattern. |
| `examples/control` | Control workflow that uses `control.AwaitInterval`, snapshots, and diagnostics queries. |

Quick command:

```bash
GOCACHE=$(pwd)/.gocache go test ./examples/greeter
```

Feel free to copy/paste patterns from these directories—each one uses the same builder API and
Temporal testsuite harness described elsewhere in the docs.
