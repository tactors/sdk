# New to Actors?

The actor model keeps state and behavior together, processing messages one at a time instead of
sharing locks or channels across the whole system. This page orients newcomers before diving into
the builder API.

## Core idea

- Each actor instance owns its state. Messages are serialized through a mailbox, so no two handlers
  mutate state concurrently.
- State is durable: in Tactors it lives inside a Temporal workflow history, so crashes or restarts
  replay back to the last processed message.
- Horizontal scale comes from more actors, not more goroutines inside one actor.

## Actor anatomy in this SDK

- **State factory:** `actors.NewStateful("kind", func() MyState { ... })` fixes a state type.
- **Commands and queries:** commands mutate state; queries are read-only and can optionally be
  cached. Embed `actors.CommandMsg` / `actors.QueryMsg` to get typed payloads.
- **Activities:** attach side-effecting work (DB/API/email) to the same description; call them from
  commands via `ctx.Activity` helpers.
- **Child actors:** `actors.Spawn` or `SpawnOneShot` create child workflows when you need isolation
  or parallelism.
- **Messages between actors:** use `actors.Tell` for fire-and-forget, `actors.Ask` for
  request/response, and `actors.QueryActor` for read-only inspection without signals.

## Responsiveness and determinism

- Handlers run on the workflow goroutine. Long sleeps, activities, or cross-actor asks block further
  command delivery until they return. Keep handlers small or push work to activities/children when
  latency matters.
- Use `actors.WithTimeout` to bound handler execution and `actors.WithSignalTimeout` to drop stale
  signals instead of letting mailboxes grow unbounded.
- Wrap non-deterministic work in `actors.Effect` or activities; the workflow replay model means pure
  randomness or real time will diverge otherwise.

## State evolution and upgrades

- `WithSnapshot` rotates actors via Continue-As-New, carrying state plus any queued signals; use this
  for long-lived actors to keep histories short.
- `actors.Patch` gates new logic so running actors can stay deterministic during upgrades.
- `actors.RequestContinueAsNew` lets orchestrators ask other actors to rotate immediately, useful
  when you want fresh init payloads or to clear big backlogs.

## Reliability and contracts

- Messages are at-least-once delivered; make commands idempotent where possible and validate inputs
  with `actors.WithValidator`.
- Use `actors.Ref` (kind + ID) as the stable identity; flow it through Ask/Tell/Spawn to keep links
  explicit.
- Deadlines and retry budgets propagate through `actors.Message(ctx)` metadata so you can enforce
  TTLs across hops.

## Testing and debugging

- Run `go test ./...` to exercise actors against the Temporal testsuite—no special server needed.
- The [Actor builder guide](ACTOR_BUILDER.md) lists every option; [Examples](EXAMPLES.md) show
  runnable scenarios; [Diagnostics](DIAGNOSTICS.md) covers queries for patches/snapshots and worker
  health.
