# Getting Started (Temporal Users)

You already know Temporal workers, signals, and queries. This guide shows the minimal delta to use
typed actors: define one actor, register it on a worker, and poke it via ask/query.

## Before you start

- Go 1.23 or newer.
- A running Temporal cluster or the testsuite (used by `go test`)—no external server needed for tests.
- Optional: [`temporalio/cli`](https://github.com/temporalio/cli) to inspect histories or trigger
  signals/queries manually.

## 1) Describe an actor

State factory + typed commands/queries. Handlers look like your usual workflow funcs but receive
typed payloads instead of untyped `any`.

```go
package hello

import "github.com/tactors/sdk/actors"

type State struct {
    Count int
}

type HelloCommand struct {
    actors.CommandMsg[HelloResponse]
    Name string
}

type HelloResponse struct {
    Message string
}

type StatusQuery struct {
    actors.QueryMsg[State]
}

func HelloActor() actors.Actor {
    return actors.NewStateful("hello", func() State { return State{} }).
        With(
            actors.Command(func(ctx actors.Ctx, st *State, cmd HelloCommand) (HelloResponse, error) {
                st.Count++
                return HelloResponse{Message: "hello " + cmd.Name}, nil
            }),
            actors.Query(func(ctx actors.Ctx, st State, _ StatusQuery) (State, error) {
                return st, nil
            }),
            actors.StopCommandAction[State](),
        ).
        Build()
}
```

Handlers receive real Go structs (not `interface{}` payloads), so requesting new behavior stays
ergonomic and deterministic.

### Handler responsiveness

Commands run on the workflow goroutine. If a handler blocks on activity/sleep/ask/query, the signal
loop pauses; keep handlers short when you care about responsiveness (trigger work, return, expose
progress via query or callback).

## 2) Register a worker (minimal delta from Temporal)

Same `client.Dial`, plus `runtime.NewWorkerSet` and `Register` with your actor. Queues default to
`<kind>-workflow` / `<kind>-activity`. Workers auto-disable the unused role: if you register only
workflows, the activity poller stays off; if you register only activities, the workflow poller stays
off. Startup logs note which queues are enabled or auto-disabled. If you explicitly point both
workflow and activity roles at the same queue and register both kinds of handlers, a single worker
services that queue.

```go
c, err := client.Dial(client.Options{
    // reuse your usual options; share DataConverter if other clients need it
    DataConverter: runtime.DataConverter(),
})
if err != nil { log.Fatal(err) }
set := runtime.NewWorkerSet(c, runtime.WorkerConfig{})
if _, err := set.Register(HelloActor(), runtime.WorkerConfig{}); err != nil {
    log.Fatal(err)
}
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
if err := set.StartAll(ctx); err != nil {
    log.Fatal(err)
}
<-ctx.Done()
```

Callers can use `actors.InvokeAsk` / `actors.InvokeTell` with the same data converter, or just signal
the workflow type `hello` using Temporal tooling.

## 3) Try it via testsuite or CLI

**Testsuite (no cluster needed):** drive real workflows/activities deterministically.

```go
scenario := testkit.NewActorTemporalScenario(hello.HelloActor(), "hello-wf", struct{}{})
scenario.
    WhenCommand(hello.HelloCommand{Name: "Neo"}).
    WhenCommand(hello.HelloCommand{Name: "Trinity"}).
    WhenCommand(actors.StopCommand{}).
    Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
        resp, err := scenario.QueryWorkflow(hello.StatusQuery{})
        if err != nil { tb.Fatalf("query: %v", err) }
        var state hello.State
        if err := resp.Get(&state); err != nil { tb.Fatalf("decode: %v", err) }
        if state.Count != 2 { tb.Fatalf("expected 2 greetings") }
    }).
    Run(t)
```

Each `WhenCommand` enqueues a deterministic signal, while the final `Then` block provides a typed
Temporal client for queries and results.

**CLI (if you have a cluster):** start the worker above, then use `temporal workflow signal` /
`temporal workflow query` against workflow type `hello` (or invoke `actors.InvokeAsk` in code).

## Where to go next

- [`docs/TEMPORAL_MENTAL_MODEL.md`](TEMPORAL_MENTAL_MODEL.md) to map familiar Temporal concepts to
  Tactors primitives.
- [`docs/PORTING_TEMPORAL.md`](PORTING_TEMPORAL.md) for a migration checklist.
- [`docs/ACTOR_BUILDER.md`](ACTOR_BUILDER.md) for the full builder surface (retries, timeouts,
  cache, activities, child workflows).
- [`docs/RUNTIME_TEMPORAL.md`](RUNTIME_TEMPORAL.md) for worker options, queue overrides, ask/query
  plumbing, and Continue-As-New internals.
- [`docs/EXAMPLES.md`](EXAMPLES.md) for runnable samples using the Temporal testsuite.
- [`docs/OBSERVABILITY.md`](OBSERVABILITY.md) to bridge spans/metrics into your telemetry stack.
