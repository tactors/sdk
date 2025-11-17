# Getting Started

New to the SDK? This guide introduces the typed builder, shows how to exercise an actor inside the
Temporal testsuite, and finishes with a Temporal worker.

## Before you start

- Go 1.23 or newer
- (Optional) [`temporalio/cli`](https://github.com/temporalio/cli) if you want to inspect real
  histories later—nothing here depends on it.

Clone the repo and open it in your editor of choice.

## 1. Describe a typed actor

Every actor starts with a state factory, typed command/query structs, and handlers registered via the
fluent builder.

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

## 2. Exercise the actor inside the Temporal testsuite

The testkit drives real workflows and activities inside Temporal’s deterministic testsuite (no
external server required). Describe the input sequence through `WhenCommand`, jump time with
`Advance`, and assert results in `Then`.

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

## 3. Register Temporal workers

When you are ready to run the same actor on Temporal, wire it into a `WorkerSet`. Each actor kind
automatically receives `<kind>-workflow` and `<kind>-activity` task queues unless you override them.

```go
c, err := client.Dial(client.Options{})
if err != nil { log.Fatal(err) }
set := runtime.NewWorkerSet(c, runtime.WorkerConfig{})
if _, err := set.Register(hello.HelloActor(), runtime.WorkerConfig{}); err != nil {
    log.Fatal(err)
}
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
if err := set.StartAll(ctx); err != nil {
    log.Fatal(err)
}
<-ctx.Done()
```

`WorkerSet` coordinates both the workflow and activity workers, so handling shutdown simply means
cancelling the context you pass to `StartAll`.

## Where to go next

- [`docs/ACTOR_BUILDER.md`](ACTOR_BUILDER.md) explores every builder knob, including activities,
  snapshots, child workflows, and validation hooks.
- [`docs/RUNTIME_TEMPORAL.md`](RUNTIME_TEMPORAL.md) explains worker registration, queue overrides,
  ask/query plumbing, and Continue-As-New internals.
- [`docs/EXAMPLES.md`](EXAMPLES.md) points to reference implementations under `examples/` that you
  can run with `go test` or `go run`.
- [`docs/OBSERVABILITY.md`](OBSERVABILITY.md) shows how to connect the observability hooks to
  OpenTelemetry (or any other telemetry stack).
