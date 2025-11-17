package actors_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tactors/sdk/actors"
)

type builderState struct {
	Value string
}

func TestBuilderPanicsOnInvalidKindOrFactory(t *testing.T) {
	t.Run("empty kind", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for empty kind")
			}
		}()
		actors.NewStateful("", func() builderState { return builderState{} })
	})
	t.Run("nil state factory", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for nil state factory")
			}
		}()
		actors.NewStateful[builderState]("demo", nil)
	})
}

func TestStateFactoryPanicsPropagate(t *testing.T) {
	actor := actors.NewStateful("panic-factory", func() builderState {
		panic("explode")
	}).Build()
	desc := actor.Spec()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic from state factory")
		}
		if !strings.Contains(fmt.Sprint(r), "explode") {
			t.Fatalf("unexpected panic value: %v", r)
		}
	}()
	_ = desc.StateFactory()
}

func TestBuilderDuplicateHandlerRegistrationPanics(t *testing.T) {
	t.Run("commands", func(t *testing.T) {
		builder := actors.NewStateful("duplicate-cmd", func() builderState { return builderState{} })
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic for duplicate command registration")
			}
		}()
		builder.With(
			actors.Command(func(ctx actors.Ctx, st *builderState, msg struct {
				actors.CommandMsg[struct{}]
			}) (struct{}, error) {
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *builderState, msg struct {
				actors.CommandMsg[struct{}]
			}) (struct{}, error) {
				return struct{}{}, nil
			}),
		)
	})
	t.Run("queries", func(t *testing.T) {
		builder := actors.NewStateful("duplicate-query", func() builderState { return builderState{} })
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic for duplicate query registration")
			}
		}()
		builder.With(
			actors.Query(func(ctx actors.Ctx, st builderState, msg struct {
				actors.QueryMsg[struct{}]
			}) (struct{}, error) {
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st builderState, msg struct {
				actors.QueryMsg[struct{}]
			}) (struct{}, error) {
				return struct{}{}, nil
			}),
		)
	})
}

func TestBuilderSupportsEmptyHandlerSets(t *testing.T) {
	actor := actors.NewStateful("empty", func() builderState { return builderState{} }).Build()
	desc := actor.Spec()
	if desc == nil {
		t.Fatalf("expected description even with no handlers")
	}
	if desc.Commands == nil || len(desc.Commands) != 0 {
		t.Fatalf("expected zero commands, got %#v", desc.Commands)
	}
	if desc.Queries == nil || len(desc.Queries) != 0 {
		t.Fatalf("expected zero queries, got %#v", desc.Queries)
	}
}

func TestOnStartCanSwapStatePointer(t *testing.T) {
	type initPayload struct {
		Value string
	}
	actor := actors.NewStateful("start", func() builderState { return builderState{Value: "factory"} }).
		OnStart(actors.Start(func(ctx actors.Ctx, payload initPayload) (builderState, error) {
			return builderState{Value: payload.Value}, nil
		})).
		Build()
	desc := actor.Spec()
	initial, ok := desc.StateFactory().(*builderState)
	if !ok {
		t.Fatalf("state factory did not return *builderState")
	}
	if initial.Value != "factory" {
		t.Fatalf("unexpected initial state: %#v", initial)
	}
	updated, err := desc.Start.Invoke(nil, initPayload{Value: "from-start"})
	if err != nil {
		t.Fatalf("start invoke failed: %v", err)
	}
	next, ok := updated.(*builderState)
	if !ok {
		t.Fatalf("start handler did not return *builderState: %T", updated)
	}
	if next == initial {
		t.Fatalf("expected start handler to swap state pointer")
	}
	if next.Value != "from-start" {
		t.Fatalf("start handler did not propagate payload: %#v", next)
	}
}

func TestBuilderPersistsConfiguration(t *testing.T) {
	retry := actors.RetryPolicy{MaxAttempts: 5, InitialInterval: time.Second, BackoffCoefficient: 2}
	actor := actors.NewStateful("ticket", func() builderState { return builderState{} }).
		WithTimeout(time.Minute).
		WithRetry(retry).
		WithSignalTimeout("assign", 3*time.Second).
		WithWorkflowQueue(" custom-wf ").
		WithActivityQueue("custom-act ").
		Build()
	desc := actor.Spec()
	if desc.Timeout != time.Minute {
		t.Fatalf("expected timeout minute, got %v", desc.Timeout)
	}
	if desc.Retry != retry {
		t.Fatalf("retry mismatch")
	}
	if got := desc.SignalTimeouts["assign"]; got != 3*time.Second {
		t.Fatalf("signal timeout mismatch: %v", got)
	}
	if desc.WorkflowQueue != "custom-wf" {
		t.Fatalf("workflow queue not trimmed: %q", desc.WorkflowQueue)
	}
	if desc.ActivityQueue != "custom-act" {
		t.Fatalf("activity queue not trimmed: %q", desc.ActivityQueue)
	}
	clone := desc.Clone()
	if clone.WorkflowQueue != "custom-wf" || clone.ActivityQueue != "custom-act" {
		t.Fatalf("queues lost after clone: %q %q", clone.WorkflowQueue, clone.ActivityQueue)
	}
}

func TestBuilderWithMixedActions(t *testing.T) {
	actor := actors.NewStateful("mixed", func() builderState { return builderState{} }).
		With(
			actors.Start(func(ctx actors.Ctx, _ struct{}) (builderState, error) {
				return builderState{Value: "init"}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *builderState, cmd struct {
				actors.CommandMsg[struct{}]
				Value string
			}) (struct{}, error) {
				st.Value = cmd.Value
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st builderState, _ struct {
				actors.QueryMsg[string]
			}) (string, error) {
				return st.Value, nil
			}),
			actors.Activity("noop", func(context.Context, struct{ Value string }) (struct{}, error) {
				return struct{}{}, nil
			}),
		).
		Build()
	desc := actor.Spec()
	if _, err := desc.Start.Invoke(nil, struct{}{}); err != nil {
		t.Fatalf("start invoke failed: %v", err)
	}
	if len(desc.Commands) != 1 || len(desc.Queries) != 1 || len(desc.Activities) != 1 {
		t.Fatalf("expected command/query/activity to be registered")
	}
}

func TestBuilderCommandWithoutNamePanics(t *testing.T) {
	builder := actors.NewStateful("broken", func() builderState { return builderState{} })
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for nameless command")
		}
	}()
	var action actors.CommandAction[builderState]
	builder.With(action)
}

func TestBuilderCommandDecodingErrors(t *testing.T) {
	actor := actors.NewStateful("decode", func() builderState { return builderState{} }).
		With(actors.Command(func(ctx actors.Ctx, st *builderState, cmd struct {
			actors.CommandMsg[struct{}]
			Delta string
		}) (struct{}, error) {
			st.Value = cmd.Delta
			return struct{}{}, nil
		})).
		Build()
	spec := actor.Spec().Commands["struct { actors.CommandMsg[struct {}]; Delta string }"]
	if _, err := spec.Handler.Invoke(nil, builderState{}, "not a struct"); err == nil {
		t.Fatalf("expected decode error")
	}
}

func TestActivityWrappers(t *testing.T) {
	name := "send"
	actor := actors.NewStateful("activity", func() builderState { return builderState{} }).
		With(actors.Activity(name, func(ctx context.Context, req struct {
			Value string
		}) (struct{ Result string }, error) {
			return struct{ Result string }{Result: req.Value}, nil
		})).
		Build()
	desc := actor.Spec()
	act := desc.Activities[name]
	if act == nil {
		t.Fatalf("activity not registered")
	}
	out, err := act(context.Background(), struct{ Value string }{Value: "ok"})
	if err != nil {
		t.Fatalf("activity call failed: %v", err)
	}
	if out.(struct{ Result string }).Result != "ok" {
		t.Fatalf("unexpected activity result: %#v", out)
	}
	decoder := desc.ActivityDecoders()
	if decoder == nil || decoder[name] == nil {
		t.Fatalf("missing activity decoder")
	}
	decoded, err := decoder[name](map[string]any{"Result": "typed"})
	if err != nil {
		t.Fatalf("decoder error: %v", err)
	}
	if decoded.(struct{ Result string }).Result != "typed" {
		t.Fatalf("decoder returned wrong value: %#v", decoded)
	}
}

func TestSnapshotConfig(t *testing.T) {
	actor := actors.NewStateful("snap", func() builderState { return builderState{} }).
		WithSnapshot(actors.SnapshotConfig[builderState]{
			Every: 2,
			ContinueArgs: func(st builderState) (any, error) {
				return st.Value, nil
			},
		}).
		Build()
	desc := actor.Spec()
	if desc.SnapshotEvery != 2 {
		t.Fatalf("expected SnapshotEvery=2, got %d", desc.SnapshotEvery)
	}
	if desc.SnapshotArgs == nil {
		t.Fatalf("expected snapshot args function")
	}
}
