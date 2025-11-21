package actors

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCommandHandlerInvokeMissingFn(t *testing.T) {
	h := CommandHandler{Name: "missing"}
	if _, err := h.Invoke(nil, nil, nil); err == nil || !strings.Contains(err.Error(), "has no handler") {
		t.Fatalf("expected error for missing handler, got %v", err)
	}
}

func TestQueryHandlerInvokeMissingFn(t *testing.T) {
	h := QueryHandler{Name: "lookup"}
	if _, err := h.Invoke(nil, nil, nil); err == nil || !strings.Contains(err.Error(), "has no handler") {
		t.Fatalf("expected error for missing handler, got %v", err)
	}
}

func TestStatelessBuilderRegistersActivitiesAndQueues(t *testing.T) {
	builder := New("stateless").
		WithTimeout(time.Minute).
		WithRetry(RetryPolicy{MaxAttempts: 2}).
		WithSignalTimeout("do", 5*time.Second).
		WithWorkflowQueue(" wf ").
		WithActivityQueue(" act ").
		WithActivity(" job ", func(context.Context, any) (any, error) { return "ok", nil })
	actor := builder.Build()
	desc := actor.Spec()
	if desc.Timeout != time.Minute {
		t.Fatalf("timeout not captured: %v", desc.Timeout)
	}
	if desc.Retry.MaxAttempts != 2 {
		t.Fatalf("retry not captured")
	}
	if desc.WorkflowQueue != "wf" || desc.ActivityQueue != "act" {
		t.Fatalf("queues not trimmed: %q %q", desc.WorkflowQueue, desc.ActivityQueue)
	}
	if desc.SignalTimeouts["do"] != 5*time.Second {
		t.Fatalf("signal timeout missing")
	}
	if desc.Activities["job"] == nil {
		t.Fatalf("activity not registered")
	}
}

func TestCommandAndQueryFuncAdapters(t *testing.T) {
	type state struct{ Count int }
	type incrCommand struct {
		CommandMsg[struct{}]
		Value int
	}
	type sumQuery struct {
		QueryMsg[int]
		Extra int
	}
	builder := NewStateful("func-adapters", func() state { return state{} }).
		With(
			CommandFunc(func(ctx Ctx, st *state, cmd incrCommand) (struct{}, error) {
				st.Count += cmd.Value
				return struct{}{}, nil
			}),
			QueryFunc(func(ctx Ctx, st state, q sumQuery) (int, error) {
				return st.Count + q.Extra, nil
			}),
		)
	desc := builder.Build().Spec()
	if len(desc.Commands) != 1 || len(desc.Queries) != 1 {
		t.Fatalf("expected command and query registration")
	}
}

func TestActivityNoResultAdapter(t *testing.T) {
	builder := NewStateful("nores", func() struct{} { return struct{}{} }).
		With(ActivityNoResultNamed("cleanup", func(context.Context, string) error { return nil }))
	desc := builder.Build().Spec()
	act := desc.Activities["cleanup"]
	if act == nil {
		t.Fatalf("activity not registered")
	}
	if _, err := act(context.Background(), "payload"); err != nil {
		t.Fatalf("activity returned error: %v", err)
	}
}

func TestActivityCallOptionsBuilders(t *testing.T) {
	opts := buildActivityCallOptions(
		WithActivityScheduleToClose(time.Second),
		WithActivityScheduleToStart(2*time.Second),
		WithActivityStartToClose(3*time.Second),
		WithActivityHeartbeat(time.Minute),
		WithActivityRetry(RetryPolicy{MaxAttempts: 3}),
		WithActivityTaskQueue("custom"),
	)
	if opts.ScheduleToClose != time.Second ||
		opts.ScheduleToStart != 2*time.Second ||
		opts.StartToClose != 3*time.Second ||
		opts.Heartbeat != time.Minute {
		t.Fatalf("timeouts not captured: %+v", opts)
	}
	if opts.Retry.MaxAttempts != 3 {
		t.Fatalf("retry missing: %+v", opts.Retry)
	}
	if opts.TaskQueue != "custom" {
		t.Fatalf("task queue mismatch: %s", opts.TaskQueue)
	}
}

func TestTypeKeyOf(t *testing.T) {
	if TypeKeyOf(nil) != "" {
		t.Fatalf("nil payload should return empty key")
	}
	val := struct{ Foo int }{}
	if key := TypeKeyOf(val); key == "" || !strings.Contains(key, "Foo") {
		t.Fatalf("unexpected type key: %s", key)
	}
}

type captureInvoker struct {
	commandMethod string
	askMethod     string
	queryMethod   string
	askOpts       AskOptions
}

func (c *captureInvoker) InvokeCommand(ctx context.Context, ref Ref, method string, payload any) error {
	c.commandMethod = method
	return nil
}

func (c *captureInvoker) InvokeAsk(ctx context.Context, ref Ref, method string, payload any, resp any, opts AskOptions) error {
	c.askMethod = method
	c.askOpts = opts
	if out, ok := resp.(*string); ok {
		*out = "response"
	}
	return nil
}

func (c *captureInvoker) InvokeQuery(ctx context.Context, ref Ref, method string, payload any, resp any) error {
	c.queryMethod = method
	if out, ok := resp.(*int); ok {
		*out = 42
	}
	return nil
}

type pingCommand struct {
	CommandMsg[struct{}]
}

type sumQuery struct {
	QueryMsg[int]
}

func TestInvokeHelpersRouteToRegisteredInvoker(t *testing.T) {
	RegisterClientInvoker(nil)
	stub := &captureInvoker{}
	RegisterClientInvoker(func(Ref) ClientInvoker { return stub })
	t.Cleanup(func() { RegisterClientInvoker(nil) })

	ref := ARef("demo", "id")
	ctx := context.Background()
	if err := InvokeCommand(ctx, ref, pingCommand{}); err != nil {
		t.Fatalf("invoke command failed: %v", err)
	}
	if stub.commandMethod != TypeKeyOf(pingCommand{}) {
		t.Fatalf("command invoker saw wrong method: %s", stub.commandMethod)
	}
	resp, err := InvokeAsk[string](ctx, ref, pingCommand{})
	if err != nil {
		t.Fatalf("invoke ask failed: %v", err)
	}
	if resp != "response" {
		t.Fatalf("ask response missing")
	}
	if stub.askMethod != TypeKeyOf(pingCommand{}) || stub.askOpts.CorrelationID == "" {
		t.Fatalf("ask metadata missing: method=%s opts=%+v", stub.askMethod, stub.askOpts)
	}
	customID := "custom-id"
	if _, err := InvokeAsk[string](ctx, ref, pingCommand{}, WithCorrelationID(customID)); err != nil {
		t.Fatalf("invoke ask with option failed: %v", err)
	}
	if stub.askOpts.CorrelationID != customID {
		t.Fatalf("ask option not applied: %s", stub.askOpts.CorrelationID)
	}
	result, err := InvokeQuery[int](ctx, ref, sumQuery{})
	if err != nil {
		t.Fatalf("invoke query failed: %v", err)
	}
	if result != 42 {
		t.Fatalf("unexpected query result: %d", result)
	}
	if stub.queryMethod != TypeKeyOf(sumQuery{}) {
		t.Fatalf("query method mismatch: %s", stub.queryMethod)
	}
}

func TestInvokeHelpersErrorWhenTypeUnknown(t *testing.T) {
	RegisterClientInvoker(nil)
	ref := ARef("demo", "id")
	if err := InvokeCommand(context.Background(), ref, nil); err == nil {
		t.Fatalf("expected type inference error")
	}
	_, err := InvokeQuery[int](context.Background(), ref, nil)
	if err == nil {
		t.Fatalf("expected query type error")
	}
}

func TestRegisterClientInvokerNilResetsToNoop(t *testing.T) {
	RegisterClientInvoker(nil)
	ref := ARef("demo", "id")
	if err := InvokeCommand(context.Background(), ref, pingCommand{}); err != ErrUnsupported {
		t.Fatalf("expected noop invoker error, got %v", err)
	}
}

func TestRefBuildersAndOptions(t *testing.T) {
	ref := ARef("worker", "id",
		WithWorkflowType("custom"),
		WithTaskQueue("queue"),
		WithStartArgs("a", "b"),
		WithRunID("run"),
	)
	if ref.WorkflowType != "custom" || ref.TaskQueue != "queue" || ref.RunID != "run" {
		t.Fatalf("options not applied: %+v", ref)
	}
	payload := ref.StartPayload()
	if len(payload) != 2 || payload[0] != "a" {
		t.Fatalf("start payload mismatch: %#v", payload)
	}
	payload[0] = "mutated"
	if ref.StartArgs[0] != "a" {
		t.Fatalf("start payload should be copied")
	}
	if TRef("tenant", "worker", "id").Tenant != "tenant" {
		t.Fatalf("tenant not applied")
	}
	if ARef("demo", "id").ID != "id" {
		t.Fatalf("id missing")
	}
	empty := Ref{}
	if !empty.Empty() {
		t.Fatalf("expected empty ref")
	}
}

func TestSpawnOptionBuilders(t *testing.T) {
	cfg := SpawnConfig{}
	for _, opt := range []SpawnOption{
		WithChildName("child"),
		WithChildTimeout(time.Second),
		WithChildKind("kind"),
		WithChildTaskQueue(" queue "),
	} {
		opt(&cfg)
	}
	if cfg.Name != "child" || cfg.Kind != "kind" {
		t.Fatalf("options not applied: %+v", cfg)
	}
	if cfg.Timeout != time.Second {
		t.Fatalf("timeout missing")
	}
	if cfg.TaskQueue != "queue" {
		t.Fatalf("task queue not trimmed: %q", cfg.TaskQueue)
	}
}
