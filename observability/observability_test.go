package observability

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type testTracer struct {
	starts int32
	errors int32
}

func (t *testTracer) Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span) {
	atomic.AddInt32(&t.starts, 1)
	return ctx, &testSpan{parent: t}
}

type testSpan struct {
	parent *testTracer
}

func (s *testSpan) End(error)                  {}
func (s *testSpan) SetAttributes(...Attribute) {}
func (s *testSpan) RecordError(error) {
	atomic.AddInt32(&s.parent.errors, 1)
}

type testMeter struct {
	hist int32
	cnt  int32
}

func (m *testMeter) RecordHistogram(context.Context, string, time.Duration, ...Attribute) {
	atomic.AddInt32(&m.hist, 1)
}

func (m *testMeter) AddCounter(context.Context, string, int64, ...Attribute) {
	atomic.AddInt32(&m.cnt, 1)
}

func TestObservabilityHooks(t *testing.T) {
	tr := &testTracer{}
	SetTracer(tr)
	m := &testMeter{}
	SetMeter(m)

	ctx, span := ActiveTracer().Start(context.Background(), "test")
	span.RecordError(errors.New("boom"))
	span.End(nil)
	RecordCommandMetrics("kind", "command", time.Millisecond, errors.New("fail"))
	RecordQueryMetrics("kind", "other", "status", 2*time.Millisecond, nil)
	RecordAskMetrics("kind", "other", "Do", 3*time.Millisecond, errors.New("woops"))

	if atomic.LoadInt32(&tr.starts) != 1 {
		t.Fatalf("expected tracer start")
	}
	if atomic.LoadInt32(&tr.errors) != 1 {
		t.Fatalf("expected recorded error")
	}
	if atomic.LoadInt32(&m.hist) != 3 || atomic.LoadInt32(&m.cnt) != 3 {
		t.Fatalf("expected meter usage")
	}

	// reset to no-op
	SetTracer(nil)
	SetMeter(nil)
	ctx, span = ActiveTracer().Start(ctx, "noop")
	span.End(nil)
	RecordCommandMetrics("kind", "cmd", time.Millisecond, nil)
	RecordQueryMetrics("kind", "other", "status", time.Millisecond, nil)
	RecordAskMetrics("kind", "other", "Do", time.Millisecond, nil)
}

type testListener struct {
	ListenerAdapter
	starts int32
}

func (l *testListener) CommandStart(context.Context, CommandEvent) {
	atomic.AddInt32(&l.starts, 1)
}

func TestListenerHooks(t *testing.T) {
	l := &testListener{}
	SetListener(l)
	if ActiveListener() == nil {
		t.Fatalf("expected listener to be active")
	}
	ActiveListener().CommandStart(context.Background(), CommandEvent{ActorKind: "demo"})
	if atomic.LoadInt32(&l.starts) != 1 {
		t.Fatalf("expected command start to be recorded")
	}
	SetListener(nil)
	if ActiveListener() != nil {
		t.Fatalf("expected listener to reset to nil")
	}
}

func TestAttributeHelpers(t *testing.T) {
	attrs := []Attribute{
		String("k", "v"),
		Bool("flag", true),
		Int("count", 3),
		Duration("dur", time.Second),
	}
	if len(attrs) != 4 {
		t.Fatalf("expected four attrs")
	}
	// Ensure ListenerAdapter no-ops are safe to call.
	var adapter ListenerAdapter
	adapter.CommandStart(context.Background(), CommandEvent{})
	adapter.CommandFinish(context.Background(), CommandEvent{}, nil, time.Millisecond)
	adapter.AskStart(context.Background(), AskEvent{})
	adapter.AskFinish(context.Background(), AskEvent{}, nil, time.Millisecond)
	adapter.QueryStart(context.Background(), QueryEvent{})
	adapter.QueryFinish(context.Background(), QueryEvent{}, nil, time.Millisecond)
	adapter.WorkerEvent(context.Background(), WorkerEvent{})
}
