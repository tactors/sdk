package observability

import (
	"context"
	"sync"
	"time"
)

// Attribute describes a key/value pair passed to tracers or meters.
type Attribute struct {
	Key   string
	Value any
}

// String returns an Attribute with a string value.
func String(key, value string) Attribute {
	return Attribute{Key: key, Value: value}
}

// Bool returns an Attribute with a boolean value.
func Bool(key string, value bool) Attribute {
	return Attribute{Key: key, Value: value}
}

// Int returns an Attribute with an integer value.
func Int(key string, value int) Attribute {
	return Attribute{Key: key, Value: value}
}

// Duration returns an Attribute with a duration value.
func Duration(key string, value time.Duration) Attribute {
	return Attribute{Key: key, Value: value}
}

// Tracer creates spans for commands and other actor events.
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span)
}

// Span wraps an in-flight trace span.
type Span interface {
	End(err error)
	SetAttributes(attrs ...Attribute)
	RecordError(err error)
}

// Meter emits metrics such as counters or histograms.
type Meter interface {
	RecordHistogram(ctx context.Context, name string, value time.Duration, attrs ...Attribute)
	AddCounter(ctx context.Context, name string, value int64, attrs ...Attribute)
}

// CommandEvent describes command execution attempts.
type CommandEvent struct {
	ActorKind     string
	Command       string
	WorkflowID    string
	RunID         string
	TaskQueue     string
	MessageID     string
	CorrelationID string
	CallerKind    string
	CallerID      string
	Attributes    []Attribute
}

// AskEvent describes cross-actor ask executions.
type AskEvent struct {
	CallerKind      string
	CallerID        string
	CallerWorkflow  string
	CallerRunID     string
	CallerTaskQueue string
	TargetKind      string
	TargetID        string
	Command         string
	CorrelationID   string
	Attributes      []Attribute
}

// QueryEvent describes cross-actor queries.
type QueryEvent struct {
	CallerKind      string
	CallerID        string
	CallerWorkflow  string
	CallerRunID     string
	CallerTaskQueue string
	TargetKind      string
	TargetID        string
	Query           string
	CorrelationID   string
	Attributes      []Attribute
}

// WorkerRole identifies the worker category emitting events.
type WorkerRole string

const (
	WorkerRoleWorkflow WorkerRole = "workflow"
	WorkerRoleActivity WorkerRole = "activity"
)

// WorkerEventType categorizes worker lifecycle events.
type WorkerEventType string

const (
	WorkerEventStart WorkerEventType = "start"
	WorkerEventStop  WorkerEventType = "stop"
	WorkerEventError WorkerEventType = "error"
)

// WorkerEvent describes lifecycle changes for workflow or activity workers.
type WorkerEvent struct {
	Queue      string
	Role       WorkerRole
	Type       WorkerEventType
	Error      error
	ActorKinds []string
}

// Listener receives deterministic lifecycle hooks emitted by the runtime.
type Listener interface {
	CommandStart(ctx context.Context, evt CommandEvent)
	CommandFinish(ctx context.Context, evt CommandEvent, err error, duration time.Duration)
	AskStart(ctx context.Context, evt AskEvent)
	AskFinish(ctx context.Context, evt AskEvent, err error, duration time.Duration)
	QueryStart(ctx context.Context, evt QueryEvent)
	QueryFinish(ctx context.Context, evt QueryEvent, err error, duration time.Duration)
}

// WorkerLifecycleListener observes worker start/stop/failure events. Listener implementations can
// optionally satisfy this interface to receive worker events.
type WorkerLifecycleListener interface {
	WorkerEvent(ctx context.Context, evt WorkerEvent)
}

// ListenerAdapter provides no-op implementations so callers can embed it.
type ListenerAdapter struct{}

func (ListenerAdapter) CommandStart(context.Context, CommandEvent)                        {}
func (ListenerAdapter) CommandFinish(context.Context, CommandEvent, error, time.Duration) {}
func (ListenerAdapter) AskStart(context.Context, AskEvent)                                {}
func (ListenerAdapter) AskFinish(context.Context, AskEvent, error, time.Duration)         {}
func (ListenerAdapter) QueryStart(context.Context, QueryEvent)                            {}
func (ListenerAdapter) QueryFinish(context.Context, QueryEvent, error, time.Duration)     {}
func (ListenerAdapter) WorkerEvent(context.Context, WorkerEvent)                          {}

var (
	tracerMu     sync.RWMutex
	activeTracer Tracer = noopTracer{}

	meterMu     sync.RWMutex
	activeMeter Meter = noopMeter{}

	listenerMu     sync.RWMutex
	activeListener Listener
)

// SetTracer configures the tracer used by the runtime.
func SetTracer(t Tracer) {
	tracerMu.Lock()
	defer tracerMu.Unlock()
	if t == nil {
		t = noopTracer{}
	}
	activeTracer = t
}

// ActiveTracer returns the currently configured tracer.
func ActiveTracer() Tracer {
	tracerMu.RLock()
	defer tracerMu.RUnlock()
	return activeTracer
}

// SetMeter configures the meter used by the runtime.
func SetMeter(m Meter) {
	meterMu.Lock()
	defer meterMu.Unlock()
	if m == nil {
		m = noopMeter{}
	}
	activeMeter = m
}

// SetListener configures the lifecycle listener used by the runtime.
func SetListener(l Listener) {
	listenerMu.Lock()
	defer listenerMu.Unlock()
	activeListener = l
}

// ActiveListener returns the currently configured lifecycle listener.
func ActiveListener() Listener {
	listenerMu.RLock()
	defer listenerMu.RUnlock()
	return activeListener
}

// RecordCommandMetrics emits histogram/counter data for a command execution.
func RecordCommandMetrics(kind, command string, duration time.Duration, err error) {
	meterMu.RLock()
	m := activeMeter
	meterMu.RUnlock()
	if m == nil {
		return
	}
	attrs := []Attribute{
		String("actor.kind", kind),
		String("actor.command", command),
	}
	m.RecordHistogram(context.Background(), "actor_command_duration", duration, attrs...)
	if err != nil {
		m.AddCounter(context.Background(), "actor_command_errors_total", 1, attrs...)
	} else {
		m.AddCounter(context.Background(), "actor_command_success_total", 1, attrs...)
	}
}

// RecordQueryMetrics emits metrics for cross-actor queries.
func RecordQueryMetrics(callerKind, calleeKind, query string, duration time.Duration, err error) {
	meterMu.RLock()
	m := activeMeter
	meterMu.RUnlock()
	if m == nil {
		return
	}
	attrs := []Attribute{
		String("actor.kind", callerKind),
		String("actor.target_kind", calleeKind),
		String("actor.query", query),
	}
	m.RecordHistogram(context.Background(), "actor_query_duration", duration, attrs...)
	counter := "actor_query_success_total"
	if err != nil {
		counter = "actor_query_errors_total"
	}
	m.AddCounter(context.Background(), counter, 1, attrs...)
}

// RecordAskMetrics emits metrics for ask-style cross-actor commands.
func RecordAskMetrics(callerKind, calleeKind, command string, duration time.Duration, err error) {
	meterMu.RLock()
	m := activeMeter
	meterMu.RUnlock()
	if m == nil {
		return
	}
	attrs := []Attribute{
		String("actor.kind", callerKind),
		String("actor.target_kind", calleeKind),
		String("actor.command", command),
	}
	m.RecordHistogram(context.Background(), "actor_ask_duration", duration, attrs...)
	counter := "actor_ask_success_total"
	if err != nil {
		counter = "actor_ask_errors_total"
	}
	m.AddCounter(context.Background(), counter, 1, attrs...)
}

// EmitWorkerEvent notifies the active listener of worker lifecycle events.
func EmitWorkerEvent(evt WorkerEvent) {
	listenerMu.RLock()
	l := activeListener
	listenerMu.RUnlock()
	if wl, ok := l.(WorkerLifecycleListener); ok && wl != nil {
		wl.WorkerEvent(context.Background(), evt)
	}
}

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ ...Attribute) (context.Context, Span) {
	return ctx, noopSpan{}
}

type noopSpan struct{}

func (noopSpan) End(error)                  {}
func (noopSpan) SetAttributes(...Attribute) {}
func (noopSpan) RecordError(error)          {}

type noopMeter struct{}

func (noopMeter) RecordHistogram(context.Context, string, time.Duration, ...Attribute) {}
func (noopMeter) AddCounter(context.Context, string, int64, ...Attribute)              {}
