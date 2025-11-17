package actors

import (
	"fmt"
	"strings"
	"time"

	"github.com/tactors/sdk/internal/codec"
)

// Ctx is the execution context passed to every handler. It intentionally mirrors the subset of
// Temporal features most actors rely on so another runtime could be wired up later.
type Ctx interface {
	ActorID() string
	Now() time.Time
	Sleep(delay time.Duration) error
	Version(changeID string, defaultVersion, newVersion int) int
	Activity(name string, payload any) ActivityFuture
	ActivityWithOptions(name string, payload any, opts ActivityCallOptions) ActivityFuture
	BackgroundActivity(name string, payload any)
	Logger() Logger
	Self() Ref
	UpsertSearchAttributes(attrs map[string]any) error
	SearchAttributes() map[string]any
	MessageMetadata() MessageMetadata
	Effect(key string, fn EffectFunc, opts ...EffectOption) (any, error)
	Correlation() CorrelationData
	SetCorrelation(CorrelationData)
	SnapshotInfo() SnapshotInfo
}

// ActivityFuture is a placeholder for the runtime specific promise returned by Activity.
type ActivityFuture interface {
	Get() (any, error)
}

// ActivityCallOptions configures per-invocation activity behavior.
type ActivityCallOptions struct {
	ScheduleToClose time.Duration
	ScheduleToStart time.Duration
	StartToClose    time.Duration
	Heartbeat       time.Duration
	Retry           RetryPolicy
	TaskQueue       string
}

// ActivityCallOption customizes ActivityCallOptions.
type ActivityCallOption func(*ActivityCallOptions)

// WithActivityScheduleToClose sets the schedule-to-close timeout.
func WithActivityScheduleToClose(d time.Duration) ActivityCallOption {
	return func(opts *ActivityCallOptions) {
		opts.ScheduleToClose = d
	}
}

// WithActivityScheduleToStart sets the schedule-to-start timeout.
func WithActivityScheduleToStart(d time.Duration) ActivityCallOption {
	return func(opts *ActivityCallOptions) {
		opts.ScheduleToStart = d
	}
}

// WithActivityStartToClose sets the start-to-close timeout.
func WithActivityStartToClose(d time.Duration) ActivityCallOption {
	return func(opts *ActivityCallOptions) {
		opts.StartToClose = d
	}
}

// WithActivityHeartbeat sets the heartbeat timeout.
func WithActivityHeartbeat(d time.Duration) ActivityCallOption {
	return func(opts *ActivityCallOptions) {
		opts.Heartbeat = d
	}
}

// WithActivityRetry sets the activity retry policy.
func WithActivityRetry(policy RetryPolicy) ActivityCallOption {
	return func(opts *ActivityCallOptions) {
		opts.Retry = policy
	}
}

// WithActivityTaskQueue routes the activity to a specific task queue.
func WithActivityTaskQueue(name string) ActivityCallOption {
	return func(opts *ActivityCallOptions) {
		opts.TaskQueue = strings.TrimSpace(name)
	}
}

func buildActivityCallOptions(opts ...ActivityCallOption) ActivityCallOptions {
	cfg := ActivityCallOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// DefaultVersion mirrors Temporal's workflow.DefaultVersion for convenience.
const DefaultVersion = -1

// Patch reports whether the change identified by changeID is active.
func Patch(ctx Ctx, changeID string) bool {
	if ctx == nil {
		return true
	}
	return ctx.Version(changeID, DefaultVersion, 0) != DefaultVersion
}

// Logger is the minimal logging surface actors need. Runtimes can adapt their logger of choice.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Parent returns the parent reference if exposed by the runtime.
func Parent(ctx Ctx) Ref {
	if pc, ok := ctx.(interface{ Parent() Ref }); ok {
		return pc.Parent()
	}
	return Ref{}
}

// Spawn launches a long-lived child actor.
func Spawn(ctx Ctx, kind string, init any, opts ...SpawnOption) (Ref, error) {
	if spawner, ok := ctx.(interface {
		SpawnChild(kind string, init any, opts ...SpawnOption) (Ref, error)
	}); ok {
		return spawner.SpawnChild(kind, init, opts...)
	}
	return Ref{}, ErrUnsupported
}

// SpawnOneShot launches a one-shot child workflow returning a typed response.
func SpawnOneShot[Req TypedCommandMessage[Resp], Resp any](ctx Ctx, req Req, opts ...SpawnOption) (Resp, error) {
	var zero Resp
	if ctx == nil {
		return zero, ErrUnsupported
	}
	if spawner, ok := ctx.(interface {
		SpawnOneShot(payload any, opts ...SpawnOption) (any, error)
	}); ok {
		val, err := spawner.SpawnOneShot(req, opts...)
		if err != nil {
			return zero, err
		}
		if val == nil {
			return zero, nil
		}
		return decodeTypedResult[Resp](val)
	}
	return zero, ErrUnsupported
}

// Tell sends a typed command to another actor instance without waiting for a response.
func Tell[Req TypedCommandMessage[Resp], Resp any](ctx Ctx, ref Ref, req Req) error {
	if ctx == nil {
		return ErrUnsupported
	}
	if sender, ok := ctx.(interface {
		SendCommand(ref Ref, payload any) error
	}); ok {
		return sender.SendCommand(ref, req)
	}
	return ErrUnsupported
}

// QueryActor executes a typed query against another actor instance.
func QueryActor[Req TypedQueryMessage[Resp], Resp any](ctx Ctx, ref Ref, req Req) (Resp, error) {
	var zero Resp
	if ctx == nil {
		return zero, ErrUnsupported
	}
	if invoker, ok := ctx.(interface {
		QueryActor(ref Ref, payload any) (any, error)
	}); ok {
		val, err := invoker.QueryActor(ref, req)
		if err != nil {
			return zero, err
		}
		return decodeTypedResult[Resp](val)
	}
	return zero, ErrUnsupported
}

// Ask sends a typed command to another actor instance and waits for the response.
func Ask[Req TypedCommandMessage[Resp], Resp any](ctx Ctx, ref Ref, req Req) (Resp, error) {
	var zero Resp
	if ctx == nil {
		return zero, ErrUnsupported
	}
	if invoker, ok := ctx.(interface {
		AskActor(ref Ref, payload any) (any, error)
	}); ok {
		val, err := invoker.AskActor(ref, req)
		if err != nil {
			return zero, err
		}
		return decodeTypedResult[Resp](val)
	}
	return zero, ErrUnsupported
}

// ContinueAsNewOptions configure how a remote continue-as-new request behaves.
type ContinueAsNewOptions struct {
	Init any
}

// ContinueAsNewOption customizes ContinueAsNewOptions.
type ContinueAsNewOption func(*ContinueAsNewOptions)

// WithContinueInit overrides the init payload supplied to the next run.
func WithContinueInit(payload any) ContinueAsNewOption {
	return func(cfg *ContinueAsNewOptions) {
		cfg.Init = payload
	}
}

// RequestContinueAsNew asks another actor instance to snapshot and continue-as-new.
func RequestContinueAsNew(ctx Ctx, ref Ref, opts ...ContinueAsNewOption) error {
	if ctx == nil {
		return ErrUnsupported
	}
	cfg := ContinueAsNewOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if requester, ok := ctx.(interface {
		RequestContinueAsNew(ref Ref, opts ContinueAsNewOptions) error
	}); ok {
		return requester.RequestContinueAsNew(ref, cfg)
	}
	return ErrUnsupported
}

// ContinueAsNew restarts the current actor workflow with a new init payload.
func ContinueAsNew(ctx Ctx, payload any) error {
	if ctx == nil {
		return ErrUnsupported
	}
	if continuer, ok := ctx.(interface {
		ContinueAsNew(payload any) error
	}); ok {
		return continuer.ContinueAsNew(payload)
	}
	return ErrUnsupported
}

// Memo returns the workflow memo if the runtime exposes it.
func Memo(ctx Ctx) map[string]any {
	if ctx == nil {
		return nil
	}
	if reader, ok := ctx.(interface {
		Memo() map[string]any
	}); ok {
		return reader.Memo()
	}
	return nil
}

// BackgroundActivity launches an activity asynchronously and logs errors via the runtime.
func BackgroundActivity(ctx Ctx, name string, payload any) {
	if ctx == nil {
		return
	}
	ctx.BackgroundActivity(name, payload)
}

// SearchAttributes returns the workflow search attributes if the runtime exposes them.
func SearchAttributes(ctx Ctx) map[string]any {
	if ctx == nil {
		return nil
	}
	return ctx.SearchAttributes()
}

// UpsertSearchAttributes updates the workflow search attributes.
func UpsertSearchAttributes(ctx Ctx, attrs map[string]any) error {
	if ctx == nil {
		return ErrUnsupported
	}
	return ctx.UpsertSearchAttributes(attrs)
}

// Effect executes fn with deduplication keyed by key and persists the result.
func Effect[Resp any](ctx Ctx, key string, fn func(Ctx) (Resp, error), opts ...EffectOption) (Resp, error) {
	var zero Resp
	if ctx == nil {
		return zero, ErrUnsupported
	}
	if fn == nil {
		return zero, fmt.Errorf("actors: effect function is nil")
	}
	val, err := ctx.Effect(key, func(inner Ctx) (any, error) {
		return fn(inner)
	}, opts...)
	if err != nil {
		return zero, err
	}
	return decodeTypedResult[Resp](val)
}

// Message returns metadata about the currently processed message.
func Message(ctx Ctx) MessageMetadata {
	if ctx == nil {
		return MessageMetadata{}
	}
	return ctx.MessageMetadata()
}

// Correlation returns the current saga/tracing annotations associated with the message.
func Correlation(ctx Ctx) CorrelationData {
	if ctx == nil {
		return CorrelationData{}
	}
	return ctx.Correlation()
}

// SetCorrelation overrides the correlation annotations that downstream calls should inherit.
func SetCorrelation(ctx Ctx, data CorrelationData) {
	if ctx == nil {
		return
	}
	ctx.SetCorrelation(data)
}

// Snapshot returns snapshot metadata from the current context.
func Snapshot(ctx Ctx) SnapshotInfo {
	if ctx == nil {
		return SnapshotInfo{}
	}
	return ctx.SnapshotInfo()
}

// RunActivity executes an activity synchronously and decodes the typed result.
func RunActivity[Req TypedActivityMessage[Resp], Resp any](ctx Ctx, name string, payload Req, opts ...ActivityCallOption) (Resp, error) {
	var zero Resp
	if ctx == nil {
		return zero, ErrUnsupported
	}
	callOpts := buildActivityCallOptions(opts...)
	fut := ctx.ActivityWithOptions(name, payload, callOpts)
	val, err := fut.Get()
	if err != nil {
		return zero, err
	}
	if decoder, ok := ctx.(interface {
		DecodeActivityResult(name string, value any) (any, error)
	}); ok {
		decoded, derr := decoder.DecodeActivityResult(name, val)
		if derr != nil {
			return zero, derr
		}
		val = decoded
	}
	return decodeTypedResult[Resp](val)
}

// RunActivityNoResult executes an activity that only returns an error.
func RunActivityNoResult[Req TypedActivityMessage[struct{}]](ctx Ctx, name string, payload Req, opts ...ActivityCallOption) error {
	_, err := RunActivity[Req, struct{}](ctx, name, payload, opts...)
	return err
}

func decodeTypedResult[Resp any](val any) (Resp, error) {
	var zero Resp
	if val == nil {
		return zero, nil
	}
	if resp, ok := val.(Resp); ok {
		return resp, nil
	}
	data, err := codec.Marshal(val)
	if err != nil {
		return zero, fmt.Errorf("actors: encode response: %w", err)
	}
	if err := codec.Unmarshal(data, &zero); err != nil {
		return zero, fmt.Errorf("actors: decode response: %w", err)
	}
	return zero, nil
}

// EffectOptions configure effect persistence behavior.
type EffectOptions struct {
	TTL time.Duration
}

// EffectOption customizes EffectOptions.
type EffectOption func(*EffectOptions)

// WithEffectTTL controls how long effect results are retained for deduplication.
func WithEffectTTL(ttl time.Duration) EffectOption {
	return func(opts *EffectOptions) {
		opts.TTL = ttl
	}
}

// EffectFunc encapsulates a side effect function invoked via ctx.Effect.
type EffectFunc func(Ctx) (any, error)
