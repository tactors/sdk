package actors

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tactors/sdk/internal/codec"
)

// Actor is the runtime-facing representation of an actor definition.
type Actor interface {
	Kind() string
	Spec() *Description
}

// Description is a runtime-agnostic schema the builder emits. It deliberately avoids
// reflection-friendly types so runtimes can stay in the typed world.
type Description struct {
	Kind              string
	VersionTag        string
	Timeout           time.Duration
	Retry             RetryPolicy
	SignalTimeouts    map[string]time.Duration
	WorkflowQueue     string
	ActivityQueue     string
	StateFactory      func() any
	Start             StartHandler
	Commands          map[string]CommandSpec
	Queries           map[string]QuerySpec
	Activities        map[string]ActivitySpec
	activityDecoders  map[string]func(any) (any, error)
	ActivityTypes     map[string]string
	ActivityResults   map[string]string
	ActivityNames     map[string]string
	ActivityObservers []func(string, ActivityCallOptions)
	CommandTypes      map[string]string
	QueryTypes        map[string]string
	Patches           map[string]PatchSpec
	SnapshotEvery     int
	SnapshotArgs      func(any) (any, error)
}

// RetryPolicy captures a handful of knobs all runtimes typically expose.
type RetryPolicy struct {
	MaxAttempts        int
	InitialInterval    time.Duration
	BackoffCoefficient float64
}

// ActivityFunc adapts user functions into a runtime-callable activity.
type ActivityFunc func(context.Context, any) (any, error)

// ActivitySpec captures routing and defaults for an activity.
type ActivitySpec struct {
	Handler ActivityFunc
	Options ActivityCallOptions
}

// PatchSpec declares a forward-compatible patch gate.
type PatchSpec struct {
	ID        string
	DefaultOn bool
	Note      string
}

// StartHandler is invoked when a new actor instance spins up.
type StartHandler struct {
	Input          string
	payloadFactory func() any
	decodePayload  func(any) (any, error)
	fn             func(Ctx, any) (any, error)
}

// Invoke executes the user-supplied logic.
func (h StartHandler) Invoke(ctx Ctx, payload any) (any, error) {
	if h.fn == nil {
		return nil, nil
	}
	if h.decodePayload != nil && payload != nil {
		decoded, err := h.decodePayload(payload)
		if err != nil {
			return nil, err
		}
		payload = decoded
	}
	return h.fn(ctx, payload)
}

// CommandHandler routes inbound commands to user code.
type CommandHandler struct {
	Name  string
	Input string
	fn    func(Ctx, any, any) (any, error)
}

// Invoke executes the user-supplied command handler.
func (h CommandHandler) Invoke(ctx Ctx, state any, payload any) (any, error) {
	if h.fn == nil {
		return nil, fmt.Errorf("command %q has no handler", h.Name)
	}
	return h.fn(ctx, state, payload)
}

// QueryHandler routes read-only queries.
type QueryHandler struct {
	Name  string
	Input string
	fn    func(Ctx, any, any) (any, error)
}

// CommandSpec captures metadata for a command route.
type CommandSpec struct {
	Handler        CommandHandler
	Timeout        time.Duration
	Retry          RetryPolicy
	ResponseType   string
	PayloadFactory func() any
	DecodePayload  func(any) (any, error)
	Validator      func(any) error
}

// QuerySpec captures metadata for a query route.
type QuerySpec struct {
	Handler        QueryHandler
	CacheTTL       time.Duration
	ResponseType   string
	PayloadFactory func() any
	DecodePayload  func(any) (any, error)
}

// Invoke executes the user-supplied query handler.
func (h QueryHandler) Invoke(ctx Ctx, state any, payload any) (any, error) {
	if h.fn == nil {
		return nil, fmt.Errorf("query %q has no handler", h.Name)
	}
	return h.fn(ctx, state, payload)
}

// StatelessBuilder is the entry point - users can register timeouts/retries without committing to a
// stateful actor. Calling WithState transitions into a typed StatefulBuilder.
type StatelessBuilder struct {
	desc *Description
}

// StatefulBuilder wires up typed handlers for the provided state.
type StatefulBuilder[S any] struct {
	desc *Description
}

// StartAction is a typed adapter created via Start.
type StartAction[S any] struct {
	handler StartHandler
}

type statefulAction[S any] interface {
	apply(desc *Description)
}

// CommandAction is a typed adapter created via Command.
type CommandAction[S any] struct {
	name           string
	responseType   string
	handler        CommandHandler
	timeout        time.Duration
	retry          *RetryPolicy
	payloadFactory func() any
	decodePayload  func(any) (any, error)
	validator      func(any) error
}

// QueryAction is a typed adapter created via Query.
type QueryAction[S any] struct {
	name           string
	responseType   string
	handler        QueryHandler
	cacheTTL       time.Duration
	payloadFactory func() any
	decodePayload  func(any) (any, error)
}

type ActivityAction[S any] struct {
	name         string
	requestType  string
	responseType string
	fn           ActivityFunc
	decodeResult func(any) (any, error)
	defaults     ActivityCallOptions
}

type commandOptions struct {
	timeout   time.Duration
	retry     *RetryPolicy
	validator func(any) error
}

type CommandOption func(*commandOptions)

type ActivityOption[S any] func(*ActivityAction[S])

// WithActivityDefaults sets default call options for the activity route.
func WithActivityDefaults(opts ...ActivityCallOption) ActivityOption[any] {
	defaults := buildActivityCallOptions(opts...)
	return func(action *ActivityAction[any]) {
		action.defaults = defaults
	}
}

// WithTimeout overrides the default timeout for this command.
func WithTimeout(timeout time.Duration) CommandOption {
	return func(cfg *commandOptions) {
		cfg.timeout = timeout
	}
}

// WithRetry overrides the retry policy for this command.
func WithRetry(policy RetryPolicy) CommandOption {
	return func(cfg *commandOptions) {
		cfg.retry = &policy
	}
}

// WithValidator attaches a validation hook executed before the handler runs.
func WithValidator[Req any](fn func(Req) error) CommandOption {
	return func(cfg *commandOptions) {
		if fn == nil {
			return
		}
		cfg.validator = func(payload any) error {
			msg, err := expectType[Req](payload)
			if err != nil {
				return err
			}
			return fn(msg)
		}
	}
}

type queryOptions struct {
	cacheTTL time.Duration
}

type QueryOption func(*queryOptions)

// WithCache sets the cache hint for this query.
func WithCache(ttl time.Duration) QueryOption {
	return func(cfg *queryOptions) {
		cfg.cacheTTL = ttl
	}
}

func (a CommandAction[S]) key() string {
	if trimmed := strings.TrimSpace(a.name); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(a.handler.Name); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(a.handler.Input)
}

func (a QueryAction[S]) key() string {
	if trimmed := strings.TrimSpace(a.name); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(a.handler.Name); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(a.handler.Input)
}

func (a StartAction[S]) apply(desc *Description) {
	desc.Start = a.handler
}

func (a CommandAction[S]) apply(desc *Description) {
	if desc.Commands == nil {
		desc.Commands = make(map[string]CommandSpec)
	}
	if desc.CommandTypes == nil {
		desc.CommandTypes = make(map[string]string)
	}
	key := a.key()
	if key == "" {
		panic("actors: command action missing name")
	}
	if _, exists := desc.Commands[key]; exists {
		panic(fmt.Sprintf("actors: command %q already registered", key))
	}
	handler := a.handler
	handler.Name = key
	spec := CommandSpec{
		Handler:        handler,
		ResponseType:   a.responseType,
		PayloadFactory: a.payloadFactory,
		DecodePayload:  a.decodePayload,
		Validator:      a.validator,
	}
	if a.timeout > 0 {
		spec.Timeout = a.timeout
	}
	if a.retry != nil {
		spec.Retry = *a.retry
	}
	desc.Commands[key] = spec
	if handler.Input != "" {
		desc.CommandTypes[handler.Input] = key
	}
}

func (a QueryAction[S]) apply(desc *Description) {
	if desc.Queries == nil {
		desc.Queries = make(map[string]QuerySpec)
	}
	if desc.QueryTypes == nil {
		desc.QueryTypes = make(map[string]string)
	}
	key := a.key()
	if key == "" {
		panic("actors: query action missing name")
	}
	if _, exists := desc.Queries[key]; exists {
		panic(fmt.Sprintf("actors: query %q already registered", key))
	}
	handler := a.handler
	handler.Name = key
	spec := QuerySpec{
		Handler:        handler,
		ResponseType:   a.responseType,
		CacheTTL:       a.cacheTTL,
		PayloadFactory: a.payloadFactory,
		DecodePayload:  a.decodePayload,
	}
	desc.Queries[key] = spec
	if handler.Input != "" {
		desc.QueryTypes[handler.Input] = key
	}
}

func (a ActivityAction[S]) apply(desc *Description) {
	if desc.Activities == nil {
		desc.Activities = make(map[string]ActivitySpec)
	}
	if desc.activityDecoders == nil {
		desc.activityDecoders = make(map[string]func(any) (any, error))
	}
	if desc.ActivityTypes == nil {
		desc.ActivityTypes = make(map[string]string)
	}
	if desc.ActivityResults == nil {
		desc.ActivityResults = make(map[string]string)
	}
	if desc.ActivityNames == nil {
		desc.ActivityNames = make(map[string]string)
	}
	key := strings.TrimSpace(a.name)
	desc.Activities[key] = ActivitySpec{
		Handler: a.fn,
		Options: a.defaults,
	}
	if a.decodeResult != nil {
		desc.activityDecoders[key] = a.decodeResult
	}
	desc.ActivityTypes[key] = a.requestType
	desc.ActivityResults[key] = a.responseType
	desc.ActivityNames[a.requestType] = key
}

// New declares a new actor kind.
func New(kind string) StatelessBuilder {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		panic("actors: kind must be non-empty")
	}
	return StatelessBuilder{desc: newDescription(kind)}
}

// NewStateful declares a new actor kind with a typed state factory.
func NewStateful[S any](kind string, factory func() S) StatefulBuilder[S] {
	if factory == nil {
		panic("actors: state factory must be non-nil")
	}
	builder := New(kind)
	builder.desc.StateFactory = func() any {
		state := factory()
		return &state
	}
	return StatefulBuilder[S]{desc: builder.desc}
}

// WithTimeout configures the workflow execution timeout.
func (b StatelessBuilder) WithTimeout(timeout time.Duration) StatelessBuilder {
	b.desc.Timeout = timeout
	return b
}

// WithRetry sets the retry policy for this actor.
func (b StatelessBuilder) WithRetry(policy RetryPolicy) StatelessBuilder {
	b.desc.Retry = policy
	return b
}

// WithSignalTimeout sets a per-command timeout for inbound signals.
func (b StatelessBuilder) WithSignalTimeout(name string, timeout time.Duration) StatelessBuilder {
	if b.desc.SignalTimeouts == nil {
		b.desc.SignalTimeouts = make(map[string]time.Duration)
	}
	b.desc.SignalTimeouts[strings.TrimSpace(name)] = timeout
	return b
}

// WithWorkflowQueue overrides the default workflow task queue for this actor.
func (b StatelessBuilder) WithWorkflowQueue(name string) StatelessBuilder {
	b.desc.WorkflowQueue = strings.TrimSpace(name)
	return b
}

// WithActivityQueue overrides the default activity task queue for this actor.
func (b StatelessBuilder) WithActivityQueue(name string) StatelessBuilder {
	b.desc.ActivityQueue = strings.TrimSpace(name)
	return b
}

// WithActivity registers a callable that handlers can trigger via ctx.Activity.
func (b StatelessBuilder) WithActivity(name string, fn ActivityFunc) StatelessBuilder {
	if b.desc.Activities == nil {
		b.desc.Activities = make(map[string]ActivitySpec)
	}
	key := strings.TrimSpace(name)
	b.desc.Activities[key] = ActivitySpec{Handler: fn}
	return b
}

// Build finalizes the actor without any stateful behavior.
func (b StatelessBuilder) Build() Actor {
	return actorSpec{desc: b.desc.clone()}
}

// WithTimeout configures the workflow execution timeout.
func (b StatefulBuilder[S]) WithTimeout(timeout time.Duration) StatefulBuilder[S] {
	b.desc.Timeout = timeout
	return b
}

// WithRetry sets the retry policy for this actor.
func (b StatefulBuilder[S]) WithRetry(policy RetryPolicy) StatefulBuilder[S] {
	b.desc.Retry = policy
	return b
}

// WithSignalTimeout sets a per-command timeout for inbound signals.
func (b StatefulBuilder[S]) WithSignalTimeout(name string, timeout time.Duration) StatefulBuilder[S] {
	if b.desc.SignalTimeouts == nil {
		b.desc.SignalTimeouts = make(map[string]time.Duration)
	}
	b.desc.SignalTimeouts[strings.TrimSpace(name)] = timeout
	return b
}

// SnapshotConfig configures automatic ContinueAsNew rotation with state snapshots.
type SnapshotConfig[S any] struct {
	Every        int
	ContinueArgs func(state S) (any, error)
}

// WithSnapshot enables periodic snapshotting/ContinueAsNew for the actor state.
func (b StatefulBuilder[S]) WithSnapshot(cfg SnapshotConfig[S]) StatefulBuilder[S] {
	if cfg.Every <= 0 {
		panic("actors: snapshot Every must be > 0")
	}
	if cfg.ContinueArgs == nil {
		panic("actors: snapshot ContinueArgs must be provided")
	}
	b.desc.SnapshotEvery = cfg.Every
	b.desc.SnapshotArgs = func(state any) (any, error) {
		ptr, err := expectValue[S](state)
		if err != nil {
			return nil, err
		}
		return cfg.ContinueArgs(ptr)
	}
	return b
}

// WithWorkflowQueue overrides the workflow task queue for this actor.
func (b StatefulBuilder[S]) WithWorkflowQueue(name string) StatefulBuilder[S] {
	b.desc.WorkflowQueue = strings.TrimSpace(name)
	return b
}

// WithActivityQueue overrides the activity task queue for this actor.
func (b StatefulBuilder[S]) WithActivityQueue(name string) StatefulBuilder[S] {
	b.desc.ActivityQueue = strings.TrimSpace(name)
	return b
}

// DeclarePatch registers a patch identifier and its default activation state for metadata consumers.
func (b StatefulBuilder[S]) DeclarePatch(id string, defaultOn bool) StatefulBuilder[S] {
	id = strings.TrimSpace(id)
	if id == "" {
		panic("actors: patch id must be non-empty")
	}
	if b.desc.Patches == nil {
		b.desc.Patches = make(map[string]PatchSpec)
	}
	spec := b.desc.Patches[id]
	spec.ID = id
	spec.DefaultOn = defaultOn
	b.desc.Patches[id] = spec
	return b
}

// WithVersionTag annotates the actor description with a semantic version identifier.
func (b StatefulBuilder[S]) WithVersionTag(tag string) StatefulBuilder[S] {
	b.desc.VersionTag = strings.TrimSpace(tag)
	return b
}

// WithActivity registers a callable that handlers can trigger via ctx.Activity.
func (b StatefulBuilder[S]) WithActivity(name string, fn ActivityFunc) StatefulBuilder[S] {
	if b.desc.Activities == nil {
		b.desc.Activities = make(map[string]ActivitySpec)
	}
	key := strings.TrimSpace(name)
	b.desc.Activities[key] = ActivitySpec{Handler: fn}
	return b
}

// OnStart registers the initialization logic for the actor.
func (b StatefulBuilder[S]) OnStart(action StartAction[S]) StatefulBuilder[S] {
	action.apply(b.desc)
	return b
}

// With applies multiple actions such as commands, queries, or start hooks.
func (b StatefulBuilder[S]) With(actions ...statefulAction[S]) StatefulBuilder[S] {
	for _, action := range actions {
		if action != nil {
			action.apply(b.desc)
		}
	}
	return b
}

// Build finalizes the actor and returns something the app can register.
func (b StatefulBuilder[S]) Build() Actor {
	return actorSpec{desc: b.desc.clone()}
}

type actorSpec struct {
	desc *Description
}

func (a actorSpec) Kind() string       { return a.desc.Kind }
func (a actorSpec) Spec() *Description { return a.desc.clone() }

func newDescription(kind string) *Description {
	return &Description{
		Kind:             kind,
		Commands:         make(map[string]CommandSpec),
		Queries:          make(map[string]QuerySpec),
		SignalTimeouts:   make(map[string]time.Duration),
		Activities:       make(map[string]ActivitySpec),
		activityDecoders: make(map[string]func(any) (any, error)),
		ActivityTypes:    make(map[string]string),
		ActivityResults:  make(map[string]string),
		ActivityNames:    make(map[string]string),
		CommandTypes:     make(map[string]string),
		QueryTypes:       make(map[string]string),
		Patches:          make(map[string]PatchSpec),
	}
}

func (d *Description) clone() *Description {
	if d == nil {
		return nil
	}
	out := *d
	if d.Commands != nil {
		out.Commands = make(map[string]CommandSpec, len(d.Commands))
		for k, v := range d.Commands {
			out.Commands[k] = v
		}
	}
	if d.Queries != nil {
		out.Queries = make(map[string]QuerySpec, len(d.Queries))
		for k, v := range d.Queries {
			out.Queries[k] = v
		}
	}
	if d.SignalTimeouts != nil {
		out.SignalTimeouts = make(map[string]time.Duration, len(d.SignalTimeouts))
		for k, v := range d.SignalTimeouts {
			out.SignalTimeouts[k] = v
		}
	}
	if d.Activities != nil {
		out.Activities = make(map[string]ActivitySpec, len(d.Activities))
		for k, v := range d.Activities {
			out.Activities[k] = v
		}
	}
	if d.activityDecoders != nil {
		out.activityDecoders = make(map[string]func(any) (any, error), len(d.activityDecoders))
		for k, v := range d.activityDecoders {
			out.activityDecoders[k] = v
		}
	}
	if d.ActivityTypes != nil {
		out.ActivityTypes = make(map[string]string, len(d.ActivityTypes))
		for k, v := range d.ActivityTypes {
			out.ActivityTypes[k] = v
		}
	}
	if d.ActivityResults != nil {
		out.ActivityResults = make(map[string]string, len(d.ActivityResults))
		for k, v := range d.ActivityResults {
			out.ActivityResults[k] = v
		}
	}
	if d.ActivityObservers != nil {
		out.ActivityObservers = make([]func(string, ActivityCallOptions), len(d.ActivityObservers))
		copy(out.ActivityObservers, d.ActivityObservers)
	}
	if d.ActivityNames != nil {
		out.ActivityNames = make(map[string]string, len(d.ActivityNames))
		for k, v := range d.ActivityNames {
			out.ActivityNames[k] = v
		}
	}
	out.SnapshotEvery = d.SnapshotEvery
	out.SnapshotArgs = d.SnapshotArgs
	if d.CommandTypes != nil {
		out.CommandTypes = make(map[string]string, len(d.CommandTypes))
		for k, v := range d.CommandTypes {
			out.CommandTypes[k] = v
		}
	}
	if d.QueryTypes != nil {
		out.QueryTypes = make(map[string]string, len(d.QueryTypes))
		for k, v := range d.QueryTypes {
			out.QueryTypes[k] = v
		}
	}
	if d.Patches != nil {
		out.Patches = make(map[string]PatchSpec, len(d.Patches))
		for k, v := range d.Patches {
			out.Patches[k] = v
		}
	}
	return &out
}

// Clone returns a deep copy of the description maps so runtimes can safely mutate them.
func (d *Description) Clone() *Description {
	return d.clone()
}

// ActivityDecoders exposes the registered activity result decoders.
func (d *Description) ActivityDecoders() map[string]func(any) (any, error) {
	if d == nil {
		return nil
	}
	return d.activityDecoders
}

// ActivityDefaults exposes the registered per-activity default call options.
func (d *Description) ActivityDefaults() map[string]ActivityCallOptions {
	if d == nil || len(d.Activities) == 0 {
		return nil
	}
	out := make(map[string]ActivityCallOptions, len(d.Activities))
	for name, spec := range d.Activities {
		out[name] = spec.Options
	}
	return out
}

// PatchSpecs returns the declared patch metadata sorted by identifier.
func (d *Description) PatchSpecs() []PatchSpec {
	if d == nil || len(d.Patches) == 0 {
		return nil
	}
	ids := make([]string, 0, len(d.Patches))
	for id := range d.Patches {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]PatchSpec, 0, len(ids))
	for _, id := range ids {
		out = append(out, d.Patches[id])
	}
	return out
}

func expectType[T any](payload any) (T, error) {
	var zero T
	if payload == nil {
		return zero, nil
	}
	val, ok := payload.(T)
	if !ok {
		return zero, fmt.Errorf("expected payload %T, got %T", zero, payload)
	}
	return val, nil
}

func expectPtr[T any](value any) (*T, error) {
	if value == nil {
		return nil, fmt.Errorf("state is nil")
	}
	ptr, ok := value.(*T)
	if !ok {
		return nil, fmt.Errorf("expected state *%s, got %T", TypeName[T](), value)
	}
	return ptr, nil
}

func expectValue[T any](value any) (T, error) {
	var zero T
	if typed, ok := value.(T); ok {
		return typed, nil
	}
	// If a pointer sneaks in, dereference if possible.
	if ptr, ok := value.(*T); ok {
		return *ptr, nil
	}
	return zero, fmt.Errorf("expected state %T, got %T", zero, value)
}

// TypeName returns the fully qualified name of the supplied type parameter.
func TypeName[T any]() string {
	var zero T
	return typeString(zero)
}

func typeString(value any) string {
	return fmt.Sprintf("%T", value)
}

func payloadFactory[T any]() func() any {
	return func() any {
		var zero T
		value := new(T)
		*value = zero
		return value
	}
}

func payloadDecoder[T any](name string) func(any) (any, error) {
	return func(val any) (any, error) {
		if val == nil {
			var zero T
			return zero, nil
		}
		switch typed := val.(type) {
		case T:
			return typed, nil
		case *T:
			return *typed, nil
		case []byte:
			var out T
			if len(typed) == 0 {
				return out, nil
			}
			if err := codec.Unmarshal(typed, &out); err != nil {
				return out, fmt.Errorf("expected payload %s, decode error: %w", name, err)
			}
			return out, nil
		default:
			var out T
			blob, err := codec.Marshal(typed)
			if err != nil {
				return out, fmt.Errorf("expected payload %s, encode error: %w", name, err)
			}
			if err := codec.Unmarshal(blob, &out); err != nil {
				return out, fmt.Errorf("expected payload %s, decode error: %w", name, err)
			}
			return out, nil
		}
	}
}

// TypeKeyOf returns the type string used to map commands/queries to names.
func TypeKeyOf(value any) string {
	if value == nil {
		return ""
	}
	return typeString(value)
}

// Start lifts a typed init function into an action the builder understands.
func Start[S any, P any](fn func(Ctx, P) (S, error)) StartAction[S] {
	return StartAction[S]{
		handler: StartHandler{
			Input:          TypeName[P](),
			payloadFactory: payloadFactory[P](),
			decodePayload:  payloadDecoder[P](TypeName[P]()),
			fn: func(ctx Ctx, payload any) (any, error) {
				msg, err := expectType[P](payload)
				if err != nil {
					return nil, err
				}
				state, err := fn(ctx, msg)
				if err != nil {
					return nil, err
				}
				return &state, nil
			},
		},
	}
}

// Command lifts a typed command handler into an action the builder understands.
func Command[S any, Req any, Resp any](fn func(Ctx, *S, Req) (Resp, error), opts ...CommandOption) CommandAction[S] {
	cfg := commandOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}
	reqName := TypeName[Req]()
	return CommandAction[S]{
		name:           reqName,
		responseType:   TypeName[Resp](),
		timeout:        cfg.timeout,
		retry:          cfg.retry,
		validator:      cfg.validator,
		payloadFactory: payloadFactory[Req](),
		decodePayload:  payloadDecoder[Req](reqName),
		handler: CommandHandler{
			Input: reqName,
			fn: func(ctx Ctx, state any, payload any) (any, error) {
				st, err := expectPtr[S](state)
				if err != nil {
					return nil, err
				}
				msg, err := expectType[Req](payload)
				if err != nil {
					return nil, err
				}
				return fn(ctx, st, msg)
			},
		},
	}
}

// CommandFunc infers the request/response types from the handler signature.
func CommandFunc[S any, Req TypedCommandMessage[Resp], Resp any](fn func(Ctx, *S, Req) (Resp, error), opts ...CommandOption) CommandAction[S] {
	return Command[S](fn, opts...)
}

// Query lifts a typed query handler into an action the builder understands.
func Query[S any, Req any, Resp any](fn func(Ctx, S, Req) (Resp, error), opts ...QueryOption) QueryAction[S] {
	cfg := queryOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}
	reqName := TypeName[Req]()
	return QueryAction[S]{
		name:           reqName,
		responseType:   TypeName[Resp](),
		cacheTTL:       cfg.cacheTTL,
		payloadFactory: payloadFactory[Req](),
		decodePayload:  payloadDecoder[Req](reqName),
		handler: QueryHandler{
			Input: reqName,
			fn: func(ctx Ctx, state any, payload any) (any, error) {
				st, err := expectValue[S](state)
				if err != nil {
					return nil, err
				}
				msg, err := expectType[Req](payload)
				if err != nil {
					return nil, err
				}
				return fn(ctx, st, msg)
			},
		},
	}
}

// QueryFunc infers the request/response types from the handler signature.
func QueryFunc[S any, Req TypedQueryMessage[Resp], Resp any](fn func(Ctx, S, Req) (Resp, error), opts ...QueryOption) QueryAction[S] {
	return Query[S](fn, opts...)
}

// Activity registers a typed activity using the payload type as the route name.
func Activity[P any, R any](fn func(context.Context, P) (R, error), opts ...ActivityOption[any]) ActivityAction[any] {
	return ActivityNamed(TypeName[P](), fn, opts...)
}

// ActivityNamed registers an activity with an explicit name.
func ActivityNamed[P any, R any](name string, fn func(context.Context, P) (R, error), opts ...ActivityOption[any]) ActivityAction[any] {
	action := ActivityAction[any]{
		name:         strings.TrimSpace(name),
		requestType:  TypeName[P](),
		responseType: TypeName[R](),
		decodeResult: func(val any) (any, error) {
			if val == nil {
				var zero R
				return zero, nil
			}
			if resp, ok := val.(R); ok {
				return resp, nil
			}
			data, err := codec.Marshal(val)
			if err != nil {
				return nil, err
			}
			var out R
			if err := codec.Unmarshal(data, &out); err != nil {
				return nil, err
			}
			return out, nil
		},
		fn: func(ctx context.Context, payload any) (any, error) {
			decoded, err := payloadDecoder[P](TypeName[P]())(payload)
			if err != nil {
				return nil, err
			}
			msg, ok := decoded.(P)
			if !ok {
				return nil, fmt.Errorf("expected payload %s, got %T", TypeName[P](), decoded)
			}
			return fn(ctx, msg)
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&action)
		}
	}
	return action
}

// ActivityNoResult registers an activity that only returns an error using the payload type as the route name.
func ActivityNoResult[P any](fn func(context.Context, P) error, opts ...ActivityOption[any]) ActivityAction[any] {
	return Activity(func(ctx context.Context, p P) (struct{}, error) {
		return struct{}{}, fn(ctx, p)
	}, opts...)
}

// ActivityNoResultNamed registers a no-result activity with an explicit name.
func ActivityNoResultNamed[P any](name string, fn func(context.Context, P) error, opts ...ActivityOption[any]) ActivityAction[any] {
	return ActivityNamed(name, func(ctx context.Context, p P) (struct{}, error) {
		return struct{}{}, fn(ctx, p)
	}, opts...)
}

// ActivityAuto registers an activity using the payload type as the route name.
func ActivityAuto[P any, R any](fn func(context.Context, P) (R, error)) ActivityAction[any] {
	return ActivityNamed(TypeName[P](), fn)
}

// ActivityNoResultAuto registers a no-result activity using the payload type as the route name.
func ActivityNoResultAuto[P any](fn func(context.Context, P) error) ActivityAction[any] {
	return ActivityNoResult(fn)
}
