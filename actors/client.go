package actors

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/tactors/sdk/internal/rand"
)

// ClientInvoker drives ask/tell/query operations from non-actor processes (e.g., HTTP gateways).
type ClientInvoker interface {
	InvokeCommand(ctx context.Context, ref Ref, method string, payload any) error
	InvokeAsk(ctx context.Context, ref Ref, method string, payload any, resp any, opts AskOptions) error
	InvokeQuery(ctx context.Context, ref Ref, method string, payload any, resp any) error
}

var (
	clientInvokerMu sync.RWMutex
	clientInvokerFn = func(Ref) ClientInvoker { return noopClientInvoker{} }
)

// RegisterClientInvoker installs the factory used by Invoke* helpers. The last registration wins.
func RegisterClientInvoker(factory func(Ref) ClientInvoker) {
	clientInvokerMu.Lock()
	defer clientInvokerMu.Unlock()
	if factory == nil {
		clientInvokerFn = func(Ref) ClientInvoker { return noopClientInvoker{} }
		return
	}
	clientInvokerFn = factory
}

func clientInvokerFactory(ref Ref) ClientInvoker {
	clientInvokerMu.RLock()
	fn := clientInvokerFn
	clientInvokerMu.RUnlock()
	return fn(ref)
}

// InvokeCommand sends a fire-and-forget command to the target actor.
func InvokeCommand(ctx context.Context, ref Ref, payload any) error {
	method := TypeKeyOf(payload)
	if method == "" {
		return fmt.Errorf("actors: cannot infer command type for %T", payload)
	}
	return InvokeCommandNamed(ctx, ref, method, payload)
}

// InvokeCommandNamed sends a fire-and-forget command to an explicit runtime route.
func InvokeCommandNamed(ctx context.Context, ref Ref, method string, payload any) error {
	method = strings.TrimSpace(method)
	if method == "" {
		return fmt.Errorf("actors: command method is required")
	}
	return clientInvokerFactory(ref).InvokeCommand(ctx, ref, method, payload)
}

// InvokeAsk executes a command and waits for the typed response.
func InvokeAsk[R any](ctx context.Context, ref Ref, payload any, opts ...AskOption) (R, error) {
	var zero R
	method := TypeKeyOf(payload)
	if method == "" {
		return zero, fmt.Errorf("actors: cannot infer command type for %T", payload)
	}
	cfg := AskOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.CorrelationID == "" {
		cfg.CorrelationID = rand.ID("ask")
	}
	if err := invokeAskNamed(ctx, ref, method, payload, &zero, cfg); err != nil {
		return zero, err
	}
	return zero, nil
}

// InvokeAskNamed executes an explicit command route and waits for the typed response.
func InvokeAskNamed[R any](ctx context.Context, ref Ref, method string, payload any, opts ...AskOption) (R, error) {
	var zero R
	cfg := AskOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.CorrelationID == "" {
		cfg.CorrelationID = rand.ID("ask")
	}
	if err := invokeAskNamed(ctx, ref, method, payload, &zero, cfg); err != nil {
		return zero, err
	}
	return zero, nil
}

// InvokeQuery executes a read-only query against the actor workflow.
func InvokeQuery[R any](ctx context.Context, ref Ref, payload any) (R, error) {
	var zero R
	method := TypeKeyOf(payload)
	if method == "" {
		return zero, fmt.Errorf("actors: cannot infer query type for %T", payload)
	}
	if err := invokeQueryNamed(ctx, ref, method, payload, &zero); err != nil {
		return zero, err
	}
	return zero, nil
}

// InvokeQueryNamed executes an explicit read-only query route.
func InvokeQueryNamed[R any](ctx context.Context, ref Ref, method string, payload any) (R, error) {
	var zero R
	if err := invokeQueryNamed(ctx, ref, method, payload, &zero); err != nil {
		return zero, err
	}
	return zero, nil
}

// AskOptions exposes optional metadata for ask calls.
type AskOptions struct {
	CorrelationID string
}

// AskOption customizes AskOptions.
type AskOption func(*AskOptions)

// WithCorrelationID overrides the correlation identifier attached to the ask.
func WithCorrelationID(id string) AskOption {
	return func(opts *AskOptions) {
		opts.CorrelationID = id
	}
}

func invokeAskNamed(ctx context.Context, ref Ref, method string, payload any, resp any, opts AskOptions) error {
	method = strings.TrimSpace(method)
	if method == "" {
		return fmt.Errorf("actors: ask method is required")
	}
	return clientInvokerFactory(ref).InvokeAsk(ctx, ref, method, payload, resp, opts)
}

func invokeQueryNamed(ctx context.Context, ref Ref, method string, payload any, resp any) error {
	method = strings.TrimSpace(method)
	if method == "" {
		return fmt.Errorf("actors: query method is required")
	}
	return clientInvokerFactory(ref).InvokeQuery(ctx, ref, method, payload, resp)
}

type noopClientInvoker struct{}

func (noopClientInvoker) InvokeCommand(context.Context, Ref, string, any) error {
	return ErrUnsupported
}
func (noopClientInvoker) InvokeAsk(context.Context, Ref, string, any, any, AskOptions) error {
	return ErrUnsupported
}
func (noopClientInvoker) InvokeQuery(context.Context, Ref, string, any, any) error {
	return ErrUnsupported
}
