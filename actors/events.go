package actors

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// EventSignalPrefix namespaces external events away from command signals and
// the runtime's internal "__actors_*" signals. An event named "approve" is
// delivered on the signal "__actors_event:approve", so a user event can never
// hijack a command route (or vice versa) even when both share a name.
const EventSignalPrefix = "__actors_event:"

// ErrEventTimeout is returned by Ctx.WaitForEvent when the timeout elapses
// before the named event is delivered. Compare with errors.Is.
var ErrEventTimeout = errors.New("actors: event wait timed out")

// EventSignalName returns the namespaced signal name used to deliver the
// event called name. It is the single place that maps user event names to
// transport names; both WaitForEvent and DeliverEvent go through it.
func EventSignalName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("actors: event name is required")
	}
	if strings.HasPrefix(name, "__actors_") {
		return "", fmt.Errorf("actors: event name %q must not use the reserved __actors_ prefix", name)
	}
	return EventSignalPrefix + name, nil
}

// WaitForEventAs suspends the handler until the named event arrives and decodes
// the payload into T using the SDK codec (the same CBOR round-trip used for
// command and activity results). See Ctx.WaitForEvent for the semantics.
func WaitForEventAs[T any](ctx Ctx, name string, timeout time.Duration) (T, error) {
	var zero T
	if ctx == nil {
		return zero, ErrUnsupported
	}
	val, err := ctx.WaitForEvent(name, timeout)
	if err != nil {
		return zero, err
	}
	return decodeTypedResult[T](val)
}

// SendEvent delivers an event to another actor from inside a handler. It is the
// workflow-side counterpart of DeliverEvent and uses the same namespaced signal.
// Runtimes that cannot signal other actors return ErrUnsupported.
func SendEvent(ctx Ctx, ref Ref, name string, payload any) error {
	if ctx == nil {
		return ErrUnsupported
	}
	if sender, ok := ctx.(interface {
		SendEvent(ref Ref, name string, payload any) error
	}); ok {
		return sender.SendEvent(ref, name, payload)
	}
	return ErrUnsupported
}

// EventDeliverer is an optional interface a ClientInvoker may implement to
// deliver events to a running actor without the signal-with-start semantics
// of InvokeCommand. When the registered invoker does not implement it,
// DeliverEvent falls back to InvokeCommand on the namespaced signal name.
type EventDeliverer interface {
	DeliverEvent(ctx context.Context, ref Ref, name string, payload any) error
}

// DeliverEvent pushes a named event into a running actor from outside the
// workflow (HTTP gateways, webhook receivers, approval UIs). The payload is
// encoded with the same codec as command payloads and is buffered by the
// runtime until a handler calls Ctx.WaitForEvent(name, ...). With the
// runtime's invokers, delivering to an actor that is not running does not
// start it; the not-found error is returned. A ClientInvoker that does not
// implement EventDeliverer falls back to InvokeCommand, whose contract does
// start the actor.
func DeliverEvent(ctx context.Context, ref Ref, name string, payload any) error {
	signal, err := EventSignalName(name)
	if err != nil {
		return err
	}
	invoker := clientInvokerFactory(ref)
	if deliverer, ok := invoker.(EventDeliverer); ok {
		return deliverer.DeliverEvent(ctx, ref, name, payload)
	}
	return invoker.InvokeCommand(ctx, ref, signal, payload)
}
