package actors

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventSignalNameNamespacesAndValidates(t *testing.T) {
	name, err := EventSignalName("approve")
	require.NoError(t, err)
	require.Equal(t, "__actors_event:approve", name)
	require.NotEqual(t, "approve", name, "event signal must differ from a same-named command signal")

	trimmed, err := EventSignalName("  approve ")
	require.NoError(t, err)
	require.Equal(t, name, trimmed)

	_, err = EventSignalName("")
	require.Error(t, err)
	_, err = EventSignalName("   ")
	require.Error(t, err)
	_, err = EventSignalName("__actors_query_request")
	require.Error(t, err, "reserved runtime signals must not be reachable as events")
}

type eventCaptureInvoker struct {
	captureInvoker
	deliverName string
	deliverRef  Ref
}

func (c *eventCaptureInvoker) DeliverEvent(ctx context.Context, ref Ref, name string, payload any) error {
	c.deliverName = name
	c.deliverRef = ref
	return nil
}

func TestDeliverEventPrefersEventDeliverer(t *testing.T) {
	inv := &eventCaptureInvoker{}
	RegisterClientInvoker(func(Ref) ClientInvoker { return inv })
	t.Cleanup(func() { RegisterClientInvoker(nil) })
	ref := ARef("kind", "id")
	require.NoError(t, DeliverEvent(context.Background(), ref, "approve", "payload"))
	require.Equal(t, "approve", inv.deliverName)
	require.Equal(t, ref, inv.deliverRef)
	require.Empty(t, inv.commandMethod, "must not fall back to InvokeCommand")
}

func TestDeliverEventFallsBackToNamespacedCommandSignal(t *testing.T) {
	inv := &captureInvoker{}
	RegisterClientInvoker(func(Ref) ClientInvoker { return inv })
	t.Cleanup(func() { RegisterClientInvoker(nil) })
	require.NoError(t, DeliverEvent(context.Background(), ARef("kind", "id"), "approve", "payload"))
	require.Equal(t, EventSignalPrefix+"approve", inv.commandMethod)
}

func TestDeliverEventRejectsInvalidNames(t *testing.T) {
	inv := &captureInvoker{}
	RegisterClientInvoker(func(Ref) ClientInvoker { return inv })
	t.Cleanup(func() { RegisterClientInvoker(nil) })
	require.Error(t, DeliverEvent(context.Background(), ARef("kind", "id"), "", nil))
	require.Empty(t, inv.commandMethod)
}

func TestDeliverEventUnsupportedWithoutInvoker(t *testing.T) {
	RegisterClientInvoker(nil)
	err := DeliverEvent(context.Background(), ARef("kind", "id"), "approve", nil)
	require.True(t, errors.Is(err, ErrUnsupported))
}

type eventStubCtx struct {
	Ctx
	payload any
	err     error
	name    string
	timeout time.Duration
}

func (s *eventStubCtx) WaitForEvent(name string, timeout time.Duration) (any, error) {
	s.name = name
	s.timeout = timeout
	return s.payload, s.err
}

func TestWaitForEventAsDecodesThroughCodec(t *testing.T) {
	type approval struct {
		Approver string
		OK       bool
	}
	// Runtimes hand back codec-decoded generic values (CBOR maps), not typed structs.
	stub := &eventStubCtx{payload: map[string]any{"Approver": "alice", "OK": true}}
	got, err := WaitForEventAs[approval](stub, "approve", time.Minute)
	require.NoError(t, err)
	require.Equal(t, approval{Approver: "alice", OK: true}, got)
	require.Equal(t, "approve", stub.name)
	require.Equal(t, time.Minute, stub.timeout)

	stub = &eventStubCtx{err: ErrEventTimeout}
	_, err = WaitForEventAs[approval](stub, "approve", time.Minute)
	require.ErrorIs(t, err, ErrEventTimeout)

	_, err = WaitForEventAs[approval](nil, "approve", time.Minute)
	require.ErrorIs(t, err, ErrUnsupported)
}

func TestSendEventRequiresRuntimeSupport(t *testing.T) {
	require.ErrorIs(t, SendEvent(nil, Ref{}, "approve", nil), ErrUnsupported)
	require.ErrorIs(t, SendEvent(&eventStubCtx{}, Ref{}, "approve", nil), ErrUnsupported)
}
