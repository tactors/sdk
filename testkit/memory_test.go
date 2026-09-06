package testkit

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
)

type memoryApproval struct {
	Approver string
	OK       bool
}

func TestMemoryCtxWaitForEventReturnsPayload(t *testing.T) {
	ctx := NewMemoryCtx("mem-1")
	done := make(chan memoryApproval, 1)
	errs := make(chan error, 1)
	go func() {
		val, err := actors.WaitForEventAs[memoryApproval](ctx, "approve", time.Second)
		errs <- err
		done <- val
	}()
	// Give the handler a moment to park, then deliver.
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, ctx.DeliverEvent("approve", memoryApproval{Approver: "alice", OK: true}))
	select {
	case err := <-errs:
		require.NoError(t, err)
		require.Equal(t, memoryApproval{Approver: "alice", OK: true}, <-done)
	case <-time.After(time.Second):
		t.Fatal("handler did not resume after event delivery")
	}
}

func TestMemoryCtxWaitForEventTimesOut(t *testing.T) {
	ctx := NewMemoryCtx("mem-2")
	type result struct {
		val any
		err error
	}
	results := make(chan result, 1)
	start := time.Now()
	go func() {
		val, err := ctx.WaitForEvent("approve", 20*time.Millisecond)
		results <- result{val: val, err: err}
	}()
	select {
	case res := <-results:
		require.Nil(t, res.val)
		require.ErrorIs(t, res.err, actors.ErrEventTimeout)
		require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForEvent did not honour its timeout")
	}
}

func TestMemoryCtxWaitForEventBuffersEarlyDelivery(t *testing.T) {
	ctx := NewMemoryCtx("mem-3")
	require.NoError(t, ctx.DeliverEvent("approve", "first"))
	require.Equal(t, 1, ctx.PendingEvents("approve"))
	val, err := ctx.WaitForEvent("approve", 0)
	require.NoError(t, err)
	require.Equal(t, "first", val)
	require.Equal(t, 0, ctx.PendingEvents("approve"))
}

func TestMemoryCtxEventsAreNamespacedFromCommands(t *testing.T) {
	ctx := NewMemoryCtx("mem-4")
	require.NoError(t, ctx.DeliverEvent("approve", "x"))
	require.Equal(t, 0, ctx.PendingEvents("other"))
	require.Equal(t, 1, ctx.PendingEvents("approve"))
	// Names in the reserved runtime namespace are rejected on both ends.
	require.Error(t, ctx.DeliverEvent("__actors_ask_request", "x"))
	_, err := ctx.WaitForEvent("", time.Millisecond)
	require.Error(t, err)
}

// Constraint 2 in-memory: handler A waits for E while command B is queued
// behind it in a serial mailbox; E is delivered, A completes, then B runs.
func TestMemoryCtxQueuedCommandRunsAfterWaitingHandler(t *testing.T) {
	ctx := NewMemoryCtx("mem-5")
	var mu sync.Mutex
	var log []string
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, s)
	}
	handlerA := func() {
		record("A:start")
		_, err := ctx.WaitForEvent("E", time.Second)
		if err != nil {
			record("A:error")
			return
		}
		record("A:done")
	}
	handlerB := func() { record("B") }

	// A serial mailbox: one goroutine drains commands in order, exactly like
	// the Temporal command loop runs handlers one at a time.
	mailbox := make(chan func(), 2)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for fn := range mailbox {
			fn()
		}
	}()
	mailbox <- handlerA
	mailbox <- handlerB
	close(mailbox)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(log) == 1 && log[0] == "A:start"
	}, time.Second, time.Millisecond, "B must not run while A is suspended")
	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	require.Equal(t, []string{"A:start"}, log)
	mu.Unlock()

	require.NoError(t, ctx.DeliverEvent("E", "go"))
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("mailbox did not drain after event delivery")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"A:start", "A:done", "B"}, log)
}

func TestMemoryCtxImplementsCtx(t *testing.T) {
	var ctx actors.Ctx = NewMemoryCtx("mem-6")
	require.Equal(t, "mem-6", ctx.ActorID())
	_, err := ctx.Activity("noop", nil).Get()
	require.True(t, errors.Is(err, actors.ErrUnsupported))
	require.Error(t, actors.SendEvent(ctx, actors.ARef("k", "id"), "approve", nil))
}
