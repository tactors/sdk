package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type approvalEvent struct {
	Approver string
	OK       bool
}

type eventWaitCommand struct {
	actors.CommandMsg[struct{}]
	Tag     string
	Event   string
	Timeout time.Duration
}

type eventNoteCommand struct {
	actors.CommandMsg[struct{}]
	Tag string
}

type eventStateQuery struct {
	actors.QueryMsg[eventActorState]
}

type eventActorState struct {
	Log      []string
	Approval approvalEvent
}

// eventTrace captures handler-side observations across the workflow coroutine
// so tests can assert on errors that never reach workflow state.
type eventTrace struct {
	mu   sync.Mutex
	errs []error
}

func (t *eventTrace) record(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.errs = append(t.errs, err)
}

func (t *eventTrace) errors() []error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]error(nil), t.errs...)
}

func newEventActor(kind string, trace *eventTrace, extra ...actors.CommandAction[eventActorState]) actors.Actor {
	builder := actors.NewStateful(kind, func() eventActorState { return eventActorState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *eventActorState, cmd eventWaitCommand) (struct{}, error) {
				st.Log = append(st.Log, "wait:"+cmd.Tag+":start")
				approval, err := actors.WaitForEventAs[approvalEvent](ctx, cmd.Event, cmd.Timeout)
				switch {
				case err == nil:
					st.Approval = approval
					st.Log = append(st.Log, "wait:"+cmd.Tag+":done")
				case errors.Is(err, actors.ErrEventTimeout):
					st.Log = append(st.Log, "wait:"+cmd.Tag+":timeout")
				default:
					st.Log = append(st.Log, "wait:"+cmd.Tag+":error")
					if trace != nil {
						trace.record(err)
					}
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *eventActorState, cmd eventNoteCommand) (struct{}, error) {
				st.Log = append(st.Log, "note:"+cmd.Tag)
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st eventActorState, _ eventStateQuery) (eventActorState, error) {
				return st, nil
			}),
			stopCommandAction[eventActorState](),
		)
	for _, action := range extra {
		builder = builder.With(action)
	}
	return builder.Build()
}

func queryEventState(t *testing.T, env *testsuite.TestWorkflowEnvironment) eventActorState {
	t.Helper()
	payload, err := codec.Marshal(eventStateQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(actors.TypeKeyOf(eventStateQuery{}), payload)
	require.NoError(t, err)
	var st eventActorState
	require.NoError(t, value.Get(&st))
	return st
}

func eventSignal(t *testing.T, name string) string {
	t.Helper()
	signal, err := actors.EventSignalName(name)
	require.NoError(t, err)
	return signal
}

func TestWaitForEventReturnsDeliveredPayload(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newEventActor("event-deliver", nil))
	waitSignal := actors.TypeKeyOf(eventWaitCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var st eventActorState
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "A", Event: "approve", Timeout: time.Hour})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "approve"), approvalEvent{Approver: "alice", OK: true})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		st = queryEventState(t, env)
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 4*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "event-deliver-1", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"wait:A:start", "wait:A:done"}, st.Log)
	require.Equal(t, approvalEvent{Approver: "alice", OK: true}, st.Approval, "payload must round-trip through the SDK codec")
}

func TestWaitForEventTimesOut(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newEventActor("event-timeout", nil))
	waitSignal := actors.TypeKeyOf(eventWaitCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var st eventActorState
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "A", Event: "approve", Timeout: 50 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		// Still waiting well before the timeout elapses.
		st = queryEventState(t, env)
		require.Equal(t, []string{"wait:A:start"}, st.Log)
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		st = queryEventState(t, env)
	}, 100*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 101*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "event-timeout-1", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"wait:A:start", "wait:A:timeout"}, st.Log)
	require.Equal(t, approvalEvent{}, st.Approval)
}

// Constraint 2: handler A parks in WaitForEvent, command B arrives meanwhile,
// event E is delivered, A completes, then B runs. B must not run while A is
// suspended and must not be lost.
func TestWaitForEventQueuesConcurrentCommands(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newEventActor("event-queue", nil))
	waitSignal := actors.TypeKeyOf(eventWaitCommand{})
	noteSignal := actors.TypeKeyOf(eventNoteCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var during, after eventActorState
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(noteSignal, eventNoteCommand{Tag: "B"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		// B has arrived but A is still suspended: B must be queued, not run.
		during = queryEventState(t, env)
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "bob", OK: true})
	}, 4*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		after = queryEventState(t, env)
	}, 5*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 6*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "event-queue-1", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"wait:A:start"}, during.Log, "B must queue behind the suspended handler")
	require.Equal(t, []string{"wait:A:start", "wait:A:done", "note:B"}, after.Log, "B must run after A resumes")
	require.Equal(t, "bob", after.Approval.Approver)
}

// Constraint 3: an event and a command sharing the name "approve" must not
// interfere. A raw "approve" signal is a command and cannot satisfy the wait;
// an "approve" event cannot invoke the command. Also covers buffering: an
// event delivered before anyone waits is returned by the next wait.
func TestWaitForEventIsNamespacedFromCommands(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	approveCommand := actors.CommandNamed("approve", func(ctx actors.Ctx, st *eventActorState, _ approvalEvent) (struct{}, error) {
		st.Log = append(st.Log, "cmd:approve")
		return struct{}{}, nil
	})
	runner := registerActorWorkflow(t, env, newEventActor("event-namespace", nil, approveCommand))
	waitSignal := actors.TypeKeyOf(eventWaitCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var st eventActorState
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "A", Event: "approve", Timeout: 30 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		// Raw command signal named like the event: must queue as a command, not satisfy the wait.
		env.SignalWorkflow("approve", approvalEvent{Approver: "cmd"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		// Nobody is waiting: the event must buffer and must not invoke the command.
		env.SignalWorkflow(eventSignal(t, "approve"), approvalEvent{Approver: "evt", OK: true})
	}, 50*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "C", Event: "approve", Timeout: time.Hour})
	}, 60*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		st = queryEventState(t, env)
	}, 61*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 62*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "event-namespace-1", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{
		"wait:A:start",
		"wait:A:timeout", // the "approve" command signal did not satisfy the wait
		"cmd:approve",    // ...and ran as a command once A returned
		"wait:C:start",
		"wait:C:done", // buffered event consumed immediately; no second cmd:approve
	}, st.Log)
	require.Equal(t, "evt", st.Approval.Approver)
}

// Command-level timeouts cancel the handler context; the wait must unblock
// with the cancellation error rather than hanging or reporting ErrEventTimeout.
func TestWaitForEventUnblocksOnCommandTimeout(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &eventTrace{}
	actor := actors.NewStateful("event-cancel", func() eventActorState { return eventActorState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *eventActorState, cmd eventWaitCommand) (struct{}, error) {
				_, err := ctx.WaitForEvent(cmd.Event, 0)
				trace.record(err)
				return struct{}{}, nil
			}, actors.WithTimeout(20*time.Millisecond)),
		).
		Build()
	runner := registerActorWorkflow(t, env, actor)
	waitSignal := actors.TypeKeyOf(eventWaitCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "A", Event: "never"})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "event-cancel-1", struct{}{})
	// The command loop converts the cancelled handler into a command timeout,
	// which is a fatal (non-business) error for a single-attempt command.
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out after 20ms")
	errs := trace.errors()
	require.Len(t, errs, 1)
	require.Error(t, errs[0])
	require.True(t, temporal.IsCanceledError(errs[0]), "expected cancellation, got %v", errs[0])
	require.False(t, errors.Is(errs[0], actors.ErrEventTimeout))
}

func TestWaitForEventRejectsInvalidNames(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &eventTrace{}
	runner := registerActorWorkflow(t, env, newEventActor("event-invalid", trace))
	waitSignal := actors.TypeKeyOf(eventWaitCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "A", Event: "  ", Timeout: time.Hour})
		env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "B", Event: "__actors_query_request", Timeout: time.Hour})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 2*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "event-invalid-1", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	errs := trace.errors()
	require.Len(t, errs, 2)
	require.ErrorContains(t, errs[0], "event name is required")
	require.ErrorContains(t, errs[1], "reserved __actors_ prefix")
}

func TestSendEventFromWorkflowUsesNamespacedSignal(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	sendCommand := actors.Command(func(ctx actors.Ctx, st *eventActorState, cmd eventNoteCommand) (struct{}, error) {
		target := actors.ARef("event-send", "peer")
		return struct{}{}, actors.SendEvent(ctx, target, "approve", approvalEvent{Approver: cmd.Tag, OK: true})
	})
	actor := actors.NewStateful("event-send", func() eventActorState { return eventActorState{} }).
		With(sendCommand, stopCommandAction[eventActorState]()).
		Build()
	runner := registerActorWorkflow(t, env, actor)
	var mu sync.Mutex
	var signals []string
	var payloads []any
	mockExternalSignalsWithCapture(env, func(name string, payload any) {
		mu.Lock()
		defer mu.Unlock()
		signals = append(signals, name)
		payloads = append(payloads, payload)
	})
	noteSignal := actors.TypeKeyOf(eventNoteCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(noteSignal, eventNoteCommand{Tag: "carol"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 2*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "event-send-1", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{eventSignal(t, "approve")}, signals)
	require.Equal(t, []any{approvalEvent{Approver: "carol", OK: true}}, payloads)
}

func TestDeliverEventSignalsWithoutStart(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	ref := actors.ARef("kind", "wf-1")
	require.NoError(t, inv.DeliverEvent(context.Background(), ref, "approve", approvalEvent{Approver: "alice"}))
	require.Len(t, fake.signalCalls, 1)
	require.Empty(t, fake.signalWithStartCalls, "events must not start workflows")
	require.Equal(t, "wf-1", fake.signalCalls[0].workflowID)
	require.Equal(t, "", fake.signalCalls[0].runID)
	require.Equal(t, actors.EventSignalPrefix+"approve", fake.signalCalls[0].signalName)
	require.Equal(t, approvalEvent{Approver: "alice"}, fake.signalCalls[0].arg)
}

func TestDeliverEventRetriesLatestRunWhenCachedRunIsStale(t *testing.T) {
	fake := &fakeTemporalClient{signalErr: &serviceerror.NotFound{}}
	inv := &temporalClientInvoker{client: fake}
	inv.storeRunID("wf-2", "stale-run")
	ref := actors.ARef("kind", "wf-2")
	err := inv.DeliverEvent(context.Background(), ref, "approve", nil)
	require.Error(t, err, "fake keeps failing, so the retry error surfaces")
	require.Len(t, fake.signalCalls, 2)
	require.Equal(t, "stale-run", fake.signalCalls[0].runID)
	require.Equal(t, "", fake.signalCalls[1].runID, "retry must target the latest run")
	require.Empty(t, fake.signalWithStartCalls)
	require.Equal(t, "", inv.cachedRunID("wf-2"), "stale run id must be evicted")
}

func TestDeliverEventRejectsInvalidName(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	require.Error(t, inv.DeliverEvent(context.Background(), actors.ARef("kind", "wf"), "", nil))
	require.Empty(t, fake.signalCalls)
}

// The selector fires the first ready case in registration order. With several
// commands buffered behind a long wait -- the normal state after WaitForEvent
// -- registration order decides which runs first, and if that order came from
// ranging over a map it would change on every replay. Two named commands
// arrive while A is parked; the order they run in must be the same on every
// run, and it must be the sorted one so a replay can predict it.
func TestQueuedCommandsDispatchInDeterministicOrder(t *testing.T) {
	first := actors.CommandNamed("aaa-first", func(ctx actors.Ctx, st *eventActorState, _ eventNoteCommand) (struct{}, error) {
		st.Log = append(st.Log, "cmd:aaa-first")
		return struct{}{}, nil
	})
	second := actors.CommandNamed("zzz-second", func(ctx actors.Ctx, st *eventActorState, _ eventNoteCommand) (struct{}, error) {
		st.Log = append(st.Log, "cmd:zzz-second")
		return struct{}{}, nil
	})
	waitSignal := actors.TypeKeyOf(eventWaitCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})

	run := func() []string {
		suite := testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()
		runner := registerActorWorkflow(t, env, newEventActor("event-order", nil, first, second))
		var after eventActorState
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(waitSignal, eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour})
		}, time.Millisecond)
		// Deliberately delivered in reverse of sorted order, so arrival order
		// and sorted order disagree and only one of them can win.
		env.RegisterDelayedCallback(func() { env.SignalWorkflow("zzz-second", eventNoteCommand{}) }, 2*time.Millisecond)
		env.RegisterDelayedCallback(func() { env.SignalWorkflow("aaa-first", eventNoteCommand{}) }, 3*time.Millisecond)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "bob", OK: true})
		}, 4*time.Millisecond)
		env.RegisterDelayedCallback(func() { after = queryEventState(t, env) }, 5*time.Millisecond)
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(stopSignal, stopLoopCommand{}) }, 6*time.Millisecond)
		env.ExecuteWorkflow(runner.Workflow(), "event-order-1", struct{}{})
		require.NoError(t, env.GetWorkflowError())
		return after.Log
	}

	want := []string{"wait:A:start", "wait:A:done", "cmd:aaa-first", "cmd:zzz-second"}
	// Map iteration order varies per process and per iteration; a dozen runs
	// is enough for an unsorted loop to show both orders.
	for i := 0; i < 12; i++ {
		require.Equal(t, want, run(), "run %d dispatched queued commands in a different order", i)
	}
}
