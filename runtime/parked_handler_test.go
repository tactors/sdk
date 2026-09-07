package runtime

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// parkedTrace records handler-side progress from inside the workflow
// coroutines. State written by a handler that never returns can still be
// captured here, so the trace distinguishes "handler completed" from "handler
// was abandoned" even when the workflow ends mid-handler.
type parkedTrace struct {
	mu     sync.Mutex
	events []string
}

func (p *parkedTrace) add(event string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
}

func (p *parkedTrace) list() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

// eventRotateCommand asks the handler itself to continue-as-new, which is the
// fourth rotation trigger alongside SnapshotEvery, Temporal's hint and a client
// continue request.
type eventRotateCommand struct {
	actors.CommandMsg[struct{}]
}

func parkedWaitAction(trace *parkedTrace) actors.CommandAction[eventActorState] {
	return actors.Command(func(ctx actors.Ctx, st *eventActorState, cmd eventWaitCommand) (struct{}, error) {
		st.Log = append(st.Log, "wait:"+cmd.Tag+":start")
		trace.add("wait:" + cmd.Tag + ":start")
		_, err := actors.WaitForEventAs[approvalEvent](ctx, cmd.Event, cmd.Timeout)
		switch {
		case err == nil:
			st.Log = append(st.Log, "wait:"+cmd.Tag+":done")
			trace.add("wait:" + cmd.Tag + ":done")
		case errors.Is(err, actors.ErrEventTimeout):
			st.Log = append(st.Log, "wait:"+cmd.Tag+":timeout")
			trace.add("wait:" + cmd.Tag + ":timeout")
		default:
			st.Log = append(st.Log, "wait:"+cmd.Tag+":error")
			trace.add("wait:" + cmd.Tag + ":error")
		}
		return struct{}{}, nil
	})
}

func parkedNoteAction(trace *parkedTrace) actors.CommandAction[eventActorState] {
	return actors.Command(func(ctx actors.Ctx, st *eventActorState, cmd eventNoteCommand) (struct{}, error) {
		st.Log = append(st.Log, "note:"+cmd.Tag)
		trace.add("note:" + cmd.Tag)
		return struct{}{}, nil
	})
}

// newRotatingEventActor builds an actor that rotates after every single
// command (SnapshotEvery=1) and whose wait command parks in WaitForEvent.
func newRotatingEventActor(kind string, trace *parkedTrace) actors.Actor {
	return actors.NewStateful(kind, func() eventActorState { return eventActorState{} }).
		WithSnapshot(actors.SnapshotConfig[eventActorState]{
			Every: 1,
			ContinueArgs: func(st eventActorState) (any, error) {
				return struct{}{}, nil
			},
		}).
		With(
			parkedWaitAction(trace),
			parkedNoteAction(trace),
			actors.Query(func(ctx actors.Ctx, st eventActorState, _ eventStateQuery) (eventActorState, error) {
				return st, nil
			}),
			stopCommandAction[eventActorState](),
		).
		Build()
}

// newExplicitRotateActor has no snapshot cadence: rotation comes only from a
// handler returning actors.ContinueAsNew.
func newExplicitRotateActor(kind string, trace *parkedTrace) actors.Actor {
	return actors.NewStateful(kind, func() eventActorState { return eventActorState{} }).
		With(
			parkedWaitAction(trace),
			actors.Command(func(ctx actors.Ctx, st *eventActorState, _ eventRotateCommand) (struct{}, error) {
				trace.add("rotate")
				return struct{}{}, actors.ContinueAsNew(ctx, struct{}{})
			}),
		).
		Build()
}

func requireContinuedAsNew(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "workflow should have ended by continue-as-new")
	var contErr *workflow.ContinueAsNewError
	require.True(t, errors.As(err, &contErr), "expected continue-as-new, got %v", err)
}

// A handler reached over the Tell request signal parks in WaitForEvent. A
// second command arrives on its own command channel and, with SnapshotEvery=1,
// trips the rotation. The rotation must not run while the parked handler is
// still in flight: it has to wait until the handler returns, so the handler is
// never abandoned and the snapshot never captures its half-applied state.
func TestSnapshotRotationWaitsForTellParkedHandler(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-tell", trace))
	mockExternalSignals(env)
	waitName := actors.TypeKeyOf(eventWaitCommand{})
	noteName := actors.TypeKeyOf(eventNoteCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: waitName,
			Payload: eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(noteName, eventNoteCommand{Tag: "B"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "alice", OK: true})
	}, 4*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-tell-1", struct{}{})
	requireContinuedAsNew(t, env.GetWorkflowError())
	require.Equal(t,
		[]string{"wait:A:start", "note:B", "wait:A:done"},
		trace.list(),
		"the parked handler must finish before the loop rotates")
}

// Same hazard on the Ask path, where losing the handler also loses the reply:
// the caller of Ask would wait for a reply that is never sent.
func TestSnapshotRotationWaitsForAskParkedHandler(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-ask", trace))
	var mu sync.Mutex
	var replies []askReply
	mockExternalSignalsWithCapture(env, func(signal string, payload any) {
		if signal != askReplySignal {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if reply, ok := payload.(askReply); ok {
			replies = append(replies, reply)
		}
	})
	waitName := actors.TypeKeyOf(eventWaitCommand{})
	noteName := actors.TypeKeyOf(eventNoteCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(askRequestSignal, askRequest{
			ID:            "ask-1",
			Command:       waitName,
			Payload:       eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour},
			ReplyWorkflow: "caller-wf",
			ReplySignal:   askReplySignal,
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(noteName, eventNoteCommand{Tag: "B"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "alice", OK: true})
	}, 4*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-ask-1", struct{}{})
	requireContinuedAsNew(t, env.GetWorkflowError())
	require.Equal(t,
		[]string{"wait:A:start", "note:B", "wait:A:done"},
		trace.list(),
		"the parked handler must finish before the loop rotates")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, replies, 1, "the Ask caller must still receive its reply")
	require.Equal(t, "ask-1", replies[0].ID)
	require.Empty(t, replies[0].Error)
}

// Same hazard on the Temporal Update path. Updates are the one case Temporal's
// own AllHandlersFinished covers, so this is the path the "suggested" branch
// already protected; it must hold for the SnapshotEvery rotation too.
func TestSnapshotRotationWaitsForUpdateParkedHandler(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-update", trace))
	mockExternalSignals(env)
	waitName := actors.TypeKeyOf(eventWaitCommand{})
	noteName := actors.TypeKeyOf(eventNoteCommand{})
	callback := newWaitUpdateCallbacks()

	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour})
		require.NoError(t, err)
		env.UpdateWorkflow(waitName, "update-parked", callback, payload)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(noteName, eventNoteCommand{Tag: "B"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "alice", OK: true})
	}, 4*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-update-1", struct{}{})
	requireContinuedAsNew(t, env.GetWorkflowError())
	require.Equal(t,
		[]string{"wait:A:start", "note:B", "wait:A:done"},
		trace.list(),
		"the parked update handler must finish before the loop rotates")
	_, err := callback.Wait(time.Second)
	require.NoError(t, err, "the update caller must still be completed")
}

// A client-issued continue request (actors.RequestContinueAsNew) is the third
// rotation trigger. It must defer for a parked handler exactly like the
// SnapshotEvery path, and it must still answer its caller afterwards.
func TestContinueRequestWaitsForParkedHandler(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	// SnapshotEvery is irrelevant here: the continue request drives rotation.
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-continue", trace))
	var mu sync.Mutex
	var replies []continueReply
	mockExternalSignalsWithCapture(env, func(signal string, payload any) {
		if signal != continueReplySignal {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if reply, ok := payload.(continueReply); ok {
			replies = append(replies, reply)
		}
	})
	waitName := actors.TypeKeyOf(eventWaitCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: waitName,
			Payload: eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueRequestSignal, continueRequest{
			ID:            "cont-1",
			ReplyWorkflow: "caller-wf",
			ReplySignal:   continueReplySignal,
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "alice", OK: true})
	}, 4*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-continue-1", struct{}{})
	requireContinuedAsNew(t, env.GetWorkflowError())
	require.Equal(t, []string{"wait:A:start", "wait:A:done"}, trace.list())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, replies, 1, "the continue caller must still receive its reply")
	require.Equal(t, "cont-1", replies[0].ID)
	require.Empty(t, replies[0].Error)
}

// The fourth rotation trigger: a handler that returns actors.ContinueAsNew
// itself. It abandons a parked handler on exactly the same terms, so it is
// queued like the others rather than ending the run on the spot.
func TestExplicitContinueAsNewWaitsForParkedHandler(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newExplicitRotateActor("parked-explicit", trace))
	mockExternalSignals(env)
	waitName := actors.TypeKeyOf(eventWaitCommand{})
	rotateName := actors.TypeKeyOf(eventRotateCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: waitName,
			Payload: eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(rotateName, eventRotateCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "alice", OK: true})
	}, 4*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-explicit-1", struct{}{})
	requireContinuedAsNew(t, env.GetWorkflowError())
	require.Equal(t,
		[]string{"wait:A:start", "rotate", "wait:A:done"},
		trace.list(),
		"a handler-issued continue-as-new must not abandon a parked handler either")
}

// While a rotation is deferred, a second continue request cannot be silently
// swallowed: one rotation can only answer one caller, so the second is refused
// out loud and its caller gets an error instead of a reply that never comes.
func TestSecondContinueRequestIsRefusedWhileRotationPending(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-continue-dup", trace))
	var mu sync.Mutex
	var replies []continueReply
	mockExternalSignalsWithCapture(env, func(signal string, payload any) {
		if signal != continueReplySignal {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if reply, ok := payload.(continueReply); ok {
			replies = append(replies, reply)
		}
	})
	waitName := actors.TypeKeyOf(eventWaitCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: waitName,
			Payload: eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueRequestSignal, continueRequest{
			ID: "cont-1", ReplyWorkflow: "caller-wf", ReplySignal: continueReplySignal,
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueRequestSignal, continueRequest{
			ID: "cont-2", ReplyWorkflow: "caller-wf", ReplySignal: continueReplySignal,
		})
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "E"), approvalEvent{Approver: "alice", OK: true})
	}, 4*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-continue-dup-1", struct{}{})
	requireContinuedAsNew(t, env.GetWorkflowError())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, replies, 2, "both callers must be answered")
	byID := map[string]continueReply{}
	for _, reply := range replies {
		byID[reply.ID] = reply
	}
	require.Contains(t, byID["cont-2"].Error, "already pending")
	require.Empty(t, byID["cont-1"].Error, "the first caller still gets its rotation")
}

// A rotation now outlives the turn that requested it, so stopping the actor
// while one is still deferred must not strand the caller that asked for it.
func TestStopWithPendingContinueRequestAnswersTheCaller(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-continue-stop", trace))
	var mu sync.Mutex
	var replies []continueReply
	mockExternalSignalsWithCapture(env, func(signal string, payload any) {
		if signal != continueReplySignal {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if reply, ok := payload.(continueReply); ok {
			replies = append(replies, reply)
		}
	})
	waitName := actors.TypeKeyOf(eventWaitCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: waitName,
			Payload: eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueRequestSignal, continueRequest{
			ID: "cont-1", ReplyWorkflow: "caller-wf", ReplySignal: continueReplySignal,
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 3*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-continue-stop-1", struct{}{})
	require.NoError(t, env.GetWorkflowError(), "ErrStopLoop ends the loop cleanly")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, replies, 1)
	require.Equal(t, "cont-1", replies[0].ID)
	require.Contains(t, replies[0].Error, "stopped before the pending continue-as-new ran")
}

// A parked handler that never resumes must not be able to drop the rotation on
// the floor either: with a finite WaitForEvent timeout the wait unblocks, the
// handler returns, and only then does the deferred rotation run.
func TestDeferredRotationRunsAfterWaitTimeout(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-timeout", trace))
	mockExternalSignals(env)
	waitName := actors.TypeKeyOf(eventWaitCommand{})
	noteName := actors.TypeKeyOf(eventNoteCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: waitName,
			Payload: eventWaitCommand{Tag: "A", Event: "never", Timeout: 50 * time.Millisecond},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(noteName, eventNoteCommand{Tag: "B"})
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-timeout-1", struct{}{})
	requireContinuedAsNew(t, env.GetWorkflowError())
	require.Equal(t,
		[]string{"wait:A:start", "note:B", "wait:A:timeout"},
		trace.list(),
		"rotation must be deferred until the wait times out and the handler returns")
}

// Documents the path the rotation guard deliberately does not cover: workflow
// cancellation. There is no snapshot and no next run, so there is nothing to
// abandon a handler into -- and because WaitForEvent also selects on
// ctx.Done(), the parked handler is scheduled and observes the cancellation
// before the run ends rather than vanishing mid-wait.
func TestCancellationUnblocksParkedHandler(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	trace := &parkedTrace{}
	runner := registerActorWorkflow(t, env, newRotatingEventActor("parked-cancel", trace))
	mockExternalSignals(env)
	waitName := actors.TypeKeyOf(eventWaitCommand{})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: waitName,
			Payload: eventWaitCommand{Tag: "A", Event: "E", Timeout: time.Hour},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "parked-cancel-1", struct{}{})
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Equal(t, []string{"wait:A:start", "wait:A:error"}, trace.list(),
		"the parked handler must see the cancellation, not disappear inside the wait")
}
