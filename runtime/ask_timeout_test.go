package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// The ask-deadline fixture models the shape the roadmap gap describes: an ask
// whose target never answers because the target's own handler is parked.
//
//	askDeadlineCommand -- asks askParkCommand with no per-call deadline
//	askTimedCommand    -- asks askParkCommand with a per-call deadline
//	askEchoingCommand  -- asks askEchoCommand (answers at once) with a deadline
//	askRemoteCommand   -- asks an actor id that is not running at all
//	askParkCommand     -- parks in WaitForEvent("release") and only then replies
//	askEchoCommand     -- answers immediately
type askDeadlineCommand struct {
	actors.CommandMsg[struct{}]
}

type askTimedCommand struct {
	actors.CommandMsg[struct{}]
	Timeout time.Duration
}

type askEchoingCommand struct {
	actors.CommandMsg[struct{}]
	Timeout time.Duration
}

type askRemoteCommand struct {
	actors.CommandMsg[struct{}]
	Target  string
	Timeout time.Duration
}

type askCrossNSCommand struct {
	actors.CommandMsg[struct{}]
	Namespace string
	Timeout   time.Duration
}

type askParkCommand struct {
	actors.CommandMsg[string]
}

type askEchoCommand struct {
	actors.CommandMsg[string]
	Message string
}

type askDeadlineQuery struct {
	actors.QueryMsg[askDeadlineState]
}

// askPendingQuery reports how many ask waiters the context still holds. It
// reaches into the runtime context on purpose: the waiter map is what makes a
// late reply addressable, and "did the timed-out ask clean its entry up" is
// otherwise invisible from outside.
type askPendingQuery struct {
	actors.QueryMsg[int]
}

type askDeadlineState struct {
	// Log is append-only across both the calling handler and the target
	// handler, so it records the interleaving as well as the outcomes.
	Log []string
	// Seen collects the ask correlation ids the *target* handlers observed,
	// which is the waiter id the reply will be addressed to.
	Seen []string
}

func newAskDeadlineActor(kind string) actors.Actor {
	record := func(st *askDeadlineState, tag, value string, err error) {
		switch {
		case err == nil:
			st.Log = append(st.Log, tag+":ok:"+value)
		case errors.Is(err, actors.ErrAskTimeout):
			st.Log = append(st.Log, tag+":timeout")
		default:
			st.Log = append(st.Log, tag+":error:"+err.Error())
		}
	}
	return actors.NewStateful(kind, func() askDeadlineState { return askDeadlineState{} }).
		With(
			// No per-call deadline: bounded only by the process default,
			// exactly as before this change.
			actors.Command(func(ctx actors.Ctx, st *askDeadlineState, _ askDeadlineCommand) (struct{}, error) {
				value, err := actors.Ask[askParkCommand, string](ctx, ctx.Self(), askParkCommand{})
				record(st, "plain", value, err)
				return struct{}{}, nil
			}),
			// Per-call deadline against a target that will not answer in time.
			actors.Command(func(ctx actors.Ctx, st *askDeadlineState, cmd askTimedCommand) (struct{}, error) {
				value, err := actors.AskWithTimeout[askParkCommand, string](ctx, ctx.Self(), askParkCommand{}, cmd.Timeout)
				record(st, "timed", value, err)
				return struct{}{}, nil
			}),
			// Per-call deadline against a target that answers immediately.
			actors.Command(func(ctx actors.Ctx, st *askDeadlineState, cmd askEchoingCommand) (struct{}, error) {
				value, err := actors.AskWithTimeout[askEchoCommand, string](ctx, ctx.Self(), askEchoCommand{Message: "pong"}, cmd.Timeout)
				record(st, "echoed", value, err)
				return struct{}{}, nil
			}),
			// Per-call deadline against an actor that was never started.
			actors.Command(func(ctx actors.Ctx, st *askDeadlineState, cmd askRemoteCommand) (struct{}, error) {
				target := actors.Ref{Kind: ctx.Self().Kind, ID: cmd.Target}
				value, err := actors.AskWithTimeout[askEchoCommand, string](ctx, target, askEchoCommand{Message: "pong"}, cmd.Timeout)
				record(st, "remote", value, err)
				return struct{}{}, nil
			}),
			// Per-call deadline against an actor in another namespace, which
			// routes through the bridge activity rather than a reply signal.
			actors.Command(func(ctx actors.Ctx, st *askDeadlineState, cmd askCrossNSCommand) (struct{}, error) {
				target := actors.Ref{Kind: ctx.Self().Kind, ID: "remote-actor", Namespace: cmd.Namespace}
				value, err := actors.AskWithTimeout[askEchoCommand, string](ctx, target, askEchoCommand{Message: "pong"}, cmd.Timeout)
				record(st, "crossns", value, err)
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *askDeadlineState, _ askParkCommand) (string, error) {
				st.Seen = append(st.Seen, ctx.MessageMetadata().CorrelationID)
				if _, err := ctx.WaitForEvent("release", 0); err != nil {
					return "", err
				}
				// Reached only after the release event; by then the caller's
				// deadline may long since have expired, which is exactly the
				// late-reply case.
				st.Log = append(st.Log, "park:done")
				return "LATE", nil
			}),
			actors.Command(func(ctx actors.Ctx, st *askDeadlineState, cmd askEchoCommand) (string, error) {
				st.Seen = append(st.Seen, ctx.MessageMetadata().CorrelationID)
				return "echo:" + cmd.Message, nil
			}),
			actors.Query(func(ctx actors.Ctx, st askDeadlineState, _ askDeadlineQuery) (askDeadlineState, error) {
				return st, nil
			}),
			actors.Query(func(ctx actors.Ctx, st askDeadlineState, _ askPendingQuery) (int, error) {
				c, ok := ctx.(*wfContext)
				if !ok {
					return -1, nil
				}
				return len(c.askWaiters), nil
			}),
			stopCommandAction[askDeadlineState](),
		).
		Build()
}

func askDeadlineSnapshot(t *testing.T, env *testsuite.TestWorkflowEnvironment) askDeadlineState {
	t.Helper()
	payload, err := codec.Marshal(askDeadlineQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(actors.TypeKeyOf(askDeadlineQuery{}), payload)
	require.NoError(t, err)
	var st askDeadlineState
	require.NoError(t, value.Get(&st))
	return st
}

func askPendingWaiters(t *testing.T, env *testsuite.TestWorkflowEnvironment) int {
	t.Helper()
	payload, err := codec.Marshal(askPendingQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(actors.TypeKeyOf(askPendingQuery{}), payload)
	require.NoError(t, err)
	var pending int
	require.NoError(t, value.Get(&pending))
	return pending
}

// swallowExternalSignals accepts every outgoing signal and delivers it
// nowhere, so an ask to that target is never answered. mockExternalSignals, by
// contrast, loops the signal back into the running workflow.
func swallowExternalSignals(env *testsuite.TestWorkflowEnvironment, onAsk func(askRequest)) {
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			if req, ok := args.Get(4).(askRequest); ok && onAsk != nil {
				onAsk(req)
			}
		}).Return(nil).Maybe()
}

// disableDefaultAskTimeout removes the process-wide fallback so a test only
// exercises the per-call deadline (or its absence).
func disableDefaultAskTimeout(t *testing.T) {
	t.Helper()
	old := askTimeout()
	SetDefaultAskTimeout(0)
	t.Cleanup(func() { SetDefaultAskTimeout(old) })
}

// This is the reproduction, kept as a regression guard for "AskActor must keep
// working unchanged". With the process-wide default disabled (a documented,
// supported configuration) a plain Ask whose target never answers never
// returns: the handler is still parked when the workflow's own execution
// timeout finally kills it, so nothing is ever recorded and the stop command
// queued behind it never runs.
//
// Before this change that was the only behaviour available. AskWithTimeout
// below is what closes it.
func TestAskWithoutDeadlineWaitsForever(t *testing.T) {
	disableDefaultAskTimeout(t)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-unbounded"))
	mockExternalSignals(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askDeadlineCommand{}), askDeadlineCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-unbounded-1", struct{}{})

	require.Empty(t, askDeadlineSnapshot(t, env).Log, "the unbounded ask must still be waiting")
	require.Error(t, env.GetWorkflowError(), "only the execution timeout ends this workflow")
}

// The fix: the same wedged target, but the ask carries its own deadline.
func TestAskWithTimeoutReturnsErrAskTimeout(t *testing.T) {
	disableDefaultAskTimeout(t)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-bounded"))
	mockExternalSignals(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askTimedCommand{}), askTimedCommand{Timeout: 10 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-bounded-1", struct{}{})

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"timed:timeout"}, askDeadlineSnapshot(t, env).Log)
	// The expired ask must not leave its waiter behind: the entry is what a
	// late reply would be delivered through.
	require.Equal(t, 0, askPendingWaiters(t, env), "a timed-out ask must drop its waiter")
}

// cancelDuringAsk drives one ask at a target that will never answer and cancels
// the workflow while the ask is still waiting, then returns what the handler
// recorded.
//
// Two details make this test able to fail. The target is unreachable rather
// than merely slow, so nothing but the caller's own select can end the wait --
// a self-ask would be released by the target's cancellation reply instead. And
// the assertion is on the *handler's* observation, not the workflow error,
// because the command loop reports a cancellation either way.
func cancelDuringAsk(t *testing.T, kind, id string, timeout time.Duration) []string {
	t.Helper()
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor(kind))
	swallowExternalSignals(env, nil)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askRemoteCommand{}), askRemoteCommand{Target: "nobody-home", Timeout: timeout})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 10*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), id, struct{}{})
	require.Error(t, env.GetWorkflowError())
	return askDeadlineSnapshot(t, env).Log
}

// An ask with no deadline still has to honour workflow cancellation -- without
// that it would be genuinely unkillable, which is worse than the hang this
// change set out to fix. With no timer running, only the ctx.Done() branch of
// the select can end this wait.
func TestAskWithoutDeadlineUnblocksOnCancellation(t *testing.T) {
	disableDefaultAskTimeout(t)

	log := cancelDuringAsk(t, "ask-cancel-unbounded", "ask-cancel-unbounded-1", 0)
	require.Len(t, log, 1, "the unbounded ask must unblock on cancellation")
	require.Contains(t, log[0], "canceled", "expected a cancellation, got %q", log[0])
}

// A cancelled ask that *does* have a deadline must report the cancellation,
// not invent a timeout: the deadline timer is cancelled along with its parent
// context, so its future resolves with an error rather than by firing.
func TestAskWithDeadlineReportsCancellationNotTimeout(t *testing.T) {
	disableDefaultAskTimeout(t)

	log := cancelDuringAsk(t, "ask-cancel-bounded", "ask-cancel-bounded-1", time.Hour)
	require.Len(t, log, 1)
	require.NotEqual(t, "remote:timeout", log[0], "a cancelled ask must not be reported as a timeout")
	require.Contains(t, log[0], "canceled", "expected a cancellation, got %q", log[0])
}

// The target is not merely slow, it was never started: the request signal is
// accepted by the server and no handler ever exists to answer it.
func TestAskWithTimeoutBoundsUnreachableTarget(t *testing.T) {
	disableDefaultAskTimeout(t)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-remote"))
	// The request signal is accepted and then goes nowhere: nothing answers it.
	swallowExternalSignals(env, nil)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askRemoteCommand{}), askRemoteCommand{Target: "nobody-home", Timeout: 10 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-remote-1", struct{}{})

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"remote:timeout"}, askDeadlineSnapshot(t, env).Log)
}

// A reply that beats the deadline is returned normally and the timer is torn
// down: the deadline must not turn a healthy ask into a failure.
func TestAskWithTimeoutReturnsReplyBeforeDeadline(t *testing.T) {
	disableDefaultAskTimeout(t)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-fast"))
	mockExternalSignals(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askEchoingCommand{}), askEchoingCommand{Timeout: time.Hour})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-fast-1", struct{}{})

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"echoed:ok:echo:pong"}, askDeadlineSnapshot(t, env).Log)
}

// timeout <= 0 means "no deadline", matching WaitForEvent -- it does not fall
// back to the process default and it does not mean "expire immediately".
func TestAskWithTimeoutZeroMeansNoDeadline(t *testing.T) {
	// Leave a *short* process default in place: if zero fell back to it, the
	// ask would time out and the log would not be empty.
	old := askTimeout()
	SetDefaultAskTimeout(5 * time.Millisecond)
	t.Cleanup(func() { SetDefaultAskTimeout(old) })

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-zero"))
	mockExternalSignals(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askTimedCommand{}), askTimedCommand{Timeout: 0})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-zero-1", struct{}{})

	require.Empty(t, askDeadlineSnapshot(t, env).Log, "timeout 0 must not adopt the process default")
}

// A per-call deadline longer than the process default wins: the caller's
// number is the deadline, not a value clamped by a global.
func TestAskWithTimeoutOverridesShorterDefault(t *testing.T) {
	old := askTimeout()
	SetDefaultAskTimeout(2 * time.Millisecond)
	t.Cleanup(func() { SetDefaultAskTimeout(old) })

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-longer"))
	mockExternalSignals(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askTimedCommand{}), askTimedCommand{Timeout: time.Hour})
	}, time.Millisecond)
	// The target only answers at 30ms, long after the 2ms default would have
	// fired but well inside the 1h per-call deadline.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "release"), approvalEvent{Approver: "alice", OK: true})
	}, 30*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 60*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-longer-1", struct{}{})

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"park:done", "timed:ok:LATE"}, askDeadlineSnapshot(t, env).Log)
}

// Constraint 3: a reply that arrives after its ask timed out is dropped. It
// must not sit in a channel waiting to be mistaken for the answer to the next
// ask.
//
// Sequence: ask #1 (10ms deadline) targets the parked handler and times out.
// The "release" event then lets that handler finish, so the reply for ask #1
// is signalled *after* its waiter is gone ("park:done" in the log is the proof
// that the late reply really was produced). Ask #2 then runs against the echo
// command and must see "echo:pong" -- never "LATE".
func TestLateAskReplyDoesNotCorruptNextAsk(t *testing.T) {
	disableDefaultAskTimeout(t)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-late"))
	mockExternalSignals(env)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askTimedCommand{}), askTimedCommand{Timeout: 10 * time.Millisecond})
	}, time.Millisecond)
	// Releases the parked target *after* ask #1 has given up: this is what
	// produces the late reply.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(eventSignal(t, "release"), approvalEvent{Approver: "alice", OK: true})
	}, 30*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askEchoingCommand{}), askEchoingCommand{Timeout: time.Hour})
	}, 45*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 70*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-late-1", struct{}{})

	require.NoError(t, env.GetWorkflowError())
	st := askDeadlineSnapshot(t, env)
	require.Equal(t, []string{"timed:timeout", "park:done", "echoed:ok:echo:pong"}, st.Log,
		"the second ask must see its own answer, not the late reply to the first")
	// The two asks used distinct waiter ids -- which is why the late reply,
	// addressed to the first, could not be handed to the second.
	require.Equal(t, []string{"ask-late-1-ask-1", "ask-late-1-ask-2"}, st.Seen)
}

// Ask replies are pinned to the run that issued the request. Waiter ids
// ("<actorID>-ask-<n>") restart at 1 after a continue-as-new, so an unpinned
// reply from a previous run would name a live waiter in the next one and be
// delivered to it.
func TestAskRequestPinsReplyToIssuingRun(t *testing.T) {
	disableDefaultAskTimeout(t)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskDeadlineActor("ask-runpin"))

	// A target that is not the running workflow goes through the external
	// signal mock, which is where the request can be inspected.
	var requests []askRequest
	swallowExternalSignals(env, func(req askRequest) { requests = append(requests, req) })

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askRemoteCommand{}), askRemoteCommand{Target: "nobody-home", Timeout: 10 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-runpin-1", struct{}{})

	require.NoError(t, env.GetWorkflowError())
	require.Len(t, requests, 1)
	require.Equal(t, "ask-runpin-1-ask-1", requests[0].ID)
	require.NotEmpty(t, requests[0].ReplyRunID,
		"ask requests must name the run that must receive the reply, or a stale reply can cross a continue-as-new")
}

// The cross-namespace route has no reply signal to wait on: the ask runs as an
// activity, and the deadline has to become the activity's own timeouts. This
// checks both halves -- the timeouts are actually applied, and a timeout that
// comes back is reported as ErrAskTimeout like the signal route's.
func TestAskWithTimeoutBoundsBridgeActivity(t *testing.T) {
	disableDefaultAskTimeout(t)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(dataConverter())
	runner := NewRunner(newAskDeadlineActor("ask-bridge"))
	routing := &namespaceRouting{
		pool: StaticClientPool{Default: "caller-ns"},
		policy: CrossNamespacePolicy{
			Enabled:   true,
			Allowlist: map[string]map[string]struct{}{"caller-ns": {"target-ns": {}}},
		},
	}
	env.RegisterWorkflowWithOptions(runner.WorkflowWithRouting(routing), workflow.RegisterOptions{Name: runner.Description().Kind})

	// ActivityInfo exposes the deadline rather than the configured timeouts,
	// so the window is what the assertion can see.
	var sawWindow time.Duration
	env.RegisterActivityWithOptions(func(ctx context.Context, req bridgeAskRequest) (string, error) {
		info := activity.GetInfo(ctx)
		sawWindow = info.Deadline.Sub(info.StartedTime)
		return "", temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)
	}, activity.RegisterOptions{Name: bridgeInvokeAskActivity})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(askCrossNSCommand{}), askCrossNSCommand{Namespace: "target-ns", Timeout: 10 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.WorkflowWithRouting(routing), "ask-bridge-1", struct{}{})

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 10*time.Millisecond, sawWindow, "the deadline must bound the bridge activity")
	require.Equal(t, []string{"crossns:timeout"}, askDeadlineSnapshot(t, env).Log)
}
