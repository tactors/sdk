package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"github.com/tactors/sdk/observability"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internalbindings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestTemporalCommandRetryAndStop(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newRetryActor())
	failSignal := actors.TypeKeyOf(retryCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(retryStateQuery{})
	var attempts int
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(failSignal, retryCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(retryStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		var state retryState
		if err := value.Get(&state); err != nil {
			t.Fatalf("decode query: %v", err)
		}
		attempts = state.Attempts
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 3*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "retry-wf", struct{}{})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestTemporalCommandTimeoutAndValidator(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newCommandControlActor())
	timeoutSignal := actors.TypeKeyOf(timeoutCommand{})
	validatorSignal := actors.TypeKeyOf(validatorCommand{})
	queryName := actors.TypeKeyOf(commandControlQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var snapshot commandControlState
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(timeoutSignal, timeoutCommand{Delay: 200 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(validatorSignal, validatorCommand{Input: ""})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(validatorSignal, validatorCommand{Input: "ok"})
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(commandControlQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&snapshot); err != nil {
			t.Fatalf("decode query: %v", err)
		}
	}, 500*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 600*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "command-control", struct{}{})
	require.Equal(t, 2, snapshot.TimeoutAttempts, "timeout command should retry after timeout")
	require.Contains(t, snapshot.Completed, "timeout")
	require.Contains(t, snapshot.Completed, "validator")
	require.Equal(t, 1, snapshot.ValidatorAccepted, "validator should reject empty payload")
	require.Equal(t, "ok", snapshot.LastValidatorInput)
	require.NotEmpty(t, snapshot.Errors, "timeout attempt should record error")
}

func TestTemporalQueryDecoding(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newGreetingActor())
	queryName := actors.TypeKeyOf(greetingQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var greeting string
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(greetingQuery{Name: "Neo"})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&greeting); err != nil {
			t.Fatalf("decode greeting: %v", err)
		}
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 2*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "greet-wf", struct{}{})
	if greeting != "hello Neo" {
		t.Fatalf("unexpected greeting: %s", greeting)
	}
}

func TestTemporalAskProtocol(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskActor())
	mockExternalSignals(env)
	proxySignal := actors.TypeKeyOf(proxyCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(askStateQuery{})
	var last string
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(proxySignal, proxyCommand{Message: "ping"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(askStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&last); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 40*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-wf", struct{}{})
	if last != "echo: ping" {
		t.Fatalf("unexpected ask result: %s", last)
	}
}

func TestTemporalAskBusinessError(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskActor())
	mockExternalSignals(env)
	rejectSignal := actors.TypeKeyOf(rejectAskCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(askStateQuery{})
	var last string
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(rejectSignal, rejectAskCommand{Message: "denied"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(askStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&last); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 40*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-err", struct{}{})
	if last != "reject denied" {
		t.Fatalf("expected business error captured, got %s", last)
	}
}

func TestTellMissingActorKind(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newTellParentActor())
	missingSignal := actors.TypeKeyOf(tellMissingCommand{})
	queryName := actors.TypeKeyOf(tellStateQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(missingSignal, tellMissingCommand{})
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "tell-missing-kind", struct{}{})
	payload, err := codec.Marshal(tellStateQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state tellState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if !strings.Contains(state.Errors["missing"], `actor kind "missing-kind" not registered`) {
		t.Fatalf("unexpected missing kind error: %s", state.Errors["missing"])
	}
}

func TestTellMissingWorkflowInstance(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	registerActorWorkflow(t, env, newTellChildActor())
	runner := registerActorWorkflow(t, env, newTellParentActor())
	env.OnSignalExternalWorkflow(mock.Anything, "tell-child-missing", "", tellRequestSignal, mock.Anything).
		Return(fmt.Errorf("workflow not found")).Once()
	orphanSignal := actors.TypeKeyOf(tellOrphanCommand{})
	queryName := actors.TypeKeyOf(tellStateQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(orphanSignal, tellOrphanCommand{TargetID: "tell-child-missing"})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "tell-missing-instance", struct{}{})
	payload, err := codec.Marshal(tellStateQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state tellState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if !strings.Contains(state.Errors["orphan"], "workflow not found") {
		t.Fatalf("unexpected orphan error: %s", state.Errors["orphan"])
	}
}

func TestSpawnOneShotRequiresChildKind(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newSpawnTesterActor())
	missingSignal := actors.TypeKeyOf(spawnMissingKindCommand{})
	queryName := actors.TypeKeyOf(spawnTesterQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(missingSignal, spawnMissingKindCommand{})
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "spawn-missing-kind", struct{}{})
	payload, err := codec.Marshal(spawnTesterQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state spawnTesterState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if !strings.Contains(state.Errors["missingKind"], "WithChildKind is required") {
		t.Fatalf("expected missing kind error, got %s", state.Errors["missingKind"])
	}
}

func TestSpawnChildConflictingIDs(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	registerActorWorkflow(t, env, newSpawnTestChildActor())
	runner := registerActorWorkflow(t, env, newSpawnTesterActor())
	conflictSignal := actors.TypeKeyOf(spawnConflictCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(spawnTesterQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(conflictSignal, spawnConflictCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(conflictSignal, spawnConflictCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(spawnStopChildCommand{}), spawnStopChildCommand{})
	}, 4*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 20*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "spawn-conflict", struct{}{})
	payload, err := codec.Marshal(spawnTesterQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state spawnTesterState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if !strings.Contains(state.Errors["conflict"], "already") {
		t.Fatalf("expected conflict error, got %s", state.Errors["conflict"])
	}
}

func TestSpawnOneShotChildCrash(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	registerActorWorkflow(t, env, newSpawnCrashChildActor())
	runner := registerActorWorkflow(t, env, newSpawnTesterActor())
	crashSignal := actors.TypeKeyOf(spawnCrashCommand{})
	queryName := actors.TypeKeyOf(spawnTesterQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(crashSignal, spawnCrashCommand{})
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "spawn-crash", struct{}{})
	payload, err := codec.Marshal(spawnTesterQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state spawnTesterState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if !strings.Contains(state.Errors["crash"], "child crashed") {
		t.Fatalf("expected crash error, got %s", state.Errors["crash"])
	}
}

func TestTellWorkflowNotReady(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	registerActorWorkflow(t, env, newTellChildActor())
	runner := registerActorWorkflow(t, env, newTellParentActor())
	env.OnSignalExternalWorkflow(mock.Anything, "tell-child-rotating", "", tellRequestSignal, mock.Anything).
		Return(serviceerror.NewWorkflowNotReady("workflow not ready")).Once()
	rotationSignal := actors.TypeKeyOf(tellRotationCommand{})
	queryName := actors.TypeKeyOf(tellStateQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(rotationSignal, tellRotationCommand{TargetID: "tell-child-rotating"})
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "tell-rotation", struct{}{})
	payload, err := codec.Marshal(tellStateQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state tellState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if !strings.Contains(state.Errors["rotation"], "workflow not ready") {
		t.Fatalf("unexpected rotation error: %s", state.Errors["rotation"])
	}
}

func TestTellQueueError(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	registerActorWorkflow(t, env, newTellChildActor())
	runner := registerActorWorkflow(t, env, newTellParentActor())
	env.OnSignalExternalWorkflow(mock.Anything, "tell-child-shared", "", tellRequestSignal, mock.Anything).
		Return(fmt.Errorf("queue overloaded")).Once()
	queueSignal := actors.TypeKeyOf(tellQueueCommand{})
	queryName := actors.TypeKeyOf(tellStateQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(queueSignal, tellQueueCommand{TargetID: "tell-child-shared"})
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "tell-queue-error", struct{}{})
	payload, err := codec.Marshal(tellStateQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state tellState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if !strings.Contains(state.Errors["queue"], "queue overloaded") {
		t.Fatalf("unexpected queue error: %s", state.Errors["queue"])
	}
}

func TestBusinessErrorDoesNotFailWorkflow(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newBusinessErrorActor())
	signal := actors.TypeKeyOf(businessErrorCommand{})
	queryName := actors.TypeKeyOf(businessStateQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signal, businessErrorCommand{Message: "validation failed"})
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "business-error", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	payload, err := codec.Marshal(businessStateQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state businessState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if len(state.Errors) != 1 || !strings.Contains(state.Errors[0], "validation failed") {
		t.Fatalf("business error not recorded: %#v", state.Errors)
	}
}

func TestUnexpectedErrorFailsWorkflow(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newBusinessErrorActor())
	signal := actors.TypeKeyOf(unexpectedErrorCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signal, unexpectedErrorCommand{Message: "boom"})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "unexpected-error", struct{}{})
	err := env.GetWorkflowError()
	require.Error(t, err, "workflow should fail on unexpected command error")
	require.ErrorContains(t, err, "unexpected: boom")
}

func TestTemporalEffectOutbox(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	var calls atomic.Int32
	runner := registerActorWorkflow(t, env, newEffectActor(&calls, false))
	effectSignal := actors.TypeKeyOf(effectCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(effectStateQuery{})
	var state effectState
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(effectSignal, effectCommand{Key: "alpha"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(effectSignal, effectCommand{Key: "alpha"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(effectStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&state); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 40*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "effect-wf", struct{}{})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected effect activity once, got %d", calls.Load())
	}
	if state.Last != "alpha-1" {
		t.Fatalf("unexpected effect state: %+v", state)
	}
}

func TestActivityExecutionEdgeCases(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	logger := &captureTemporalLogger{}
	suite.SetLogger(logger)
	env := suite.NewTestWorkflowEnvironment()
	var deadlineCaptured bool
	env.SetOnActivityStartedListener(func(info *activity.Info, ctx context.Context, _ converter.EncodedValues) {
		if info != nil && info.ActivityType.Name == "deadline-check" {
			_, deadlineCaptured = ctx.Deadline()
		}
	})
	runner := registerActorWorkflow(t, env, newActivityEdgeActor())
	deadlineSignal := actors.TypeKeyOf(activityDeadlineCommand{})
	decodeSignal := actors.TypeKeyOf(activityDecodeCommand{})
	bgSignal := actors.TypeKeyOf(activityBackgroundCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(activityEdgeQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(deadlineSignal, activityDeadlineCommand{StartToClose: 5 * time.Millisecond, Work: 50 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(decodeSignal, activityDecodeCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(bgSignal, activityBackgroundCommand{Wait: 5 * time.Millisecond})
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 20*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "activity-edge", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	payload, err := codec.Marshal(activityEdgeQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var state activityEdgeState
	require.NoError(t, value.Get(&state))
	require.True(t, deadlineCaptured, "activity context should expose deadlines")
	require.NotEmpty(t, state.Errors, "expected decode error to be recorded")
	require.Contains(t, strings.Join(state.Errors, "|"), "decode response")
	require.True(t, state.BackgroundLogged, "background command should toggle flag")
	require.True(t, logger.hasBackgroundFailure(), "expected background activity failure log entry")
}

func TestTemporalErrorSemantics(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newErrorActor())
	mockExternalSignals(env)
	nonRetrySignal := actors.TypeKeyOf(nonRetryableCommand{})
	retrySignal := actors.TypeKeyOf(retryAfterCommand{})
	queryName := actors.TypeKeyOf(errorStateQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var state errorState
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(nonRetrySignal, nonRetryableCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(retrySignal, retryAfterCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(errorStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&state); err != nil {
			t.Fatalf("decode state: %v", err)
		}
	}, 7*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 8*time.Second)
	env.ExecuteWorkflow(runner.Workflow(), "error-sem", struct{}{})
	if state.NonRetryAttempts != 1 {
		t.Fatalf("expected non-retry attempts 1, got %d", state.NonRetryAttempts)
	}
	if state.RetryAttempts != 2 {
		t.Fatalf("expected retry attempts 2, got %d", state.RetryAttempts)
	}
	if len(state.RetryDeltas) < 2 {
		t.Fatalf("expected two retry timestamps, got %v", state.RetryDeltas)
	}
	diff := state.RetryDeltas[1] - state.RetryDeltas[0]
	if diff < 5*time.Second {
		t.Fatalf("expected retry delay >= 5s, got %s", diff)
	}
}

func TestEffectTTLExpiresAndReexecutes(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	var calls atomic.Int32
	runner := registerActorWorkflow(t, env, newEffectEdgeActor(&calls))
	ttlSignal := actors.TypeKeyOf(effectTTLCommand{})
	sleepSignal := actors.TypeKeyOf(effectSleepCommand{})
	queryName := actors.TypeKeyOf(effectEdgeQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ttlSignal, effectTTLCommand{Key: "alpha", TTL: 5 * time.Millisecond})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(sleepSignal, effectSleepCommand{Delay: 20 * time.Millisecond})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ttlSignal, effectTTLCommand{Key: "alpha", TTL: 5 * time.Millisecond})
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 40*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "effect-ttl", effectEdgeInit{})
	require.NoError(t, env.GetWorkflowError())
	payload, err := codec.Marshal(effectEdgeQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var state effectEdgeState
	require.NoError(t, value.Get(&state))
	require.Len(t, state.History, 2)
	require.Equal(t, []string{"alpha-1", "alpha-2"}, state.History)
	require.Equal(t, int32(2), calls.Load(), "effect should re-execute after TTL expiry")
}

func TestEffectMemoPersistsAcrossContinueAsNew(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	var calls atomic.Int32
	runner := registerActorWorkflow(t, env, newEffectEdgeActor(&calls))
	mockExternalSignals(env)
	ttlSignal := actors.TypeKeyOf(effectTTLCommand{})
	continueSignal := actors.TypeKeyOf(effectContinueCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(effectEdgeQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ttlSignal, effectTTLCommand{Key: "persist"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueSignal, effectContinueCommand{Key: "persist"})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ttlSignal, effectTTLCommand{Key: "persist"})
	}, 5*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "effect-continue", effectEdgeInit{})
	if err := env.GetWorkflowError(); err != nil {
		if wfErr, ok := err.(interface{ Unwrap() error }); ok {
			if unwrapped := wfErr.Unwrap(); unwrapped != nil {
				err = unwrapped
			}
		}
		if !workflow.IsContinueAsNewError(err) {
			t.Fatalf("workflow error: %v", err)
		}
	}
	payload, err := codec.Marshal(effectEdgeQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var state effectEdgeState
	require.NoError(t, value.Get(&state))
	require.Len(t, state.History, 2)
	require.Equal(t, []string{"persist-1", "persist-1"}, state.History)
	require.Equal(t, int32(1), calls.Load(), "effect should reuse memo across Continue-As-New")
}
func TestTemporalAskValidator(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskActor())
	mockExternalSignals(env)
	validateSignal := actors.TypeKeyOf(validateAskCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(askStateQuery{})
	var last string
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(validateSignal, validateAskCommand{Message: ""})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(askStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&last); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}, 10*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 40*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-validator", struct{}{})
	if last != "validator: message required" {
		t.Fatalf("expected validator error recorded, got %s", last)
	}
}

func TestTemporalAskCalleeFailure(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskActor())
	mockExternalSignals(env)
	errorSignal := actors.TypeKeyOf(errorAskCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(askStateQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(errorSignal, errorAskCommand{Message: "fail"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 2*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-failure", struct{}{})
	payload, err := codec.Marshal(askStateQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var last string
	if err := value.Get(&last); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(last, "callee failed") {
		t.Fatalf("expected callee failure recorded, got %s", last)
	}
}

func TestTemporalAskCallerCancellation(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskActor())
	mockExternalSignals(env)
	slowSignal := actors.TypeKeyOf(slowAskCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(slowSignal, slowAskCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 5*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-cancel", struct{}{})
	err := env.GetWorkflowError()
	require.Error(t, err, "expected cancellation error")
	require.Contains(t, err.Error(), "canceled", "expected cancellation, got %v", err)
}

func TestTemporalWorkflowCancellationStopsLoop(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newGreetingActor())
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "cancel-idle", struct{}{})
	err := env.GetWorkflowError()
	require.Error(t, err, "expected cancellation error")
	require.True(t, temporal.IsCanceledError(err), "expected cancellation, got %v", err)
}

func TestTemporalAskTimeoutOverride(t *testing.T) {
	oldTimeout := askTimeout()
	SetDefaultAskTimeout(5 * time.Millisecond)
	defer SetDefaultAskTimeout(oldTimeout)

	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newAskActor())
	mockExternalSignals(env)
	slowSignal := actors.TypeKeyOf(slowAskCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(askStateQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(slowSignal, slowAskCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "ask-timeout", struct{}{})
	payload, err := codec.Marshal(askStateQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var last string
	require.NoError(t, value.Get(&last))
	require.Contains(t, last, "timed out")
}

func TestTemporalWorkflowUpdateHandler(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newUpdateProbeActor())
	updateName := actors.TypeKeyOf(updateAddCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	callback := newWaitUpdateCallbacks()
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(updateAddCommand{Value: 5})
		if err != nil {
			t.Fatalf("marshal update: %v", err)
		}
		env.UpdateWorkflow(updateName, "update-1", callback, payload)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 5*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "update-inline", struct{}{})
	value, err := callback.Wait(time.Second)
	require.NoError(t, err)
	require.Equal(t, "count:5", value)
}

func TestTemporalSpawnChildWorkflow(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	registerActorWorkflow(t, env, newSpawnChildActor())
	runner := registerActorWorkflow(t, env, newSpawnParentActor())
	mockExternalSignals(env)
	spawnSignal := actors.TypeKeyOf(spawnCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(spawnStateQuery{})
	var childID string
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(spawnSignal, spawnCommand{Value: "data"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(spawnStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		var st spawnState
		if err := value.Get(&st); err != nil {
			t.Fatalf("decode: %v", err)
		}
		childID = st.ChildID
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 3*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "spawn-parent", struct{}{})
	if childID == "" {
		t.Fatalf("expected child ID to be recorded")
	}
}

func TestTemporalContinueAsNew(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newContinueActor())
	restartSignal := actors.TypeKeyOf(restartCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(restartSignal, restartCommand{Next: "renewed"})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "continue-wf", "initial")
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatalf("expected continue-as-new error")
	}
	if wfErr, ok := err.(interface{ Unwrap() error }); ok {
		err = wfErr.Unwrap()
	}
	if !workflow.IsContinueAsNewError(err) && err.Error() != "continue as new" {
		t.Fatalf("expected continue-as-new error, got %v", err)
	}
}

func TestTemporalSelectorProcessesBurst(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newSelectorBenchActor())
	incSignal := actors.TypeKeyOf(selectorBenchCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(selectorBenchQuery{})
	total := 50
	var count int
	env.RegisterDelayedCallback(func() {
		for i := 0; i < total; i++ {
			env.SignalWorkflow(incSignal, selectorBenchCommand{})
		}
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(selectorBenchQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&count); err != nil {
			t.Fatalf("decode: %v", err)
		}
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 2*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "selector-burst", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, total, count)
}

func TestSignalBurstDuringStartup(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newBurstActor())
	addSignal := actors.TypeKeyOf(burstAddCommand{})
	queryName := actors.TypeKeyOf(burstStateQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var snapshot burstState
	total := 50
	for i := 0; i < total; i++ {
		v := i
		delay := time.Duration(i) * time.Microsecond
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(addSignal, burstAddCommand{Value: v})
		}, delay)
	}
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(burstStateQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&snapshot); err != nil {
			t.Fatalf("decode snapshot: %v", err)
		}
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 3*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "burst-startup", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, snapshot.Values, total)
	for i := 0; i < total; i++ {
		require.Equal(t, i, snapshot.Values[i])
	}
}

func TestQueryCacheRespectsTTLAndInvalidation(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	var hits atomic.Int32
	runner := registerActorWorkflow(t, env, newQueryCacheActor(&hits, 20*time.Millisecond))
	queryName := actors.TypeKeyOf(cacheQuery{})
	incrementSignal := actors.TypeKeyOf(cacheIncrementCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var results []int
	queryAt := func(delay time.Duration) {
		env.RegisterDelayedCallback(func() {
			payload, err := codec.Marshal(cacheQuery{})
			if err != nil {
				t.Fatalf("marshal query: %v", err)
			}
			value, err := env.QueryWorkflow(queryName, payload)
			if err != nil {
				t.Fatalf("query workflow: %v", err)
			}
			var out int
			if err := value.Get(&out); err != nil {
				t.Fatalf("decode query result: %v", err)
			}
			results = append(results, out)
		}, delay)
	}
	queryAt(1 * time.Millisecond)
	queryAt(2 * time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(incrementSignal, cacheIncrementCommand{Delta: 1})
	}, 3*time.Millisecond)
	queryAt(4 * time.Millisecond)
	queryAt(5 * time.Millisecond)
	queryAt(50 * time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 60*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "query-cache", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []int{0, 0, 1, 1, 1}, results)
	require.Equal(t, int32(3), hits.Load(), "expected cache to avoid handler invocations")
}

func TestContinueRequestSignal(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newRotateChildActor())
	var reply continueReply
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, continueReplySignal, mock.Anything).
		Run(func(args mock.Arguments) {
			if val, ok := args.Get(4).(continueReply); ok {
				reply = val
			}
		}).Return(nil).Maybe()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueRequestSignal, continueRequest{
			ID:            "req-1",
			ReplyWorkflow: "orchestrator",
			ReplySignal:   continueReplySignal,
		})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "rotate-child", rotateChildInit{Generation: 0})
	if err := env.GetWorkflowError(); err != nil {
		if wfErr, ok := err.(interface{ Unwrap() error }); ok {
			err = wfErr.Unwrap()
		}
		if !workflow.IsContinueAsNewError(err) {
			t.Fatalf("workflow error: %v", err)
		}
	}
	if reply.ID != "req-1" {
		t.Fatalf("expected reply for req-1, got %q", reply.ID)
	}
	next, ok := reply.Init.(rotateChildInit)
	if !ok {
		t.Fatalf("expected init payload in reply")
	}
	if next.Generation != 1 {
		t.Fatalf("expected generation 1 after rotation, got %d", next.Generation)
	}
}

func TestContinueRequestOverrideInit(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newRotateChildActor())
	var reply continueReply
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, continueReplySignal, mock.Anything).
		Run(func(args mock.Arguments) {
			if val, ok := args.Get(4).(continueReply); ok {
				reply = val
			}
		}).Return(nil).Maybe()
	override := rotateChildInit{Generation: 99}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueRequestSignal, continueRequest{
			ID:            "req-override",
			ReplyWorkflow: "orchestrator",
			ReplySignal:   continueReplySignal,
			Init:          override,
		})
	}, time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "rotate-child", rotateChildInit{Generation: 0})
	err := env.GetWorkflowError()
	if err != nil {
		if wfErr, ok := err.(interface{ Unwrap() error }); ok {
			err = wfErr.Unwrap()
		}
		if !workflow.IsContinueAsNewError(err) {
			t.Fatalf("workflow error: %v", err)
		}
	}
	if reply.ID != "req-override" {
		t.Fatalf("unexpected reply ID: %s", reply.ID)
	}
	raw, err := codec.Marshal(reply.Init)
	if err != nil {
		t.Fatalf("marshal override: %v", err)
	}
	var init rotateChildInit
	if err := codec.Unmarshal(raw, &init); err != nil {
		t.Fatalf("decode override: %v", err)
	}
	if init != override {
		t.Fatalf("expected override %+v, got %+v", override, init)
	}
}

func TestTemporalOneShotWorkflow(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newOneShotActor())
	desc := runner.Description()
	cmdName := desc.CommandTypes[actors.TypeKeyOf(oneShotComputeCommand{})]
	start := startEnvelope{
		Payload:  oneShotInit{Prefix: "compute"},
		OneShot:  &oneShotCommand{Name: cmdName, Payload: oneShotComputeCommand{Value: "value"}},
		Envelope: true,
	}
	env.ExecuteWorkflow(runner.Workflow(), "one-shot", start)
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "compute:value", result)
}

func TestTemporalPatchVersion(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newVersionActor())
	cmdSignal := actors.TypeKeyOf(versionCommand{})
	queryName := actors.TypeKeyOf(versionQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	var result string
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(cmdSignal, versionCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		payload, err := codec.Marshal(versionQuery{})
		if err != nil {
			t.Fatalf("marshal query: %v", err)
		}
		value, err := env.QueryWorkflow(queryName, payload)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if err := value.Get(&result); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 4*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "version-wf", struct{}{})
	if result != "new" {
		t.Fatalf("expected patch to activate new path, got %s", result)
	}
}

// --- actor builders ---

type retryState struct {
	Attempts int
}

type selectorBenchState struct {
	Count int
}

type updateProbeState struct {
	Count int
}

type retryCommand struct {
	actors.CommandMsg[struct{}]
}

type stopLoopCommand struct {
	actors.CommandMsg[struct{}]
}

type selectorTickCommand struct {
	actors.CommandMsg[struct{}]
}

type selectorBenchCommand struct {
	actors.CommandMsg[struct{}]
}

type selectorBenchQuery struct {
	actors.QueryMsg[int]
}

type retryStateQuery struct {
	actors.QueryMsg[retryState]
}

type timeoutCommand struct {
	actors.CommandMsg[struct{}]
	Delay time.Duration
}

type validatorCommand struct {
	actors.CommandMsg[struct{}]
	Input string
}

type commandControlQuery struct {
	actors.QueryMsg[commandControlState]
}

type greetingQuery struct {
	actors.QueryMsg[string]
	Name string
}

type proxyCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type echoCommand struct {
	actors.CommandMsg[echoResponse]
	Message string
}

type echoResponse struct {
	Text string
}

type updateAddCommand struct {
	actors.CommandMsg[string]
	Value int
}

type rejectAskCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type rejectEchoCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type validateAskCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type slowAskCommand struct {
	actors.CommandMsg[struct{}]
}

type slowEchoCommand struct {
	actors.CommandMsg[struct{}]
	Delay time.Duration
}

type errorAskCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type errorEchoCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type tellMissingCommand struct {
	actors.CommandMsg[struct{}]
}

type tellOrphanCommand struct {
	actors.CommandMsg[struct{}]
	TargetID string
}

type tellStateQuery struct {
	actors.QueryMsg[tellState]
}

type tellState struct {
	Errors map[string]string
}

type memoCommand struct {
	actors.CommandMsg[struct{}]
	Value string
}

type memoContinueCommand struct {
	actors.CommandMsg[struct{}]
}

type memoQuery struct {
	actors.QueryMsg[memoState]
}

type memoState struct {
	Values []string
}

type memoInit struct {
	Values []string
}

type searchAttrCommand struct {
	actors.CommandMsg[struct{}]
	Key   string
	Value string
}

type searchAttrInvalidCommand struct {
	actors.CommandMsg[struct{}]
}

type searchAttrQuery struct {
	actors.QueryMsg[searchAttrState]
}

type searchAttrState struct {
	Values map[string]string
	Errors []string
}

type observabilityCommand struct {
	actors.CommandMsg[struct{}]
}

type observabilityErrorCommand struct {
	actors.CommandMsg[struct{}]
}

type observabilityAskCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type observabilityEchoCommand struct {
	actors.CommandMsg[string]
	Message string
}

type observabilityState struct {
	Count int
}

type businessErrorCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type unexpectedErrorCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type businessStateQuery struct {
	actors.QueryMsg[businessState]
}

type businessState struct {
	Errors []string
}

type spawnMissingKindCommand struct {
	actors.CommandMsg[struct{}]
}

type spawnConflictCommand struct {
	actors.CommandMsg[struct{}]
}

type spawnCrashCommand struct {
	actors.CommandMsg[struct{}]
}

type spawnStopChildCommand struct {
	actors.CommandMsg[struct{}]
}

type spawnTesterQuery struct {
	actors.QueryMsg[spawnTesterState]
}

type spawnTesterState struct {
	Errors  map[string]string
	ChildID string
}

type crashChildCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

const spawnChildFixedID = "spawn-test-child-fixed"

type tellRotationCommand struct {
	actors.CommandMsg[struct{}]
	TargetID string
}

type tellQueueCommand struct {
	actors.CommandMsg[struct{}]
	TargetID string
}

type validatedEchoCommand struct {
	actors.CommandMsg[struct{}]
	Message string
}

type askState struct {
	Last   string
	Errors []string
}

type askStateQuery struct {
	actors.QueryMsg[string]
}

type spawnState struct {
	ChildID string
}

type spawnStateQuery struct {
	actors.QueryMsg[spawnState]
}

type spawnCommand struct {
	actors.CommandMsg[struct{}]
	Value string
}

type restartCommand struct {
	actors.CommandMsg[struct{}]
	Next string
}

type continueState struct {
	Name string
}

type childInit struct {
	Value string
}

type childState struct {
	Value string
}

type patchState struct {
	Decisions []bool
}

type patchStateQuery struct {
	actors.QueryMsg[patchState]
}

type patchRecordCommand struct {
	actors.CommandMsg[struct{}]
}

type patchSkipCommand struct {
	actors.CommandMsg[struct{}]
	Skip bool
}

type diagState struct {
	Value int
}

type diagCommand struct {
	actors.CommandMsg[struct{}]
	Value int
}

type versionState struct {
	Result string
}

type versionCommand struct {
	actors.CommandMsg[struct{}]
}

type versionQuery struct {
	actors.QueryMsg[string]
}

type rotateChildInit struct {
	Generation int
}

type rotateChildState struct {
	Generation int
}

type rotateChildStatusQuery struct {
	actors.QueryMsg[rotateChildState]
}

type nonRetryableCommand struct {
	actors.CommandMsg[struct{}]
}

type retryAfterCommand struct {
	actors.CommandMsg[struct{}]
}

type errorStateQuery struct {
	actors.QueryMsg[errorState]
}

type errorState struct {
	Start            time.Time
	NonRetryAttempts int
	RetryAttempts    int
	RetryDeltas      []time.Duration
}

type effectCommand struct {
	actors.CommandMsg[struct{}]
	Key string
}

type effectStateQuery struct {
	actors.QueryMsg[effectState]
}

type effectState struct {
	Last string
}

type recordEffectActivity struct {
	actors.ActivityMsg[string]
	Key string
}

type effectEdgeQuery struct {
	actors.QueryMsg[effectEdgeState]
}

type effectEdgeState struct {
	History []string
}

type effectEdgeInit struct {
	History []string
}

type effectTTLCommand struct {
	actors.CommandMsg[struct{}]
	Key string
	TTL time.Duration
}

type effectSleepCommand struct {
	actors.CommandMsg[struct{}]
	Delay time.Duration
}

type effectContinueCommand struct {
	actors.CommandMsg[struct{}]
	Key string
}

type activityDeadlineCommand struct {
	actors.CommandMsg[struct{}]
	StartToClose time.Duration
	Work         time.Duration
}

type activityDecodeCommand struct {
	actors.CommandMsg[struct{}]
}

type activityBackgroundCommand struct {
	actors.CommandMsg[struct{}]
	Wait time.Duration
}

type activityEdgeQuery struct {
	actors.QueryMsg[activityEdgeState]
}

type activityEdgeState struct {
	Errors           []string
	BackgroundLogged bool
}

type deadlineActivityReq struct {
	actors.ActivityMsg[bool]
	Wait time.Duration
}

type decodeMismatchRequest struct {
	actors.ActivityMsg[int]
}

type commandControlState struct {
	TimeoutAttempts    int
	Completed          []string
	Errors             []string
	ValidatorAccepted  int
	LastValidatorInput string
}

type oneShotInit struct {
	Prefix string
}

type oneShotState struct {
	Prefix string
}

type oneShotComputeCommand struct {
	actors.CommandMsg[string]
	Value string
}

type burstAddCommand struct {
	actors.CommandMsg[struct{}]
	Value int
}

type burstStateQuery struct {
	actors.QueryMsg[burstState]
}

type burstState struct {
	Values []int
}

type cacheState struct {
	Count int
}

type cacheIncrementCommand struct {
	actors.CommandMsg[struct{}]
	Delta int
}

type cacheQuery struct {
	actors.QueryMsg[int]
}

func newRetryActor() actors.Actor {
	return actors.NewStateful("retry", func() retryState { return retryState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *retryState, _ retryCommand) (struct{}, error) {
				st.Attempts++
				if st.Attempts < 2 {
					return struct{}{}, fmt.Errorf("fail")
				}
				return struct{}{}, nil
			}, actors.WithRetry(actors.RetryPolicy{MaxAttempts: 3})),
			stopCommandAction[retryState](),
			actors.Query(func(ctx actors.Ctx, st retryState, _ retryStateQuery) (retryState, error) {
				return st, nil
			}),
		).
		Build()
}

func newGreetingActor() actors.Actor {
	return actors.NewStateful("greeter", func() struct{} { return struct{}{} }).
		With(
			actors.Query(func(ctx actors.Ctx, _ struct{}, req greetingQuery) (string, error) {
				return "hello " + req.Name, nil
			}),
			stopCommandAction[struct{}](),
		).
		Build()
}

func newSelectorBenchActor() actors.Actor {
	return actors.NewStateful("selector-bench", func() selectorBenchState { return selectorBenchState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *selectorBenchState, _ selectorBenchCommand) (struct{}, error) {
				st.Count++
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st selectorBenchState, _ selectorBenchQuery) (int, error) {
				return st.Count, nil
			}),
			stopCommandAction[selectorBenchState](),
		).
		Build()
}

func newAskActor() actors.Actor {
	return actors.NewStateful("asker", func() askState { return askState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *askState, cmd proxyCommand) (struct{}, error) {
				resp, err := actors.Ask[echoCommand, echoResponse](ctx, ctx.Self(), echoCommand{Message: cmd.Message})
				if err != nil {
					return struct{}{}, err
				}
				st.Last = resp.Text
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd echoCommand) (echoResponse, error) {
				return echoResponse{Text: "echo: " + cmd.Message}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd rejectAskCommand) (struct{}, error) {
				_, err := actors.Ask[rejectEchoCommand, struct{}](ctx, ctx.Self(), rejectEchoCommand{Message: cmd.Message})
				if err != nil {
					st.Last = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd rejectEchoCommand) (struct{}, error) {
				return struct{}{}, actors.BusinessError(fmt.Errorf("reject %s", cmd.Message))
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd validateAskCommand) (struct{}, error) {
				_, err := actors.Ask[validatedEchoCommand, struct{}](ctx, ctx.Self(), validatedEchoCommand{Message: cmd.Message})
				if err != nil {
					st.Last = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd validatedEchoCommand) (struct{}, error) {
				st.Last = "validated: " + cmd.Message
				return struct{}{}, nil
			}, actors.WithValidator(func(cmd validatedEchoCommand) error {
				if strings.TrimSpace(cmd.Message) == "" {
					return fmt.Errorf("validator: message required")
				}
				return nil
			})),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd errorAskCommand) (struct{}, error) {
				_, err := actors.Ask[errorEchoCommand, struct{}](ctx, ctx.Self(), errorEchoCommand{Message: cmd.Message})
				if err != nil {
					st.Last = err.Error()
					st.Errors = append(st.Errors, err.Error())
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd errorEchoCommand) (struct{}, error) {
				return struct{}{}, fmt.Errorf("callee failed: %s", cmd.Message)
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, _ slowAskCommand) (struct{}, error) {
				_, err := actors.Ask[slowEchoCommand, struct{}](ctx, ctx.Self(), slowEchoCommand{Delay: time.Minute})
				if err != nil {
					st.Last = err.Error()
					st.Errors = append(st.Errors, err.Error())
				}
				return struct{}{}, err
			}),
			actors.Command(func(ctx actors.Ctx, st *askState, cmd slowEchoCommand) (struct{}, error) {
				if err := ctx.Sleep(cmd.Delay); err != nil {
					return struct{}{}, err
				}
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st askState, _ askStateQuery) (string, error) {
				return st.Last, nil
			}),
			stopCommandAction[askState](),
		).
		Build()
}

func newUpdateProbeActor() actors.Actor {
	return actors.NewStateful("update-probe", func() updateProbeState { return updateProbeState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *updateProbeState, cmd updateAddCommand) (string, error) {
				st.Count += cmd.Value
				return fmt.Sprintf("count:%d", st.Count), nil
			}),
			stopCommandAction[updateProbeState](),
		).
		Build()
}

func newSpawnParentActor() actors.Actor {
	return actors.NewStateful("spawn-parent", func() spawnState { return spawnState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *spawnState, cmd spawnCommand) (struct{}, error) {
				ref, err := actors.Spawn(ctx, "spawn-child", childInit{Value: cmd.Value}, actors.WithChildTaskQueue("child-queue"))
				if err != nil {
					return struct{}{}, err
				}
				st.ChildID = ref.ID
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st spawnState, _ spawnStateQuery) (spawnState, error) {
				return st, nil
			}),
			stopCommandAction[spawnState](),
		).
		Build()
}

func newSpawnChildActor() actors.Actor {
	return actors.NewStateful("spawn-child", func() childState { return childState{} }).
		OnStart(actors.Start(func(ctx actors.Ctx, init childInit) (childState, error) {
			return childState{Value: init.Value}, nil
		})).
		Build()
}

func newCommandControlActor() actors.Actor {
	return actors.NewStateful("command-control", func() commandControlState { return commandControlState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *commandControlState, cmd timeoutCommand) (struct{}, error) {
				st.TimeoutAttempts++
				delay := cmd.Delay
				if st.TimeoutAttempts > 1 {
					delay = time.Millisecond
				}
				if delay > 0 {
					if err := ctx.Sleep(delay); err != nil {
						st.Errors = append(st.Errors, err.Error())
						return struct{}{}, err
					}
				}
				st.Completed = append(st.Completed, "timeout")
				return struct{}{}, nil
			}, actors.WithRetry(actors.RetryPolicy{MaxAttempts: 2}), actors.WithTimeout(20*time.Millisecond)),
			actors.Command(func(ctx actors.Ctx, st *commandControlState, cmd validatorCommand) (struct{}, error) {
				st.ValidatorAccepted++
				st.LastValidatorInput = cmd.Input
				st.Completed = append(st.Completed, "validator")
				return struct{}{}, nil
			}, actors.WithValidator(func(cmd validatorCommand) error {
				if strings.TrimSpace(cmd.Input) == "" {
					return fmt.Errorf("validator: input required")
				}
				return nil
			})),
			actors.Query(func(ctx actors.Ctx, st commandControlState, _ commandControlQuery) (commandControlState, error) {
				return st, nil
			}),
			stopCommandAction[commandControlState](),
		).
		Build()
}

func newEffectActor(counter *atomic.Int32, snapshot bool) actors.Actor {
	builder := actors.NewStateful("effect", func() effectState { return effectState{} }).
		With(
			actors.Activity[recordEffectActivity, string]("record-effect", func(ctx context.Context, payload recordEffectActivity) (string, error) {
				val := counter.Add(1)
				return fmt.Sprintf("%s-%d", payload.Key, val), nil
			}),
			actors.Command(func(ctx actors.Ctx, st *effectState, cmd effectCommand) (struct{}, error) {
				result, err := actors.Effect[string](ctx, cmd.Key, func(inner actors.Ctx) (string, error) {
					return actors.RunActivity(inner, "record-effect", recordEffectActivity{Key: cmd.Key}, actors.WithActivityStartToClose(5*time.Second))
				})
				if err != nil {
					return struct{}{}, err
				}
				st.Last = result
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st effectState, _ effectStateQuery) (effectState, error) {
				return st, nil
			}),
			stopCommandAction[effectState](),
		)
	if snapshot {
		builder = builder.WithSnapshot(actors.SnapshotConfig[effectState]{
			Every: 1,
			ContinueArgs: func(state effectState) (any, error) {
				return state, nil
			},
		})
	}
	return builder.Build()
}

func newBurstActor() actors.Actor {
	return actors.NewStateful("burst", func() burstState { return burstState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *burstState, cmd burstAddCommand) (struct{}, error) {
				st.Values = append(st.Values, cmd.Value)
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st burstState, _ burstStateQuery) (burstState, error) {
				return burstState{
					Values: append([]int(nil), st.Values...),
				}, nil
			}),
			stopCommandAction[burstState](),
		).
		Build()
}

func newQueryCacheActor(hits *atomic.Int32, ttl time.Duration) actors.Actor {
	return actors.NewStateful("query-cache", func() cacheState { return cacheState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *cacheState, cmd cacheIncrementCommand) (struct{}, error) {
				st.Count += cmd.Delta
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st cacheState, _ cacheQuery) (int, error) {
				if hits != nil {
					hits.Add(1)
				}
				return st.Count, nil
			}, actors.WithCache(ttl)),
			stopCommandAction[cacheState](),
		).
		Build()
}

func newTellParentActor() actors.Actor {
	return actors.NewStateful("tell-parent", func() tellState {
		return tellState{Errors: make(map[string]string)}
	}).
		With(
			actors.Command(func(ctx actors.Ctx, st *tellState, _ tellMissingCommand) (struct{}, error) {
				err := actors.Tell(ctx, actors.Ref{Kind: "missing-kind", ID: "missing-1"}, proxyCommand{Message: "noop"})
				if err != nil {
					st.Errors["missing"] = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *tellState, cmd tellOrphanCommand) (struct{}, error) {
				ref := actors.Ref{Kind: "tell-child", ID: cmd.TargetID}
				err := actors.Tell(ctx, ref, proxyCommand{Message: "orphan"})
				if err != nil {
					st.Errors["orphan"] = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *tellState, cmd tellRotationCommand) (struct{}, error) {
				ref := actors.Ref{Kind: "tell-child", ID: cmd.TargetID}
				err := actors.Tell(ctx, ref, proxyCommand{Message: "rotate"})
				if err != nil {
					st.Errors["rotation"] = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *tellState, cmd tellQueueCommand) (struct{}, error) {
				ref := actors.Ref{Kind: "tell-child", ID: cmd.TargetID}
				err := actors.Tell(ctx, ref, proxyCommand{Message: "queue"})
				if err != nil {
					st.Errors["queue"] = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st tellState, _ tellStateQuery) (tellState, error) {
				return st, nil
			}),
			stopCommandAction[tellState](),
		).
		Build()
}

func newTellChildActor() actors.Actor {
	return actors.NewStateful("tell-child", func() struct{} { return struct{}{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *struct{}, cmd proxyCommand) (struct{}, error) {
				return struct{}{}, nil
			}),
			stopCommandAction[struct{}](),
		).
		Build()
}

func newSpawnTesterActor() actors.Actor {
	return actors.NewStateful("spawn-tester", func() spawnTesterState {
		return spawnTesterState{Errors: make(map[string]string)}
	}).
		With(
			actors.Command(func(ctx actors.Ctx, st *spawnTesterState, _ spawnMissingKindCommand) (struct{}, error) {
				_, err := actors.SpawnOneShot(ctx, proxyCommand{Message: "missing"})
				if err != nil {
					st.Errors["missingKind"] = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *spawnTesterState, _ spawnConflictCommand) (struct{}, error) {
				ref, err := actors.Spawn(ctx, "spawn-test-child", childInit{Value: "conflict"},
					actors.WithChildName(spawnChildFixedID))
				if err != nil {
					st.Errors["conflict"] = err.Error()
				} else {
					st.ChildID = ref.ID
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *spawnTesterState, _ spawnCrashCommand) (struct{}, error) {
				_, err := actors.SpawnOneShot(ctx, crashChildCommand{Message: "boom"}, actors.WithChildKind("spawn-crash-child"))
				if err != nil {
					st.Errors["crash"] = err.Error()
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *spawnTesterState, _ spawnStopChildCommand) (struct{}, error) {
				if st.ChildID != "" {
					_ = actors.Tell(ctx, actors.Ref{Kind: "spawn-test-child", ID: st.ChildID}, stopLoopCommand{})
					st.ChildID = ""
				}
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st spawnTesterState, _ spawnTesterQuery) (spawnTesterState, error) {
				return st, nil
			}),
			stopCommandAction[spawnTesterState](),
		).
		Build()
}

func newSpawnTestChildActor() actors.Actor {
	return actors.NewStateful("spawn-test-child", func() struct{} { return struct{}{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *struct{}, cmd proxyCommand) (struct{}, error) {
				if cmd.Message == "rotate" {
					return struct{}{}, actors.ContinueAsNew(ctx, childInit{Value: "rotate"})
				}
				return struct{}{}, nil
			}),
			stopCommandAction[struct{}](),
		).
		Build()
}

func newSpawnCrashChildActor() actors.Actor {
	return actors.NewStateful("spawn-crash-child", func() struct{} { return struct{}{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *struct{}, cmd crashChildCommand) (struct{}, error) {
				return struct{}{}, fmt.Errorf("child crashed: %s", cmd.Message)
			}),
			stopCommandAction[struct{}](),
		).
		Build()
}

func newBusinessErrorActor() actors.Actor {
	return actors.NewStateful("business", func() businessState { return businessState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *businessState, cmd businessErrorCommand) (struct{}, error) {
				st.Errors = append(st.Errors, "business error: "+cmd.Message)
				return struct{}{}, actors.BusinessError(fmt.Errorf("business error: %s", cmd.Message))
			}),
			actors.Command(func(ctx actors.Ctx, st *businessState, cmd unexpectedErrorCommand) (struct{}, error) {
				return struct{}{}, fmt.Errorf("unexpected: %s", cmd.Message)
			}),
			actors.Query(func(ctx actors.Ctx, st businessState, _ businessStateQuery) (businessState, error) {
				return st, nil
			}),
			stopCommandAction[businessState](),
		).
		Build()
}

func newErrorActor() actors.Actor {
	return actors.NewStateful("error-semantics", func() errorState { return errorState{} }).
		OnStart(actors.Start(func(ctx actors.Ctx, init struct{}) (errorState, error) {
			return errorState{Start: ctx.Now()}, nil
		})).
		With(
			actors.Command(func(ctx actors.Ctx, st *errorState, cmd nonRetryableCommand) (struct{}, error) {
				st.NonRetryAttempts++
				return struct{}{}, actors.NonRetryable(fmt.Errorf("no retry"))
			}),
			actors.Command(func(ctx actors.Ctx, st *errorState, cmd retryAfterCommand) (struct{}, error) {
				st.RetryAttempts++
				now := ctx.Now()
				if st.Start.IsZero() {
					st.Start = now
				}
				st.RetryDeltas = append(st.RetryDeltas, now.Sub(st.Start))
				if st.RetryAttempts == 1 {
					return struct{}{}, actors.RetryAfter(fmt.Errorf("retry later"), 5*time.Second)
				}
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st errorState, _ errorStateQuery) (errorState, error) {
				return st, nil
			}),
			stopCommandAction[errorState](),
		).
		Build()
}

func newContinueActor() actors.Actor {
	return actors.NewStateful("continue", func() continueState { return continueState{} }).
		With(
			actors.Start(func(ctx actors.Ctx, init string) (continueState, error) {
				return continueState{Name: init}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *continueState, cmd restartCommand) (struct{}, error) {
				st.Name = cmd.Next
				return struct{}{}, actors.ContinueAsNew(ctx, cmd.Next)
			}),
		).
		Build()
}

func newVersionActor() actors.Actor {
	return actors.NewStateful("version", func() versionState { return versionState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *versionState, _ versionCommand) (struct{}, error) {
				if actors.Patch(ctx, "v2") {
					st.Result = "new"
				} else {
					st.Result = "old"
				}
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st versionState, _ versionQuery) (string, error) {
				return st.Result, nil
			}),
			stopCommandAction[versionState](),
		).
		Build()
}

func newRotateChildActor() actors.Actor {
	return actors.NewStateful("rotate-child", func() rotateChildState { return rotateChildState{} }).
		OnStart(actors.Start(func(ctx actors.Ctx, init rotateChildInit) (rotateChildState, error) {
			return rotateChildState{Generation: init.Generation}, nil
		})).
		With(
			actors.Query(func(ctx actors.Ctx, st rotateChildState, _ rotateChildStatusQuery) (rotateChildState, error) {
				return st, nil
			}),
			stopCommandAction[rotateChildState](),
		).
		WithSnapshot(actors.SnapshotConfig[rotateChildState]{
			Every: 1000,
			ContinueArgs: func(st rotateChildState) (any, error) {
				return rotateChildInit{Generation: st.Generation + 1}, nil
			},
		}).
		Build()
}

func newOneShotActor() actors.Actor {
	return actors.NewStateful("one-shot", func() oneShotState { return oneShotState{} }).
		OnStart(actors.Start(func(ctx actors.Ctx, init oneShotInit) (oneShotState, error) {
			return oneShotState{Prefix: init.Prefix}, nil
		})).
		With(
			actors.Command(func(ctx actors.Ctx, st *oneShotState, cmd oneShotComputeCommand) (string, error) {
				return fmt.Sprintf("%s:%s", st.Prefix, cmd.Value), nil
			}),
		).
		Build()
}

func TestDiagnosticQueries(t *testing.T) {
	builder := actors.NewStateful("diag", func() diagState { return diagState{} }).
		WithSnapshot(actors.SnapshotConfig[diagState]{
			Every: 3,
			ContinueArgs: func(state diagState) (any, error) {
				return state, nil
			},
		}).
		DeclarePatch("feat", true).
		With(actors.Command(func(ctx actors.Ctx, st *diagState, cmd diagCommand) (struct{}, error) {
			if actors.Patch(ctx, "feat") {
				st.Value = cmd.Value
			}
			return struct{}{}, nil
		}),
			stopCommandAction[diagState]()).
		Build()
	desc := builder.Spec()
	cmdName := desc.CommandTypes[actors.TypeKeyOf(diagCommand{})]
	stopName := desc.CommandTypes[actors.TypeKeyOf(stopLoopCommand{})]

	runner := NewRunner(builder)
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(dataConverter())
	env.RegisterWorkflowWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: desc.Kind})
	mockExternalSignals(env)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(cmdName, diagCommand{Value: 42})
		env.SignalWorkflow(stopName, stopLoopCommand{})
	}, time.Millisecond)

	env.ExecuteWorkflow(runner.Workflow(), "diag-1", struct{}{})
	require.NoError(t, env.GetWorkflowError())

	raw, err := env.QueryWorkflow(actors.DiagnosticsPatchesQuery)
	require.NoError(t, err)
	var patchReport actors.PatchReport
	require.NoError(t, raw.Get(&patchReport))
	require.Equal(t, desc.Kind, patchReport.Kind)
	require.Len(t, patchReport.Patches, 1)
	require.Equal(t, "feat", patchReport.Patches[0].ID)
	require.True(t, patchReport.Patches[0].DefaultOn)

	snapRaw, err := env.QueryWorkflow(actors.DiagnosticsSnapshotQuery)
	require.NoError(t, err)
	var snapshotReport actors.SnapshotReport
	require.NoError(t, snapRaw.Get(&snapshotReport))
	require.Equal(t, desc.Kind, snapshotReport.Kind)
	require.True(t, snapshotReport.Snapshot.Enabled)
	require.Equal(t, 3, snapshotReport.Snapshot.SnapshotEvery)
	require.Equal(t, 1, snapshotReport.Snapshot.CommandsSinceSnapshot)
}

func TestPatchDeterminismDoesNotToggleMidRun(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newPatchEdgeActor())
	recordSignal := actors.TypeKeyOf(patchRecordCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(patchStateQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(recordSignal, patchRecordCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(recordSignal, patchRecordCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 3*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "patch-determinism", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	payload, err := codec.Marshal(patchStateQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var state patchState
	require.NoError(t, value.Get(&state))
	require.Len(t, state.Decisions, 2)
	require.Equal(t, state.Decisions[0], state.Decisions[1], "patch decision should remain stable within a run")
}

func TestPatchRemovalContinuesCallingGateForCleanup(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newPatchEdgeActor())
	recordSignal := actors.TypeKeyOf(patchRecordCommand{})
	skipSignal := actors.TypeKeyOf(patchSkipCommand{})
	queryName := actors.TypeKeyOf(patchStateQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(recordSignal, patchRecordCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(skipSignal, patchSkipCommand{Skip: true})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(actors.TypeKeyOf(stopLoopCommand{}), stopLoopCommand{})
	}, 3*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "patch-skip", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	payload, err := codec.Marshal(patchStateQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var state patchState
	require.NoError(t, value.Get(&state))
	require.Len(t, state.Decisions, 2)
}

func stopCommandAction[S any]() actors.CommandAction[S] {
	return actors.Command(func(ctx actors.Ctx, st *S, _ stopLoopCommand) (struct{}, error) {
		return struct{}{}, actors.ErrStopLoop
	})
}

type waitUpdateCallbacks struct {
	done   chan struct{}
	result string
	err    error
	once   sync.Once
}

var _ internalbindings.UpdateCallbacks = (*waitUpdateCallbacks)(nil)

func newWaitUpdateCallbacks() *waitUpdateCallbacks {
	return &waitUpdateCallbacks{done: make(chan struct{})}
}

func (w *waitUpdateCallbacks) Accept() {}

func (w *waitUpdateCallbacks) Reject(err error) {
	w.err = err
	w.finish()
}

func (w *waitUpdateCallbacks) Complete(success interface{}, err error) {
	if str, ok := success.(string); ok {
		w.result = str
	} else if success != nil {
		w.result = fmt.Sprint(success)
	}
	w.err = err
	w.finish()
}

func (w *waitUpdateCallbacks) Wait(timeout time.Duration) (string, error) {
	select {
	case <-w.done:
		return w.result, w.err
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout waiting for update completion")
	}
}

func (w *waitUpdateCallbacks) finish() {
	w.once.Do(func() {
		close(w.done)
	})
}

func registerActorWorkflow(tb testing.TB, env *testsuite.TestWorkflowEnvironment, actor actors.Actor) *Runner {
	tb.Helper()
	env.SetDataConverter(dataConverter())
	runner := NewRunner(actor)
	env.RegisterWorkflowWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: runner.Description().Kind})
	for name, fn := range runner.Activities() {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	return runner
}

func mockExternalSignals(env *testsuite.TestWorkflowEnvironment) {
	mockExternalSignalsWithCapture(env, nil)
}

func mockExternalSignalsWithCapture(env *testsuite.TestWorkflowEnvironment, capture func(string, any)) {
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			workflowID, _ := args.Get(1).(string)
			signalName, _ := args.Get(3).(string)
			payload := args.Get(4)
			if capture != nil {
				capture(signalName, payload)
			}
			env.SignalWorkflow(signalName, payload)
			_ = workflowID
		}).Return(nil).Maybe()
}
func TestMemoPersistsAcrossContinueAsNew(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetMemoOnStart(map[string]any{"seed": "alpha"})
	runner := registerActorWorkflow(t, env, newMemoActor())
	mockExternalSignals(env)
	addSignal := actors.TypeKeyOf(memoCommand{})
	continueSignal := actors.TypeKeyOf(memoContinueCommand{})
	queryName := actors.TypeKeyOf(memoQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(addSignal, memoCommand{Value: "beta"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(continueSignal, memoContinueCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 50*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "memo-actor", memoInit{})
	payload, err := codec.Marshal(memoQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state memoState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if len(state.Values) != 2 || state.Values[0] != "alpha:beta" || state.Values[1] != "alpha:gamma" {
		t.Fatalf("unexpected memo state: %#v", state)
	}
}

func TestSearchAttributeHelpers(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newSearchAttrActor())
	setSignal := actors.TypeKeyOf(searchAttrCommand{})
	invalidSignal := actors.TypeKeyOf(searchAttrInvalidCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	queryName := actors.TypeKeyOf(searchAttrQuery{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(setSignal, searchAttrCommand{Key: "status", Value: "active"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(invalidSignal, searchAttrInvalidCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 3*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "search-attr", struct{}{})
	payload, err := codec.Marshal(searchAttrQuery{})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	value, err := env.QueryWorkflow(queryName, payload)
	if err != nil {
		t.Fatalf("query state: %v", err)
	}
	var state searchAttrState
	if err := value.Get(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if state.Values["status"] != "active" {
		t.Fatalf("search attribute missing: %#v", state.Values)
	}
	if len(state.Errors) == 0 || state.Errors[0] != "unsupported attribute type" {
		t.Fatalf("expected invalid attribute error, got %#v", state.Errors)
	}
}

func TestTellRequestDeadlineDrop(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newMetadataActor())
	mockExternalSignals(env)
	recordName := actors.TypeKeyOf(metadataRecordCommand{})
	queryName := actors.TypeKeyOf(metadataQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: recordName,
			Payload: metadataRecordCommand{Value: "expired"},
			Envelope: actors.MessageMetadata{
				Deadline: time.Unix(0, 0),
			},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: recordName,
			Payload: metadataRecordCommand{Value: "fresh"},
			Envelope: actors.MessageMetadata{
				Deadline: time.Unix(1<<60, 0),
			},
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 20*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "metadata-deadline", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	payload, err := codec.Marshal(metadataQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var state metadataState
	require.NoError(t, value.Get(&state))
	require.Equal(t, []string{"fresh"}, state.Values)
}

func TestTellRequestRetryBudgetDrop(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newMetadataActor())
	mockExternalSignals(env)
	recordName := actors.TypeKeyOf(metadataRecordCommand{})
	queryName := actors.TypeKeyOf(metadataQuery{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: recordName,
			Payload: metadataRecordCommand{Value: "denied"},
			Envelope: actors.MessageMetadata{
				RetryBudget:    0,
				RetryBudgetSet: true,
			},
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(tellRequestSignal, tellRequest{
			Command: recordName,
			Payload: metadataRecordCommand{Value: "allowed"},
			Envelope: actors.MessageMetadata{
				RetryBudget:    1,
				RetryBudgetSet: true,
			},
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 20*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "metadata-budget", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	payload, err := codec.Marshal(metadataQuery{})
	require.NoError(t, err)
	value, err := env.QueryWorkflow(queryName, payload)
	require.NoError(t, err)
	var state metadataState
	require.NoError(t, value.Get(&state))
	require.Equal(t, []string{"allowed"}, state.Values)
}

func TestSignalTimeoutSetsDeadline(t *testing.T) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newSignalTimeoutActor(5*time.Second))
	var captured []tellRequest
	mockExternalSignalsWithCapture(env, func(signal string, payload any) {
		if signal == tellRequestSignal {
			if req, ok := payload.(tellRequest); ok {
				captured = append(captured, req)
			}
		}
	})
	sendSignal := actors.TypeKeyOf(timeoutSendCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(sendSignal, timeoutSendCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 20*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "signal-timeout", struct{}{})
	require.NoError(t, env.GetWorkflowError())
	require.NotEmpty(t, captured, "expected tell request captured")
	require.False(t, captured[0].Envelope.Deadline.IsZero(), "deadline should be populated from signal timeout")
}

func TestObservabilityHooks(t *testing.T) {
	listener := &captureListener{}
	observability.SetListener(listener)
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	runner := registerActorWorkflow(t, env, newObservabilityActor())
	commandSignal := actors.TypeKeyOf(observabilityCommand{})
	errorSignal := actors.TypeKeyOf(observabilityErrorCommand{})
	askSignal := actors.TypeKeyOf(observabilityAskCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(commandSignal, observabilityCommand{})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(errorSignal, observabilityErrorCommand{})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(askSignal, observabilityAskCommand{Message: "ping"})
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, 3*time.Millisecond)
	env.ExecuteWorkflow(runner.Workflow(), "observability", struct{}{})
	observability.SetListener(nil)
	if len(listener.commandStarts) == 0 || len(listener.commandFinishes) == 0 {
		t.Fatalf("expected command events, got %#v %#v", listener.commandStarts, listener.commandFinishes)
	}
	if !listener.errorObserved {
		t.Fatalf("expected error event to be observed")
	}
	if len(listener.askStarts) == 0 || len(listener.askFinishes) == 0 {
		t.Fatalf("expected ask events, got %#v %#v", listener.askStarts, listener.askFinishes)
	}
}

func newMemoActor() actors.Actor {
	return actors.NewStateful("memo", func() memoState { return memoState{} }).
		OnStart(actors.Start(func(ctx actors.Ctx, init memoInit) (memoState, error) {
			return memoState{Values: append([]string(nil), init.Values...)}, nil
		})).
		With(
			actors.Command(func(ctx actors.Ctx, st *memoState, cmd memoCommand) (struct{}, error) {
				memo := actors.Memo(ctx)
				seed, _ := memo["seed"].(string)
				st.Values = append(st.Values, seed+":"+cmd.Value)
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *memoState, _ memoContinueCommand) (struct{}, error) {
				_ = actors.Tell(ctx, ctx.Self(), memoCommand{Value: "gamma"})
				return struct{}{}, actors.ContinueAsNew(ctx, memoInit{Values: append([]string(nil), st.Values...)})
			}),
			actors.Query(func(ctx actors.Ctx, st memoState, _ memoQuery) (memoState, error) {
				return st, nil
			}),
			stopCommandAction[memoState](),
		).
		Build()
}

func newSearchAttrActor() actors.Actor {
	return actors.NewStateful("search-attr", func() searchAttrState {
		return searchAttrState{Values: make(map[string]string)}
	}).
		With(
			actors.Command(func(ctx actors.Ctx, st *searchAttrState, cmd searchAttrCommand) (struct{}, error) {
				if err := actors.UpsertSearchAttributes(ctx, map[string]any{cmd.Key: cmd.Value}); err != nil {
					st.Errors = append(st.Errors, err.Error())
				}
				attrs := actors.SearchAttributes(ctx)
				for k, v := range attrs {
					if s, ok := v.(string); ok {
						st.Values[k] = s
					}
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *searchAttrState, _ searchAttrInvalidCommand) (struct{}, error) {
				defer func() {
					if r := recover(); r != nil {
						st.Errors = append(st.Errors, "unsupported attribute type")
						st.Errors = append(st.Errors, fmt.Sprint(r))
					}
				}()
				payload := map[string]any{"invalid": func() {}}
				if err := actors.UpsertSearchAttributes(ctx, payload); err != nil {
					st.Errors = append(st.Errors, "unsupported attribute type")
					st.Errors = append(st.Errors, err.Error())
					return struct{}{}, nil
				}
				if _, err := codec.Marshal(payload); err != nil {
					st.Errors = append(st.Errors, "unsupported attribute type")
					st.Errors = append(st.Errors, err.Error())
					return struct{}{}, nil
				}
				st.Errors = append(st.Errors, "unsupported attribute type")
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st searchAttrState, _ searchAttrQuery) (searchAttrState, error) {
				return st, nil
			}),
			stopCommandAction[searchAttrState](),
		).
		Build()
}

func newObservabilityActor() actors.Actor {
	return actors.NewStateful("observability", func() observabilityState { return observabilityState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *observabilityState, _ observabilityCommand) (struct{}, error) {
				st.Count++
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *observabilityState, _ observabilityErrorCommand) (struct{}, error) {
				return struct{}{}, actors.BusinessError(fmt.Errorf("observability error"))
			}),
			actors.Command(func(ctx actors.Ctx, st *observabilityState, cmd observabilityAskCommand) (struct{}, error) {
				_, err := actors.Ask[observabilityEchoCommand, string](ctx, ctx.Self(), observabilityEchoCommand{Message: cmd.Message})
				return struct{}{}, err
			}),
			actors.Command(func(ctx actors.Ctx, st *observabilityState, cmd observabilityEchoCommand) (string, error) {
				return "echo:" + cmd.Message, nil
			}),
			stopCommandAction[observabilityState](),
		).
		Build()
}

type metadataState struct {
	Values []string
}

type metadataRecordCommand struct {
	actors.CommandMsg[struct{}]
	Value string
}

type metadataQuery struct{}

func newMetadataActor() actors.Actor {
	return actors.NewStateful("metadata", func() metadataState { return metadataState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *metadataState, cmd metadataRecordCommand) (struct{}, error) {
				st.Values = append(st.Values, cmd.Value)
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st metadataState, _ metadataQuery) (metadataState, error) {
				return st, nil
			}),
			stopCommandAction[metadataState](),
		).
		Build()
}

type timeoutState struct{}

type timeoutSendCommand struct {
	actors.CommandMsg[struct{}]
}

type timeoutRecordCommand struct {
	actors.CommandMsg[struct{}]
}

func newSignalTimeoutActor(timeout time.Duration) actors.Actor {
	builder := actors.NewStateful("signal-timeout", func() timeoutState { return timeoutState{} }).
		WithSignalTimeout(actors.TypeKeyOf(timeoutRecordCommand{}), timeout)
	return builder.With(
		actors.Command(func(ctx actors.Ctx, st *timeoutState, _ timeoutSendCommand) (struct{}, error) {
			return struct{}{}, actors.Tell(ctx, ctx.Self(), timeoutRecordCommand{})
		}),
		actors.Command(func(ctx actors.Ctx, st *timeoutState, _ timeoutRecordCommand) (struct{}, error) {
			return struct{}{}, nil
		}),
		stopCommandAction[timeoutState](),
	).Build()
}

func newActivityEdgeActor() actors.Actor {
	builder := actors.NewStateful("activity-edge", func() activityEdgeState { return activityEdgeState{} }).
		WithActivity("decode-mismatch", func(context.Context, any) (any, error) {
			return map[string]string{"value": "oops"}, nil
		}).
		WithActivity("bg-fail", func(context.Context, any) (any, error) {
			return nil, fmt.Errorf("bg fail")
		})
	return builder.
		With(
			actors.Activity[deadlineActivityReq, bool]("deadline-check", func(ctx context.Context, req deadlineActivityReq) (bool, error) {
				select {
				case <-ctx.Done():
					return true, ctx.Err()
				case <-time.After(req.Wait):
					return false, nil
				}
			}),
			actors.Command(func(ctx actors.Ctx, st *activityEdgeState, cmd activityDeadlineCommand) (struct{}, error) {
				if _, err := actors.RunActivity[deadlineActivityReq, bool](ctx, "deadline-check", deadlineActivityReq{Wait: cmd.Work},
					actors.WithActivityStartToClose(cmd.StartToClose),
					actors.WithActivityScheduleToClose(cmd.StartToClose)); err != nil {
					st.Errors = append(st.Errors, err.Error())
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *activityEdgeState, _ activityDecodeCommand) (struct{}, error) {
				_, err := actors.RunActivity[decodeMismatchRequest, int](ctx, "decode-mismatch", decodeMismatchRequest{}, actors.WithActivityStartToClose(time.Second))
				if err != nil {
					st.Errors = append(st.Errors, err.Error())
					return struct{}{}, nil
				}
				return struct{}{}, fmt.Errorf("expected decode error")
			}),
			actors.Command(func(ctx actors.Ctx, st *activityEdgeState, cmd activityBackgroundCommand) (struct{}, error) {
				actors.BackgroundActivity(ctx, "bg-fail", struct{}{})
				if cmd.Wait > 0 {
					if err := ctx.Sleep(cmd.Wait); err != nil {
						return struct{}{}, err
					}
				}
				st.BackgroundLogged = true
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st activityEdgeState, _ activityEdgeQuery) (activityEdgeState, error) {
				return st, nil
			}),
			stopCommandAction[activityEdgeState](),
		).
		Build()
}

func newEffectEdgeActor(counter *atomic.Int32) actors.Actor {
	return actors.NewStateful("effect-edge", func() effectEdgeState { return effectEdgeState{} }).
		OnStart(actors.Start(func(ctx actors.Ctx, init effectEdgeInit) (effectEdgeState, error) {
			return effectEdgeState{History: append([]string(nil), init.History...)}, nil
		})).
		With(
			actors.Activity[recordEffectActivity, string]("edge-effect", func(ctx context.Context, payload recordEffectActivity) (string, error) {
				n := counter.Add(1)
				return fmt.Sprintf("%s-%d", payload.Key, n), nil
			}),
			actors.Command(func(ctx actors.Ctx, st *effectEdgeState, cmd effectTTLCommand) (struct{}, error) {
				opts := []actors.EffectOption{}
				if cmd.TTL > 0 {
					opts = append(opts, actors.WithEffectTTL(cmd.TTL))
				}
				result, err := actors.Effect[string](ctx, cmd.Key, func(inner actors.Ctx) (string, error) {
					return actors.RunActivity(inner, "edge-effect", recordEffectActivity{Key: cmd.Key}, actors.WithActivityStartToClose(5*time.Second))
				}, opts...)
				if err != nil {
					return struct{}{}, err
				}
				st.History = append(st.History, result)
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *effectEdgeState, cmd effectSleepCommand) (struct{}, error) {
				if cmd.Delay > 0 {
					if err := ctx.Sleep(cmd.Delay); err != nil {
						return struct{}{}, err
					}
				}
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *effectEdgeState, cmd effectContinueCommand) (struct{}, error) {
				if strings.TrimSpace(cmd.Key) != "" {
					_ = actors.Tell(ctx, ctx.Self(), effectTTLCommand{Key: cmd.Key})
				}
				return struct{}{}, actors.ContinueAsNew(ctx, effectEdgeInit{History: append([]string(nil), st.History...)})
			}),
			actors.Query(func(ctx actors.Ctx, st effectEdgeState, _ effectEdgeQuery) (effectEdgeState, error) {
				return st, nil
			}),
			stopCommandAction[effectEdgeState](),
		).
		Build()
}

func newPatchEdgeActor() actors.Actor {
	return actors.NewStateful("patch-edge", func() patchState { return patchState{} }).
		DeclarePatch("edge-change", false).
		With(
			actors.Command(func(ctx actors.Ctx, st *patchState, _ patchRecordCommand) (struct{}, error) {
				enabled := actors.Patch(ctx, "edge-change")
				st.Decisions = append(st.Decisions, enabled)
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *patchState, cmd patchSkipCommand) (struct{}, error) {
				enabled := actors.Patch(ctx, "edge-change")
				st.Decisions = append(st.Decisions, enabled)
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st patchState, _ patchStateQuery) (patchState, error) {
				return st, nil
			}),
			stopCommandAction[patchState](),
		).
		Build()
}

var _ log.Logger = (*captureTemporalLogger)(nil)

type captureTemporalLogger struct {
	entries []temporalLogEntry
}

type temporalLogEntry struct {
	level   string
	message string
}

func (c *captureTemporalLogger) Debug(string, ...interface{}) {}
func (c *captureTemporalLogger) Info(string, ...interface{})  {}
func (c *captureTemporalLogger) Warn(string, ...interface{})  {}
func (c *captureTemporalLogger) Error(msg string, _ ...interface{}) {
	c.entries = append(c.entries, temporalLogEntry{level: "error", message: msg})
}

func (c *captureTemporalLogger) hasBackgroundFailure() bool {
	for _, entry := range c.entries {
		if entry.level == "error" && entry.message == "background activity failed" {
			return true
		}
	}
	return false
}

type captureListener struct {
	observability.ListenerAdapter
	commandStarts   []string
	commandFinishes []string
	askStarts       []string
	askFinishes     []string
	errorObserved   bool
}

func (c *captureListener) CommandStart(ctx context.Context, evt observability.CommandEvent) {
	c.commandStarts = append(c.commandStarts, evt.Command)
}

func (c *captureListener) CommandFinish(ctx context.Context, evt observability.CommandEvent, err error, _ time.Duration) {
	c.commandFinishes = append(c.commandFinishes, evt.Command)
	if err != nil {
		c.errorObserved = true
	}
}

func (c *captureListener) AskStart(ctx context.Context, evt observability.AskEvent) {
	c.askStarts = append(c.askStarts, evt.Command)
}

func (c *captureListener) AskFinish(ctx context.Context, evt observability.AskEvent, err error, _ time.Duration) {
	c.askFinishes = append(c.askFinishes, evt.Command)
	if err != nil {
		c.errorObserved = true
	}
}
