package runtime

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"github.com/tactors/sdk/observability"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestBuildSnapshotDrainsSignals(t *testing.T) {
	desc := buildTestDescription()
	doubleName := commandNameOf[doubleReq]()
	rawName := commandNameOf[rawReq]()
	doubleSpec := desc.Commands[doubleName]
	doubleSpec.DecodePayload = func(val any) (any, error) {
		switch typed := val.(type) {
		case *doubleReq:
			typed.Value *= 2
			return *typed, nil
		case doubleReq:
			typed.Value *= 2
			return typed, nil
		default:
			return nil, fmt.Errorf("unexpected payload %T", val)
		}
	}
	desc.Commands[doubleName] = doubleSpec
	rawSpec := desc.Commands[rawName]
	rawSpec.PayloadFactory = nil
	desc.Commands[rawName] = rawSpec
	inst := newTemporalInstance(desc)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(doubleName, doubleReq{Value: 3})
		env.SignalWorkflow(doubleName, doubleReq{Value: 5})
		env.SignalWorkflow(rawName, "foo")
	}, time.Millisecond)
	state := &struct{ Value int }{Value: 42}
	wf := func(ctx workflow.Context) (snapshotRecord, error) {
		chans := map[string]workflow.ReceiveChannel{
			doubleName: workflow.GetSignalChannel(ctx, doubleName),
			rawName:    workflow.GetSignalChannel(ctx, rawName),
		}
		err := workflow.Await(ctx, func() bool {
			return totalPendingSignals(chans) == 3
		})
		if err != nil {
			return snapshotRecord{}, err
		}
		return inst.buildSnapshot(ctx, chans, state)
	}
	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var record snapshotRecord
	require.NoError(t, env.GetWorkflowResult(&record))
	require.Len(t, record.Signals, 3)

	var restoredState struct{ Value int }
	require.NoError(t, codec.Unmarshal(record.State, &restoredState))
	require.Equal(t, state.Value, restoredState.Value)

	collected := map[string][]any{}
	for _, sig := range record.Signals {
		switch sig.Name {
		case doubleName:
			var payload doubleReq
			require.NoError(t, codec.Unmarshal(sig.Payload, &payload))
			collected[sig.Name] = append(collected[sig.Name], payload)
		case rawName:
			var payload any
			require.NoError(t, codec.Unmarshal(sig.Payload, &payload))
			collected[sig.Name] = append(collected[sig.Name], payload)
		default:
			t.Fatalf("unexpected signal %s", sig.Name)
		}
	}
	require.Equal(t, []any{doubleReq{Value: 6}, doubleReq{Value: 10}}, collected[doubleName])
	require.Equal(t, []any{"foo"}, collected[rawName])
}

func TestBuildSnapshotNoPendingSignals(t *testing.T) {
	desc := buildTestDescription()
	inst := newTemporalInstance(desc)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	wf := func(ctx workflow.Context) (snapshotRecord, error) {
		return inst.buildSnapshot(ctx, map[string]workflow.ReceiveChannel{}, &testCommandState{})
	}
	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var record snapshotRecord
	require.NoError(t, env.GetWorkflowResult(&record))
	require.Empty(t, record.Signals)
}

func TestBuildSnapshotLargeBacklog(t *testing.T) {
	desc := buildTestDescription()
	inst := newTemporalInstance(desc)
	doubleName := commandNameOf[doubleReq]()
	want := 64
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterDelayedCallback(func() {
		for i := 0; i < want; i++ {
			env.SignalWorkflow(doubleName, doubleReq{Value: i})
		}
	}, time.Millisecond)
	wf := func(ctx workflow.Context) (snapshotRecord, error) {
		chans := map[string]workflow.ReceiveChannel{
			doubleName: workflow.GetSignalChannel(ctx, doubleName),
		}
		err := workflow.Await(ctx, func() bool {
			return totalPendingSignals(chans) == want
		})
		if err != nil {
			return snapshotRecord{}, err
		}
		return inst.buildSnapshot(ctx, chans, &testCommandState{})
	}
	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var record snapshotRecord
	require.NoError(t, env.GetWorkflowResult(&record))
	require.Len(t, record.Signals, want)
	for i, sig := range record.Signals {
		require.Equal(t, doubleName, sig.Name)
		var payload doubleReq
		require.NoError(t, codec.Unmarshal(sig.Payload, &payload))
		require.Equal(t, i, payload.Value)
	}
}

func totalPendingSignals(chans map[string]workflow.ReceiveChannel) int {
	total := 0
	for _, ch := range chans {
		total += ch.Len()
	}
	return total
}

func TestReplaySnapshotSignalsInvokesHandlers(t *testing.T) {
	desc := buildTestDescription()
	doubleName := commandNameOf[doubleReq]()
	rawName := commandNameOf[rawReq]()
	inst := newTemporalInstance(desc)
	signals := []snapshotSignal{
		{Name: doubleName, Payload: mustMarshal(t, doubleReq{Value: 2})},
		{Name: rawName, Payload: mustMarshal(t, rawReq{Value: 5})},
	}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	wf := func(ctx workflow.Context) ([]int, error) {
		wfCtx := &wfContext{
			workflowCtx:      ctx,
			ref:              actors.Ref{Kind: desc.Kind, ID: "wf"},
			activityDecoders: desc.ActivityDecoders(),
			activityQueue:    activityQueueFor(desc.Kind, desc),
			tracer:           observability.ActiveTracer(),
		}
		state := &testCommandState{}
		if err := inst.replaySnapshotSignals(ctx, wfCtx, state, signals); err != nil {
			return nil, err
		}
		return append([]int(nil), state.Values...), nil
	}
	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var values []int
	require.NoError(t, env.GetWorkflowResult(&values))
	require.Equal(t, []int{2, 5}, values)
}

func TestReplaySnapshotSignalsPreservesOrder(t *testing.T) {
	desc := buildTestDescription()
	inst := newTemporalInstance(desc)
	doubleName := commandNameOf[doubleReq]()
	const total = 32
	signals := make([]snapshotSignal, 0, total)
	for i := 0; i < total; i++ {
		signals = append(signals, snapshotSignal{Name: doubleName, Payload: mustMarshal(t, doubleReq{Value: i})})
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	wf := func(ctx workflow.Context) ([]int, error) {
		wfCtx := &wfContext{
			workflowCtx:      ctx,
			ref:              actors.Ref{Kind: desc.Kind, ID: "wf"},
			activityDecoders: desc.ActivityDecoders(),
			activityQueue:    activityQueueFor(desc.Kind, desc),
			tracer:           observability.ActiveTracer(),
		}
		state := &testCommandState{}
		if err := inst.replaySnapshotSignals(ctx, wfCtx, state, signals); err != nil {
			return nil, err
		}
		return append([]int(nil), state.Values...), nil
	}
	env.ExecuteWorkflow(wf)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var values []int
	require.NoError(t, env.GetWorkflowResult(&values))
	require.Len(t, values, total)
	for i := 0; i < total; i++ {
		require.Equal(t, i, values[i])
	}
}

type testCommandState struct {
	Sum    int
	Values []int
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	data, err := codec.Marshal(v)
	require.NoError(t, err)
	return data
}

func buildTestDescription() *actors.Description {
	builder := actors.NewStateful[testCommandState]("counter", func() testCommandState {
		return testCommandState{}
	}).WithSnapshot(actors.SnapshotConfig[testCommandState]{
		Every: 1,
		ContinueArgs: func(state testCommandState) (any, error) {
			return map[string]int{"sum": state.Sum}, nil
		},
	})
	builder = builder.With(
		actors.Command[testCommandState, doubleReq, struct{}](func(ctx actors.Ctx, state *testCommandState, req doubleReq) (struct{}, error) {
			state.Sum += req.Value
			state.Values = append(state.Values, req.Value)
			return struct{}{}, nil
		}),
		actors.Command[testCommandState, rawReq, struct{}](func(ctx actors.Ctx, state *testCommandState, req rawReq) (struct{}, error) {
			state.Sum += req.Value
			state.Values = append(state.Values, req.Value)
			return struct{}{}, nil
		}),
	)
	actor := builder.Build()
	return actor.Spec()
}

type doubleReq struct {
	Value int
}

type rawReq struct {
	Value int
}

func commandNameOf[T any]() string {
	var zero T
	return fmt.Sprintf("%T", zero)
}

func TestSnapshotAndContinueUsesOverride(t *testing.T) {
	desc := buildTestDescription()
	inst := newTemporalInstance(desc)
	state := &testCommandState{Sum: 7}
	override := map[string]int{"sum": 42}
	capturedArgs, err := runSnapshotAndContinueWorkflow(t, inst, desc, state, override)
	require.Error(t, err)
	var contErr *workflow.ContinueAsNewError
	require.True(t, errors.As(err, &contErr))
	require.Equal(t, override, capturedArgs)
}

func TestSnapshotAndContinueUsesSnapshotArgsWhenNoOverride(t *testing.T) {
	desc := buildTestDescription()
	inst := newTemporalInstance(desc)
	state := &testCommandState{Sum: 9}
	capturedArgs, err := runSnapshotAndContinueWorkflow(t, inst, desc, state, nil)
	require.Error(t, err)
	var contErr *workflow.ContinueAsNewError
	require.True(t, errors.As(err, &contErr))
	require.Equal(t, map[string]int{"sum": state.Sum}, capturedArgs)
}

func TestSnapshotAndContinueFallsBackToInitialPayload(t *testing.T) {
	desc := buildTestDescription()
	desc.SnapshotArgs = nil
	inst := newTemporalInstance(desc)
	state := &testCommandState{Sum: 3}
	inst.initialPayload = map[string]int{"sum": -1}
	capturedArgs, err := runSnapshotAndContinueWorkflow(t, inst, desc, state, nil)
	require.Error(t, err)
	var contErr *workflow.ContinueAsNewError
	require.True(t, errors.As(err, &contErr))
	require.Equal(t, inst.initialPayload, capturedArgs)
}

func runSnapshotAndContinueWorkflow(t *testing.T, inst *temporalInstance, desc *actors.Description, state *testCommandState, override any) (any, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var capturedArgs any
	var wfErr error
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		wfCtx := &wfContext{
			workflowCtx:      ctx,
			ref:              actors.Ref{Kind: desc.Kind, ID: "wf"},
			activityDecoders: desc.ActivityDecoders(),
			activityQueue:    activityQueueFor(desc.Kind, desc),
			tracer:           observability.ActiveTracer(),
		}
		var err error
		capturedArgs, err = inst.snapshotAndContinue(ctx, wfCtx, state, map[string]workflow.ReceiveChannel{}, override)
		wfErr = err
		return err
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, wfErr)
	return capturedArgs, wfErr
}

func TestRestoreSnapshotMemoDecodeError(t *testing.T) {
	desc := buildTestDescription()
	inst := newTemporalInstance(desc)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	require.NoError(t, env.SetMemoOnStart(map[string]any{
		snapshotMemoKey: "corrupted",
	}))
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := inst.restoreSnapshot(ctx, &testCommandState{})
		return err
	})
	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshotRecord")
}
