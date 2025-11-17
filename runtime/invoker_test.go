package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

func TestInvokeCommandSignalWithStart(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	ctx := context.Background()
	ref := actors.ARef("kind", "id")
	require.NoError(t, inv.InvokeCommand(ctx, ref, "foo", "payload"))
	require.Len(t, fake.signalWithStartCalls, 1)
	require.Empty(t, fake.signalCalls)
	call := fake.signalWithStartCalls[0]
	require.Equal(t, "id", call.workflowID)
	require.Equal(t, "foo", call.signalName)
	require.Equal(t, client.StartWorkflowOptions{
		ID:        "id",
		TaskQueue: "kind",
	}, call.options)
	require.Equal(t, "kind", call.workflowType)
	require.Equal(t, []interface{}{"id"}, call.workflowArgs)
	require.Equal(t, "payload", call.arg)
}

func TestInvokeCommandRunScopedSignal(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	ctx := context.Background()
	ref := actors.ARef("kind", "id", actors.WithRunID("run-id"))
	require.NoError(t, inv.InvokeCommand(ctx, ref, "foo", "payload"))
	require.Len(t, fake.signalCalls, 1)
	require.Empty(t, fake.signalWithStartCalls)
	call := fake.signalCalls[0]
	require.Equal(t, "id", call.workflowID)
	require.Equal(t, "run-id", call.runID)
	require.Equal(t, "foo", call.signalName)
	require.Equal(t, "payload", call.arg)
}

func TestInvokeCommandUsesCachedRunID(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	inv.storeRunID("id", "cached-run")
	ctx := context.Background()
	ref := actors.ARef("kind", "id")
	require.NoError(t, inv.InvokeCommand(ctx, ref, "foo", "payload"))
	require.Len(t, fake.signalCalls, 1)
	require.Equal(t, "cached-run", fake.signalCalls[0].runID)
	require.Empty(t, fake.signalWithStartCalls)
}

func TestInvokeCommandCachesRunIDFromSignalWithStart(t *testing.T) {
	fake := &fakeTemporalClient{signalWithStartRunID: "new-run"}
	inv := &temporalClientInvoker{client: fake}
	ctx := context.Background()
	ref := actors.ARef("kind", "id")
	require.NoError(t, inv.InvokeCommand(ctx, ref, "foo", "payload"))
	require.Equal(t, "new-run", inv.cachedRunID("id"))
}

func TestHandleUpdateErrorNotFound(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	ctx := context.Background()
	options := client.UpdateWorkflowOptions{}
	retry, err := inv.handleUpdateError(ctx, actors.Ref{Workflow: "wf", Kind: "kind"}, &options, &serviceerror.NotFound{})
	require.True(t, retry)
	require.NoError(t, err)
	require.Equal(t, 1, fake.executeCount)
}

func TestHandleUpdateErrorUnknownExternal(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	ctx := context.Background()
	options := client.UpdateWorkflowOptions{}
	retry, err := inv.handleUpdateError(ctx, actors.Ref{Workflow: "wf", Kind: "kind"}, &options, &temporal.UnknownExternalWorkflowExecutionError{})
	require.True(t, retry)
	require.NoError(t, err)
	require.Equal(t, 1, fake.executeCount)
}

func TestHandleUpdateErrorContinueAsNew(t *testing.T) {
	fake := &fakeTemporalClient{
		describeResp: &workflowservicepb.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{RunId: "new-run"},
			},
		},
	}
	inv := &temporalClientInvoker{client: fake}
	ctx := context.Background()
	options := client.UpdateWorkflowOptions{}
	retry, err := inv.handleUpdateError(ctx, actors.Ref{Workflow: "wf", Kind: "kind"}, &options, &serviceerror.WorkflowNotReady{})
	require.True(t, retry)
	require.NoError(t, err)
	require.Equal(t, "new-run", options.RunID)
	require.Equal(t, "new-run", inv.cachedRunID("wf"))
}

func TestHandleUpdateErrorNonRetryable(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	ctx := context.Background()
	options := client.UpdateWorkflowOptions{}
	baseErr := errors.New("boom")
	retry, err := inv.handleUpdateError(ctx, actors.Ref{Workflow: "wf", Kind: "kind"}, &options, baseErr)
	require.False(t, retry)
	require.Equal(t, baseErr, err)
}

func TestInvokeAskUsesCachedRunID(t *testing.T) {
	fake := &fakeTemporalClient{}
	inv := &temporalClientInvoker{client: fake}
	inv.storeRunID("wf", "cached-run")
	ctx := context.Background()
	ref := actors.ARef("kind", "wf")
	opts := actors.AskOptions{CorrelationID: "corr"}
	require.NoError(t, inv.InvokeAsk(ctx, ref, "ask-method", nil, nil, opts))
	require.Len(t, fake.updateCalls, 1)
	require.Equal(t, "cached-run", fake.updateCalls[0].RunID)
}

type fakeTemporalClient struct {
	executeCount         int
	describeResp         *workflowservicepb.DescribeWorkflowExecutionResponse
	signalCalls          []signalCall
	signalWithStartCalls []signalWithStartCall
	updateCalls          []client.UpdateWorkflowOptions
	signalWithStartRunID string
	executeRunID         string
	updateRunID          string
}

type signalCall struct {
	workflowID string
	runID      string
	signalName string
	arg        interface{}
}

type signalWithStartCall struct {
	workflowID   string
	signalName   string
	arg          interface{}
	options      client.StartWorkflowOptions
	workflowType interface{}
	workflowArgs []interface{}
}

func (f *fakeTemporalClient) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error {
	f.signalCalls = append(f.signalCalls, signalCall{
		workflowID: workflowID,
		runID:      runID,
		signalName: signalName,
		arg:        arg,
	})
	return nil
}

func (f *fakeTemporalClient) SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, arg interface{}, options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	f.signalWithStartCalls = append(f.signalWithStartCalls, signalWithStartCall{
		workflowID:   workflowID,
		signalName:   signalName,
		arg:          arg,
		options:      options,
		workflowType: workflow,
		workflowArgs: append([]interface{}(nil), workflowArgs...),
	})
	runID := f.signalWithStartRunID
	if runID == "" {
		runID = "run-" + workflowID
	}
	return fakeWorkflowRun{id: options.ID, runID: runID}, nil
}

func (f *fakeTemporalClient) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	return nil, nil
}

func (f *fakeTemporalClient) UpdateWorkflow(ctx context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	f.updateCalls = append(f.updateCalls, options)
	runID := options.RunID
	if runID == "" {
		runID = f.updateRunID
	}
	if runID == "" {
		runID = "run-" + options.WorkflowID
	}
	return fakeUpdateHandle{workflowID: options.WorkflowID, runID: runID}, nil
}

func (f *fakeTemporalClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservicepb.DescribeWorkflowExecutionResponse, error) {
	if f.describeResp != nil {
		return f.describeResp, nil
	}
	return &workflowservicepb.DescribeWorkflowExecutionResponse{}, nil
}

func (f *fakeTemporalClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.executeCount++
	runID := f.executeRunID
	if runID == "" {
		runID = "run-" + options.ID
	}
	return fakeWorkflowRun{id: options.ID, runID: runID}, nil
}

type fakeWorkflowRun struct {
	id    string
	runID string
}

func (f fakeWorkflowRun) GetID() string {
	return f.id
}

func (f fakeWorkflowRun) GetRunID() string {
	return f.runID
}

func (f fakeWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return nil
}

func (f fakeWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options client.WorkflowRunGetOptions) error {
	return nil
}

type fakeUpdateHandle struct {
	workflowID string
	runID      string
}

func (f fakeUpdateHandle) WorkflowID() string {
	return f.workflowID
}

func (f fakeUpdateHandle) RunID() string {
	return f.runID
}

func (f fakeUpdateHandle) UpdateID() string {
	return ""
}

func (f fakeUpdateHandle) Get(ctx context.Context, valuePtr interface{}) error {
	return nil
}
