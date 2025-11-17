package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"github.com/tactors/sdk/internal/rand"
	"go.temporal.io/api/serviceerror"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

// TemporalClient describes the subset of the Temporal Go SDK client needed to
// implement actor client proxies.
type TemporalClient interface {
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, arg interface{}, options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error)
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (converter.EncodedValue, error)
	UpdateWorkflow(ctx context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservicepb.DescribeWorkflowExecutionResponse, error)
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
}

// RegisterTemporalClient wires a Temporal client into the actors.Client invoker pipeline.
func RegisterTemporalClient(cli TemporalClient) {
	if cli == nil {
		return
	}
	actors.RegisterClientInvoker(func(ref actors.Ref) actors.ClientInvoker {
		return &temporalClientInvoker{client: cli}
	})
}

type temporalClientInvoker struct {
	client     TemporalClient
	runIDCache sync.Map
}

func (t *temporalClientInvoker) InvokeCommand(ctx context.Context, ref actors.Ref, method string, payload any) error {
	if t.client == nil {
		return errors.New("actors: temporal client not registered")
	}
	if ref.Workflow == "" {
		return errors.New("actors: command target workflow id is empty")
	}
	runID := ref.RunID
	if runID == "" {
		runID = t.cachedRunID(ref.Workflow)
	}
	if runID != "" {
		err := t.client.SignalWorkflow(ctx, ref.Workflow, runID, method, payload)
		if err == nil {
			return nil
		}
		if isStaleRunError(err) {
			t.clearRunID(ref.Workflow)
		} else {
			return err
		}
	}
	opts := startWorkflowOptions(ref)
	workflowType := workflowTypeName(ref)
	args := ref.StartPayload()
	run, err := t.client.SignalWithStartWorkflow(ctx, ref.Workflow, method, payload, opts, workflowType, args...)
	if err != nil {
		return err
	}
	if id := run.GetRunID(); id != "" {
		t.storeRunID(ref.Workflow, id)
	}
	return nil
}

func (t *temporalClientInvoker) InvokeQuery(ctx context.Context, ref actors.Ref, method string, payload any, resp any) error {
	if t.client == nil {
		return errors.New("actors: temporal client not registered")
	}
	var args []interface{}
	if payload != nil {
		blob, err := codec.Marshal(payload)
		if err != nil {
			return err
		}
		args = append(args, blob)
	}
	result, err := t.client.QueryWorkflow(ctx, ref.Workflow, "", method, args...)
	if err != nil {
		return err
	}
	if resp == nil || !result.HasValue() {
		return nil
	}
	return result.Get(resp)
}

func (t *temporalClientInvoker) InvokeAsk(ctx context.Context, ref actors.Ref, method string, payload any, resp any, opts actors.AskOptions) error {
	if t.client == nil {
		return errors.New("actors: temporal client not registered")
	}
	if ref.Workflow == "" {
		return errors.New("actors: ask target workflow id is empty")
	}
	options := client.UpdateWorkflowOptions{
		WorkflowID:   ref.Workflow,
		UpdateName:   method,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	}
	if ref.RunID != "" {
		options.RunID = ref.RunID
	} else if cached := t.cachedRunID(ref.Workflow); cached != "" {
		options.RunID = cached
	}
	if payload != nil {
		blob, err := codec.Marshal(payload)
		if err != nil {
			return err
		}
		options.Args = []interface{}{blob}
	}
	if opts.CorrelationID == "" {
		opts.CorrelationID = rand.ID("ask")
	}
	options.UpdateID = opts.CorrelationID
	handle, err := t.client.UpdateWorkflow(ctx, options)
	if err != nil {
		retry, handledErr := t.handleUpdateError(ctx, ref, &options, err)
		if handledErr != nil {
			return handledErr
		}
		if !retry {
			return err
		}
		handle, err = t.client.UpdateWorkflow(ctx, options)
		if err != nil {
			return err
		}
	}
	if runID := handle.RunID(); runID != "" {
		t.storeRunID(ref.Workflow, runID)
	}
	return handle.Get(ctx, resp)
}

func (t *temporalClientInvoker) handleUpdateError(ctx context.Context, ref actors.Ref, options *client.UpdateWorkflowOptions, err error) (bool, error) {
	switch err.(type) {
	case *serviceerror.NotFound, *temporal.UnknownExternalWorkflowExecutionError:
		if startErr := t.startWorkflow(ctx, ref); startErr != nil {
			return false, startErr
		}
		return true, nil
	case *serviceerror.WorkflowNotReady:
		if t.client == nil {
			return false, err
		}
		resp, describeErr := t.client.DescribeWorkflowExecution(ctx, ref.Workflow, ref.RunID)
		if describeErr != nil {
			return false, describeErr
		}
		if info := resp.GetWorkflowExecutionInfo(); info != nil && info.Execution != nil {
			options.RunID = info.Execution.RunId
			t.storeRunID(ref.Workflow, options.RunID)
		}
		return true, nil
	default:
		return false, err
	}
}

func startWorkflowOptions(ref actors.Ref) client.StartWorkflowOptions {
	queue := ref.TaskQueue
	if queue == "" {
		queue = ref.Kind
	}
	return client.StartWorkflowOptions{
		ID:        ref.Workflow,
		TaskQueue: queue,
	}
}

func (t *temporalClientInvoker) startWorkflow(ctx context.Context, ref actors.Ref) error {
	if t.client == nil {
		return errors.New("actors: temporal client not registered")
	}
	if ref.Workflow == "" {
		return errors.New("actors: workflow id is empty")
	}
	opts := startWorkflowOptions(ref)
	workflowType := workflowTypeName(ref)
	args := ref.StartPayload()
	run, err := t.client.ExecuteWorkflow(ctx, opts, workflowType, args...)
	if err == nil {
		if id := run.GetRunID(); id != "" {
			t.storeRunID(ref.Workflow, id)
		}
		return nil
	}
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) || isWorkflowExecutionAlreadyStartedServiceError(err) {
		return nil
	}
	return err
}

func isWorkflowExecutionAlreadyStartedServiceError(err error) bool {
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	return errors.As(err, &already)
}

func workflowTypeName(ref actors.Ref) string {
	if ref.WorkflowType != "" {
		return ref.WorkflowType
	}
	return ref.Kind
}

func (t *temporalClientInvoker) cachedRunID(workflowID string) string {
	if workflowID == "" {
		return ""
	}
	if v, ok := t.runIDCache.Load(workflowID); ok {
		if id, _ := v.(string); id != "" {
			return id
		}
	}
	return ""
}

func (t *temporalClientInvoker) storeRunID(workflowID, runID string) {
	if workflowID == "" || runID == "" {
		return
	}
	t.runIDCache.Store(workflowID, runID)
}

func (t *temporalClientInvoker) clearRunID(workflowID string) {
	if workflowID == "" {
		return
	}
	t.runIDCache.Delete(workflowID)
}

func isStaleRunError(err error) bool {
	switch err.(type) {
	case *serviceerror.NotFound, *temporal.UnknownExternalWorkflowExecutionError:
		return true
	default:
		return false
	}
}
