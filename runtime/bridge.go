package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tactors/sdk/actors"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const (
	bridgeInvokeCommandActivity = "tactors.bridge.invokeCommand"
	bridgeInvokeAskActivity     = "tactors.bridge.invokeAsk"
	bridgeInvokeQueryActivity   = "tactors.bridge.invokeQuery"
	bridgeSpawnRemoteActivity   = "tactors.bridge.spawnRemote"
)

type bridgeCommandRequest struct {
	Ref             actors.Ref
	Method          string
	Payload         any
	CallerNamespace string
	TargetNamespace string
}

type bridgeAskRequest struct {
	Ref             actors.Ref
	Method          string
	Payload         any
	CorrelationID   string
	CallerNamespace string
	TargetNamespace string
}

type bridgeQueryRequest struct {
	Ref             actors.Ref
	Method          string
	Payload         any
	CallerNamespace string
	TargetNamespace string
}

type bridgeSpawnRequest struct {
	Ref             actors.Ref
	Init            any
	Parent          actors.Ref
	Correlation     actors.CorrelationData
	Timeout         time.Duration
	CallerNamespace string
	TargetNamespace string
}

type bridgeActivities struct {
	routing  *namespaceRouting
	mu       sync.Mutex
	invokers map[string]*temporalClientInvoker
}

func newBridgeActivities(routing *namespaceRouting) *bridgeActivities {
	if routing == nil {
		routing = &namespaceRouting{}
	}
	return &bridgeActivities{
		routing:  routing,
		invokers: make(map[string]*temporalClientInvoker),
	}
}

func (b *bridgeActivities) InvokeCommand(ctx context.Context, req bridgeCommandRequest) error {
	ref, callerNS, targetNS, err := b.normalizeRequest(req.Ref, req.CallerNamespace, req.TargetNamespace)
	if err != nil {
		return err
	}
	if err := b.routing.canCrossNamespace(callerNS, targetNS); err != nil {
		return err
	}
	inv, err := b.invokerFor(targetNS)
	if err != nil {
		return err
	}
	return inv.InvokeCommand(ctx, ref, req.Method, req.Payload)
}

func (b *bridgeActivities) InvokeAsk(ctx context.Context, req bridgeAskRequest) (any, error) {
	ref, callerNS, targetNS, err := b.normalizeRequest(req.Ref, req.CallerNamespace, req.TargetNamespace)
	if err != nil {
		return nil, err
	}
	if err := b.routing.canCrossNamespace(callerNS, targetNS); err != nil {
		return nil, err
	}
	inv, err := b.invokerFor(targetNS)
	if err != nil {
		return nil, err
	}
	var out any
	opts := actors.AskOptions{CorrelationID: req.CorrelationID}
	if err := inv.InvokeAsk(ctx, ref, req.Method, req.Payload, &out, opts); err != nil {
		return nil, err
	}
	return out, nil
}

func (b *bridgeActivities) InvokeQuery(ctx context.Context, req bridgeQueryRequest) (any, error) {
	ref, callerNS, targetNS, err := b.normalizeRequest(req.Ref, req.CallerNamespace, req.TargetNamespace)
	if err != nil {
		return nil, err
	}
	if err := b.routing.canCrossNamespace(callerNS, targetNS); err != nil {
		return nil, err
	}
	inv, err := b.invokerFor(targetNS)
	if err != nil {
		return nil, err
	}
	var out any
	if err := inv.InvokeQuery(ctx, ref, req.Method, req.Payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (b *bridgeActivities) SpawnRemote(ctx context.Context, req bridgeSpawnRequest) error {
	ref, callerNS, targetNS, err := b.normalizeRequest(req.Ref, req.CallerNamespace, req.TargetNamespace)
	if err != nil {
		return err
	}
	if err := b.routing.canCrossNamespace(callerNS, targetNS); err != nil {
		return err
	}
	inv, err := b.invokerFor(targetNS)
	if err != nil {
		return err
	}
	env := startEnvelope{
		Parent:      req.Parent,
		Payload:     req.Init,
		Envelope:    true,
		Correlation: req.Correlation.Clone(),
	}
	ref.StartArgs = []any{ref.ID, env}
	if ref.WorkflowType == "" {
		ref.WorkflowType = ref.Kind
	}
	opts := startWorkflowOptions(ref)
	if req.Timeout > 0 {
		opts.WorkflowRunTimeout = req.Timeout
	}
	workflowType := workflowTypeName(ref)
	args := ref.StartPayload()
	run, err := inv.client.ExecuteWorkflow(ctx, opts, workflowType, args...)
	if err == nil {
		if id := run.GetRunID(); id != "" {
			inv.storeRunID(ref.Workflow, id)
		}
		return nil
	}
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) || isWorkflowExecutionAlreadyStartedServiceError(err) {
		return nil
	}
	return err
}

func (b *bridgeActivities) normalizeRequest(ref actors.Ref, callerNS, targetNS string) (actors.Ref, string, string, error) {
	ref = normalizeWorkflowRef(ref)
	caller := b.routing.effectiveNamespace(callerNS)
	target := strings.TrimSpace(targetNS)
	if target == "" {
		target = b.routing.resolveNamespace(ref)
	}
	target = b.routing.effectiveNamespace(target)
	if target == "" {
		return actors.Ref{}, caller, target, fmt.Errorf("runtime: target namespace is empty")
	}
	ref.Namespace = target
	return ref, caller, target, nil
}

func (b *bridgeActivities) invokerFor(namespace string) (*temporalClientInvoker, error) {
	client, err := b.routing.client(namespace)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if inv := b.invokers[namespace]; inv != nil {
		return inv, nil
	}
	inv := &temporalClientInvoker{client: client}
	b.invokers[namespace] = inv
	return inv, nil
}

func (s *WorkerSet) registerBridgeActivities(worker temporalWorker) {
	if s == nil || worker == nil || s.routing == nil || !s.routing.bridgeEnabled() {
		return
	}
	if s.bridgeRegistered == nil {
		s.bridgeRegistered = make(map[temporalWorker]struct{})
	}
	if _, ok := s.bridgeRegistered[worker]; ok {
		return
	}
	bridge := newBridgeActivities(s.routing)
	worker.RegisterActivityWithOptions(bridge.InvokeCommand, activity.RegisterOptions{Name: bridgeInvokeCommandActivity})
	worker.RegisterActivityWithOptions(bridge.InvokeAsk, activity.RegisterOptions{Name: bridgeInvokeAskActivity})
	worker.RegisterActivityWithOptions(bridge.InvokeQuery, activity.RegisterOptions{Name: bridgeInvokeQueryActivity})
	worker.RegisterActivityWithOptions(bridge.SpawnRemote, activity.RegisterOptions{Name: bridgeSpawnRemoteActivity})
	s.bridgeRegistered[worker] = struct{}{}
}

func normalizeWorkflowRef(ref actors.Ref) actors.Ref {
	if strings.TrimSpace(ref.Workflow) == "" {
		ref.Workflow = strings.TrimSpace(ref.ID)
	}
	return ref
}
