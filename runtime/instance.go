package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"github.com/tactors/sdk/observability"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type startEnvelope struct {
	Parent      actors.Ref
	Payload     any
	OneShot     *oneShotCommand
	Correlation actors.CorrelationData
	Envelope    bool
}

type oneShotCommand struct {
	Name    string
	Payload any
}

const (
	queryRequestSignal    = "__actors_query_request"
	queryReplySignal      = "__actors_query_reply"
	askRequestSignal      = "__actors_ask_request"
	askReplySignal        = "__actors_ask_reply"
	tellRequestSignal     = "__actors_tell_request"
	continueRequestSignal = "__actors_continue_request"
	continueReplySignal   = "__actors_continue_reply"
)

type queryRequest struct {
	ID            string
	Query         string
	Payload       any
	ReplyWorkflow string
	ReplyRunID    string
	ReplySignal   string
	Envelope      actors.MessageMetadata
}

type queryReply struct {
	ID      string
	Payload any
	Error   string
}

type askRequest struct {
	ID            string
	Command       string
	Payload       any
	ReplyWorkflow string
	ReplyRunID    string
	ReplySignal   string
	Envelope      actors.MessageMetadata
}

type askReply struct {
	ID      string
	Payload any
	Error   string
}

type tellRequest struct {
	Command  string
	Payload  any
	Envelope actors.MessageMetadata
}

type continueRequest struct {
	ID            string
	Init          any
	ReplyWorkflow string
	ReplyRunID    string
	ReplySignal   string
	Envelope      actors.MessageMetadata
}

type continueReply struct {
	ID    string
	Error string
	Init  any
}

type temporalInstance struct {
	desc                 *actors.Description
	processedSinceRotate int
	initialPayload       any
}

func newTemporalInstance(desc *actors.Description) *temporalInstance {
	return &temporalInstance{desc: desc.Clone()}
}

func (i *temporalInstance) run(ctx workflow.Context, id string, init any) (any, error) {
	wfCtx, state, oneShotReq, err := i.prepareWorkflow(ctx, id, init)
	if err != nil {
		return nil, err
	}
	if oneShotReq != nil {
		return i.executeOneShot(wfCtx, state, oneShotReq)
	}
	if len(i.desc.Commands) == 0 {
		return nil, nil
	}
	return i.driveCommandLoop(ctx, wfCtx, state)
}

func (i *temporalInstance) prepareWorkflow(ctx workflow.Context, id string, init any) (*wfContext, any, *oneShotCommand, error) {
	wfCtx, state, oneShotReq, err := i.buildWorkflowContext(ctx, id, init)
	if err != nil {
		return nil, nil, nil, err
	}
	record, err := i.restoreSnapshot(ctx, state)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(record.Signals) > 0 {
		if err := i.replaySnapshotSignals(ctx, wfCtx, state, record.Signals); err != nil {
			return nil, nil, nil, err
		}
	}
	wfCtx.restoreSnapshotStats(record.Stats)
	if err := i.registerQueries(ctx, wfCtx, state); err != nil {
		return nil, nil, nil, err
	}
	if err := i.registerUpdates(ctx, wfCtx, state); err != nil {
		return nil, nil, nil, err
	}
	if err := i.registerDiagnostics(ctx, wfCtx); err != nil {
		return nil, nil, nil, err
	}
	i.startSignalHandlers(ctx, wfCtx, state)
	return wfCtx, state, oneShotReq, nil
}

func (i *temporalInstance) buildWorkflowContext(ctx workflow.Context, id string, init any) (*wfContext, any, *oneShotCommand, error) {
	execInfo := workflow.GetInfo(ctx)
	ref := actors.Ref{
		Kind:     i.desc.Kind,
		ID:       id,
		Workflow: execInfo.WorkflowExecution.ID,
		RunID:    execInfo.WorkflowExecution.RunID,
	}
	var parent actors.Ref
	var initCorr actors.CorrelationData
	var oneShotReq *oneShotCommand
	if env, ok := extractStartEnvelope(init); ok {
		parent = env.Parent
		init = env.Payload
		oneShotReq = env.OneShot
		initCorr = env.Correlation
	}
	i.initialPayload = init
	wfCtx := &wfContext{
		workflowCtx:      ctx,
		ref:              ref,
		parent:           parent,
		activityDecoders: i.desc.ActivityDecoders(),
		activityQueue:    activityQueueFor(i.desc.Kind, i.desc),
		tracer:           observability.ActiveTracer(),
	}
	if i.desc.SnapshotEvery > 0 {
		wfCtx.snapshotInfo.Enabled = true
		wfCtx.snapshotInfo.SnapshotEvery = i.desc.SnapshotEvery
	}
	wfCtx.correlation = initCorr
	wfCtx.messageMeta = actors.MessageMetadata{Correlation: initCorr}
	var state any
	if i.desc.StateFactory != nil {
		state = i.desc.StateFactory()
	}
	start, err := i.desc.Start.Invoke(wfCtx, init)
	if err != nil {
		return nil, nil, nil, err
	}
	if start != nil {
		state = start
	}
	return wfCtx, state, oneShotReq, nil
}

func (i *temporalInstance) driveCommandLoop(ctx workflow.Context, wfCtx *wfContext, state any) (any, error) {
	chans := make(map[string]workflow.ReceiveChannel, len(i.desc.Commands))
	for name := range i.desc.Commands {
		chans[name] = workflow.GetSignalChannel(ctx, name)
	}
	continueCh := workflow.GetSignalChannel(ctx, continueRequestSignal)
	logger := workflow.GetLogger(ctx)
	var exitErr error
	selector := workflow.NewSelector(ctx)
	for name, ch := range chans {
		spec := i.desc.Commands[name]
		name := name
		ch := ch
		selector.AddReceive(ch, i.commandReceiveHandler(ctx, wfCtx, state, spec, name, logger, chans, &exitErr))
	}
	selector.AddReceive(continueCh, i.continueReceiveHandler(ctx, wfCtx, state, chans, &exitErr))
	selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {
		if exitErr == nil {
			exitErr = temporal.NewCanceledError("actors: workflow canceled")
		}
		wfCtx.requestStop()
	})
	for {
		if wfCtx.stopRequested() {
			return nil, nil
		}
		selector.Select(ctx)
		if exitErr != nil {
			return nil, exitErr
		}
		info := workflow.GetInfo(ctx)
		if info != nil && info.GetContinueAsNewSuggested() {
			if i.desc.SnapshotArgs == nil {
				logger.Warn("temporal: continue-as-new suggested but snapshot not configured")
			} else {
				if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
					return nil, err
				}
				logger.Info("temporal: continue-as-new suggested by server")
				if _, err := i.snapshotAndContinue(ctx, wfCtx, state, chans, nil); err != nil {
					return nil, err
				}
			}
		}
		if wfCtx.stopRequested() {
			return nil, nil
		}
	}
}

func (i *temporalInstance) commandReceiveHandler(
	ctx workflow.Context,
	wfCtx *wfContext,
	state any,
	spec actors.CommandSpec,
	name string,
	logger log.Logger,
	chans map[string]workflow.ReceiveChannel,
	exitErr *error,
) func(workflow.ReceiveChannel, bool) {
	return func(c workflow.ReceiveChannel, more bool) {
		payload, err := receiveCommandPayload(ctx, c, spec)
		if err != nil {
			logger.Error("decode command payload", "command", name, "error", err)
			return
		}
		meta := actors.MessageMetadata{}
		if err := func() error {
			_, err := i.handleCommand(ctx, wfCtx, state, spec, payload, logger, name, meta)
			return err
		}(); err != nil {
			if workflow.IsContinueAsNewError(err) {
				*exitErr = err
				wfCtx.requestStop()
				return
			}
			if inner, ok := actors.AsBusinessError(err); ok {
				logger.Info("business error", "command", name, "error", inner)
				return
			}
			if errors.Is(err, errMessageDeadlineExceeded) || errors.Is(err, errMessageRetryBudgetExceeded) {
				logger.Info("command discarded", "command", name, "error", err)
				return
			}
			if actors.IsNonRetryable(err) {
				logger.Warn("command non-retryable", "command", name, "error", err)
				return
			}
			if errors.Is(err, actors.ErrStopLoop) {
				wfCtx.requestStop()
			} else {
				logger.Error("command failed", "command", name, "error", err)
				*exitErr = err
				wfCtx.requestStop()
			}
			return
		}
		wfCtx.snapshotInfo.CommandsSinceSnapshot++
		if i.desc.SnapshotArgs != nil && i.desc.SnapshotEvery > 0 {
			i.processedSinceRotate++
			if i.processedSinceRotate >= i.desc.SnapshotEvery {
				if _, err := i.snapshotAndContinue(ctx, wfCtx, state, chans, nil); err != nil {
					*exitErr = err
				}
				wfCtx.requestStop()
				return
			}
		}
	}
}

func (i *temporalInstance) continueReceiveHandler(
	ctx workflow.Context,
	wfCtx *wfContext,
	state any,
	chans map[string]workflow.ReceiveChannel,
	exitErr *error,
) func(workflow.ReceiveChannel, bool) {
	return func(rc workflow.ReceiveChannel, more bool) {
		var req continueRequest
		rc.Receive(ctx, &req)
		value, err := wfCtx.withMessageMetadata(req.Envelope, func() (any, error) {
			return i.snapshotAndContinue(ctx, wfCtx, state, chans, req.Init)
		})
		initArgs := value
		reply := continueReply{ID: req.ID, Init: initArgs}
		if err != nil && !workflow.IsContinueAsNewError(err) {
			reply.Error = err.Error()
		}
		i.sendContinueReply(ctx, req, reply)
		if err != nil && workflow.IsContinueAsNewError(err) {
			*exitErr = err
			wfCtx.requestStop()
			return
		}
	}
}

func receiveCommandPayload(ctx workflow.Context, ch workflow.ReceiveChannel, spec actors.CommandSpec) (any, error) {
	if spec.PayloadFactory != nil {
		holder := spec.PayloadFactory()
		ch.Receive(ctx, holder)
		if spec.DecodePayload != nil {
			return spec.DecodePayload(holder)
		}
		return holder, nil
	}
	var payload any
	ch.Receive(ctx, &payload)
	return payload, nil
}

func decodePayloadValue(payload any, decode func(any) (any, error)) (any, error) {
	if decode == nil {
		return payload, nil
	}
	return decode(payload)
}

func (i *temporalInstance) prepareCommandRequest(name string, payload any) (actors.CommandSpec, any, bool, error) {
	spec, ok := i.desc.Commands[name]
	if !ok {
		return actors.CommandSpec{}, nil, false, nil
	}
	decoded, err := decodePayloadValue(payload, spec.DecodePayload)
	if err != nil {
		return actors.CommandSpec{}, nil, true, err
	}
	return spec, decoded, true, nil
}

func (i *temporalInstance) registerQueries(ctx workflow.Context, wfCtx *wfContext, state any) error {
	for name, spec := range i.desc.Queries {
		spec := spec
		name := name
		if err := workflow.SetQueryHandler(ctx, name, func(raw []byte) (any, error) {
			var payload any
			if spec.PayloadFactory != nil {
				holder := spec.PayloadFactory()
				if len(raw) > 0 {
					if err := codec.Unmarshal(raw, holder); err != nil {
						return nil, err
					}
				}
				if spec.DecodePayload != nil {
					decoded, err := spec.DecodePayload(holder)
					if err != nil {
						return nil, err
					}
					payload = decoded
				} else {
					payload = holder
				}
			}
			cacheKey := ""
			if spec.CacheTTL > 0 {
				cacheKey = queryCacheKey(raw)
				if value, ok := wfCtx.cachedQuery(name, cacheKey, workflow.Now(ctx)); ok {
					return value, nil
				}
			}
			result, err := spec.Handler.Invoke(wfCtx, state, payload)
			if err != nil {
				return nil, err
			}
			if spec.CacheTTL > 0 {
				wfCtx.storeQueryCache(name, cacheKey, result, workflow.Now(ctx), spec.CacheTTL)
			}
			return result, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (i *temporalInstance) registerDiagnostics(ctx workflow.Context, wfCtx *wfContext) error {
	if err := workflow.SetQueryHandler(ctx, actors.DiagnosticsPatchesQuery, func() (any, error) {
		specs := i.desc.PatchSpecs()
		report := actors.PatchReport{Kind: i.desc.Kind}
		if len(specs) > 0 {
			report.Patches = make([]actors.PatchStatus, 0, len(specs))
			for _, spec := range specs {
				report.Patches = append(report.Patches, actors.PatchStatus{
					ID:        spec.ID,
					DefaultOn: spec.DefaultOn,
					Note:      spec.Note,
				})
			}
		}
		return report, nil
	}); err != nil {
		return err
	}
	return workflow.SetQueryHandler(ctx, actors.DiagnosticsSnapshotQuery, func() (any, error) {
		return actors.SnapshotReport{
			Kind:     i.desc.Kind,
			Snapshot: wfCtx.SnapshotInfo(),
		}, nil
	})
}

func (i *temporalInstance) registerUpdates(ctx workflow.Context, wfCtx *wfContext, state any) error {
	logger := workflow.GetLogger(ctx)
	for name, spec := range i.desc.Commands {
		name := name
		spec := spec
		handler := func(updateCtx workflow.Context, raw []byte) (any, error) {
			payload, err := i.decodeCommandPayload(raw, spec)
			if err != nil {
				logger.Error("decode update payload", "command", name, "error", err)
				return nil, err
			}
			meta := actors.MessageMetadata{}
			return i.handleCommand(updateCtx, wfCtx, state, spec, payload, logger, name, meta)
		}
		if err := workflow.SetUpdateHandler(ctx, name, handler); err != nil {
			return err
		}
	}
	return nil
}

func (i *temporalInstance) decodeCommandPayload(raw []byte, spec actors.CommandSpec) (any, error) {
	if spec.PayloadFactory == nil {
		if len(raw) == 0 {
			return nil, nil
		}
		var payload any
		if err := codec.Unmarshal(raw, &payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	holder := spec.PayloadFactory()
	if len(raw) > 0 {
		if err := codec.Unmarshal(raw, holder); err != nil {
			return nil, err
		}
	}
	if spec.DecodePayload != nil {
		return spec.DecodePayload(holder)
	}
	return holder, nil
}

func (i *temporalInstance) handleCommand(ctx workflow.Context, wfCtx *wfContext, state any, spec actors.CommandSpec, payload any, logger log.Logger, name string, meta actors.MessageMetadata) (result any, err error) {
	if spec.Validator != nil {
		if validationErr := spec.Validator(payload); validationErr != nil {
			return nil, actors.BusinessError(validationErr)
		}
	}
	instrumentation := i.beginCommandInstrumentation(ctx, wfCtx, name, meta)
	defer func() {
		instrumentation.finish(ctx, err)
	}()
	result, err = i.executeCommandWithRetry(ctx, wfCtx, state, spec, payload, logger, name, meta)
	if err == nil {
		wfCtx.invalidateQueryCache()
	}
	return result, err
}

func commandEventFrom(ctx workflow.Context, desc *actors.Description, name string, meta actors.MessageMetadata, attrs []observability.Attribute) observability.CommandEvent {
	info := workflow.GetInfo(ctx)
	event := observability.CommandEvent{
		ActorKind:     desc.Kind,
		Command:       name,
		WorkflowID:    info.WorkflowExecution.ID,
		RunID:         info.WorkflowExecution.RunID,
		TaskQueue:     info.TaskQueueName,
		MessageID:     meta.ID,
		CorrelationID: meta.CorrelationID,
		Attributes:    append([]observability.Attribute(nil), attrs...),
	}
	if meta.Caller.Kind != "" {
		event.CallerKind = meta.Caller.Kind
		event.CallerID = meta.Caller.ID
	}
	return event
}

func (i *temporalInstance) invokeCommand(ctx workflow.Context, wfCtx *wfContext, state any, spec actors.CommandSpec, payload any, name string, meta actors.MessageMetadata) (any, error) {
	execCtx := ctx
	cancelExec := func() {}
	if spec.Timeout > 0 {
		var cancel workflow.CancelFunc
		execCtx, cancel = workflow.WithCancel(ctx)
		cancelExec = cancel
	}
	defer cancelExec()

	var timerFuture workflow.Future
	cancelTimer := func() {}
	if spec.Timeout > 0 {
		var timerCtx workflow.Context
		timerCtx, cancelTimer = workflow.WithCancel(ctx)
		timerFuture = workflow.NewTimer(timerCtx, spec.Timeout)
		workflow.Go(ctx, func(ctx workflow.Context) {
			if err := timerFuture.Get(ctx, nil); err == nil {
				cancelExec()
			}
		})
	}
	defer cancelTimer()

	result, err := wfCtx.withWorkflowContext(execCtx, func() (any, error) {
		return wfCtx.withMessageMetadata(meta, func() (any, error) {
			return spec.Handler.Invoke(wfCtx, state, payload)
		})
	})

	if timerFuture != nil && timerFuture.IsReady() {
		if err == nil || temporal.IsCanceledError(err) {
			err = fmt.Errorf("actors: command %s timed out after %s", name, spec.Timeout)
		}
	}
	return result, err
}

func (i *temporalInstance) executeCommandWithRetry(ctx workflow.Context, wfCtx *wfContext, state any, spec actors.CommandSpec, payload any, logger log.Logger, name string, meta actors.MessageMetadata) (any, error) {
	policy := i.commandRetryPolicy(spec)
	attempts := policy.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	var lastResult any
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err := i.invokeCommand(ctx, wfCtx, state, spec, payload, name, meta)
		lastResult = result
		if err == nil || errors.Is(err, actors.ErrStopLoop) {
			return result, err
		}
		lastErr = err
		if actors.IsNonRetryable(err) {
			return lastResult, err
		}
		if delay, ok := actors.IsRetryAfter(err); ok {
			if delay > 0 {
				_ = workflow.Sleep(ctx, delay)
			}
			attempt--
			continue
		}
		if attempt < attempts {
			logger.Warn("command retry scheduled", "command", name, "attempt", attempt, "error", err)
			if delay := retryDelay(policy, attempt); delay > 0 {
				_ = workflow.Sleep(ctx, delay)
			}
		}
	}
	return lastResult, lastErr
}

type commandInstrumentation struct {
	start time.Time
	event observability.CommandEvent
	span  observability.Span
}

func (ci *commandInstrumentation) finish(ctx workflow.Context, err error) {
	duration := time.Since(ci.start)
	if ci.span != nil {
		if err != nil {
			ci.span.RecordError(err)
		}
		ci.span.End(err)
	}
	emitCommandFinish(ctx, ci.event, err, duration)
	observability.RecordCommandMetrics(ci.event.ActorKind, ci.event.Command, duration, err)
}

func (i *temporalInstance) beginCommandInstrumentation(ctx workflow.Context, wfCtx *wfContext, name string, meta actors.MessageMetadata) *commandInstrumentation {
	tracer := wfCtx.tracer
	if tracer == nil {
		tracer = observability.ActiveTracer()
	}
	attrs := []observability.Attribute{
		observability.String("actor.kind", i.desc.Kind),
		observability.String("actor.command", name),
		observability.String("actor.id", wfCtx.ref.ID),
	}
	if meta.ID != "" {
		attrs = append(attrs, observability.String("actor.message_id", meta.ID))
	}
	event := commandEventFrom(ctx, i.desc, name, meta, attrs)
	emitCommandStart(ctx, event)
	_, span := tracer.Start(context.Background(), "actor.command", attrs...)
	return &commandInstrumentation{
		start: time.Now(),
		event: event,
		span:  span,
	}
}

func (i *temporalInstance) commandRetryPolicy(spec actors.CommandSpec) actors.RetryPolicy {
	if hasRetryConfig(spec.Retry) {
		return spec.Retry
	}
	return i.desc.Retry
}

func hasRetryConfig(policy actors.RetryPolicy) bool {
	return policy.MaxAttempts != 0 || policy.InitialInterval != 0 || policy.BackoffCoefficient != 0
}

func retryDelay(policy actors.RetryPolicy, attempt int) time.Duration {
	if policy.InitialInterval <= 0 {
		return 0
	}
	delay := float64(policy.InitialInterval)
	if policy.BackoffCoefficient > 1 && attempt > 1 {
		multiplier := 1.0
		for i := 1; i < attempt; i++ {
			multiplier *= policy.BackoffCoefficient
		}
		delay *= multiplier
	}
	return time.Duration(delay)
}

func (i *temporalInstance) executeOneShot(wfCtx *wfContext, state any, req *oneShotCommand) (any, error) {
	if req == nil || req.Name == "" {
		return nil, fmt.Errorf("actors: oneshot command missing name")
	}
	spec, ok := i.desc.Commands[req.Name]
	if !ok {
		return nil, fmt.Errorf("actors: command %s not registered for kind %s", req.Name, i.desc.Kind)
	}
	payload, err := decodePayloadValue(req.Payload, spec.DecodePayload)
	if err != nil {
		return nil, err
	}
	if spec.Validator != nil {
		if err := spec.Validator(payload); err != nil {
			return nil, err
		}
	}
	return wfCtx.withMessageMetadata(actors.MessageMetadata{}, func() (any, error) {
		return spec.Handler.Invoke(wfCtx, state, payload)
	})
}

func extractStartEnvelope(value any) (startEnvelope, bool) {
	if env, ok := value.(startEnvelope); ok && env.Envelope {
		return env, true
	}
	if value == nil {
		return startEnvelope{}, false
	}
	data, err := codec.Marshal(value)
	if err != nil {
		return startEnvelope{}, false
	}
	var env startEnvelope
	if err := codec.Unmarshal(data, &env); err != nil {
		return startEnvelope{}, false
	}
	if !env.Envelope {
		return startEnvelope{}, false
	}
	return env, true
}

func (i *temporalInstance) startSignalHandlers(ctx workflow.Context, wfCtx *wfContext, state any) {
	startSignalLoop(ctx, queryRequestSignal, func(loopCtx workflow.Context, req queryRequest) {
		i.handleQueryRequest(loopCtx, wfCtx, state, req)
	})
	startSignalLoop(ctx, queryReplySignal, func(_ workflow.Context, reply queryReply) {
		wfCtx.deliverQueryReply(reply)
	})
	startSignalLoop(ctx, askRequestSignal, func(loopCtx workflow.Context, req askRequest) {
		i.handleAskRequest(loopCtx, wfCtx, state, req)
	})
	startSignalLoop(ctx, askReplySignal, func(_ workflow.Context, reply askReply) {
		wfCtx.deliverAskReply(reply)
	})
	startSignalLoop(ctx, continueReplySignal, func(_ workflow.Context, reply continueReply) {
		wfCtx.deliverContinueReply(reply)
	})
	startSignalLoop(ctx, tellRequestSignal, func(loopCtx workflow.Context, req tellRequest) {
		i.handleTellRequest(loopCtx, wfCtx, state, req)
	})
}

func startSignalLoop[T any](ctx workflow.Context, signal string, handle func(workflow.Context, T)) {
	ch := workflow.GetSignalChannel(ctx, signal)
	workflow.Go(ctx, func(goCtx workflow.Context) {
		for {
			var msg T
			if ok := ch.Receive(goCtx, &msg); !ok {
				return
			}
			handle(goCtx, msg)
		}
	})
}

func (i *temporalInstance) handleQueryRequest(ctx workflow.Context, wfCtx *wfContext, state any, req queryRequest) {
	reply := queryReply{ID: req.ID}
	spec, ok := i.desc.Queries[req.Query]
	if !ok {
		reply.Error = fmt.Sprintf("actors: query %s not registered on kind %s", req.Query, i.desc.Kind)
		i.sendQueryReply(ctx, req, reply)
		return
	}
	payload, err := decodePayloadValue(req.Payload, spec.DecodePayload)
	if err != nil {
		reply.Error = err.Error()
		i.sendQueryReply(ctx, req, reply)
		return
	}
	value, err := wfCtx.withMessageMetadata(req.Envelope, func() (any, error) {
		return spec.Handler.Invoke(wfCtx, state, payload)
	})
	if err != nil {
		if inner, ok := actors.AsBusinessError(err); ok {
			reply.Error = inner.Error()
		} else {
			reply.Error = err.Error()
		}
	} else {
		reply.Payload = value
	}
	i.sendQueryReply(ctx, req, reply)
}

func (i *temporalInstance) sendQueryReply(ctx workflow.Context, req queryRequest, reply queryReply) {
	target := req.ReplySignal
	if target == "" {
		target = queryReplySignal
	}
	if req.ReplyWorkflow == "" {
		workflow.GetLogger(ctx).Error("query reply missing workflow target", "query", req.Query)
		return
	}
	fut := workflow.SignalExternalWorkflow(ctx, req.ReplyWorkflow, req.ReplyRunID, target, reply)
	if err := fut.Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("query reply delivery failed", "workflow_id", req.ReplyWorkflow, "signal", target, "error", err)
	}
}

func (i *temporalInstance) handleAskRequest(ctx workflow.Context, wfCtx *wfContext, state any, req askRequest) {
	reply := askReply{ID: req.ID}
	spec, payload, ok, err := i.prepareCommandRequest(req.Command, req.Payload)
	if !ok {
		reply.Error = fmt.Sprintf("actors: command %s not registered on kind %s", req.Command, i.desc.Kind)
		i.sendAskReply(ctx, req, reply)
		return
	}
	if err != nil {
		reply.Error = err.Error()
		i.sendAskReply(ctx, req, reply)
		return
	}
	result, err := i.handleCommand(ctx, wfCtx, state, spec, payload, workflow.GetLogger(ctx), req.Command, req.Envelope)
	if err != nil {
		if inner, ok := actors.AsBusinessError(err); ok {
			reply.Error = inner.Error()
		} else {
			reply.Error = err.Error()
		}
	} else {
		reply.Payload = result
	}
	i.sendAskReply(ctx, req, reply)
}

func (i *temporalInstance) handleTellRequest(ctx workflow.Context, wfCtx *wfContext, state any, req tellRequest) {
	spec, payload, ok, err := i.prepareCommandRequest(req.Command, req.Payload)
	if !ok {
		workflow.GetLogger(ctx).Warn("tell command discarded", "command", req.Command, "kind", i.desc.Kind)
		return
	}
	if err != nil {
		workflow.GetLogger(ctx).Error("decode tell payload", "command", req.Command, "error", err)
		return
	}
	_, err = i.handleCommand(ctx, wfCtx, state, spec, payload, workflow.GetLogger(ctx), req.Command, req.Envelope)
	if err == nil {
		return
	}
	if workflow.IsContinueAsNewError(err) {
		wfCtx.requestStop()
		return
	}
	if inner, ok := actors.AsBusinessError(err); ok {
		workflow.GetLogger(ctx).Info("tell command business error", "command", req.Command, "error", inner)
		return
	}
	if errors.Is(err, errMessageDeadlineExceeded) || errors.Is(err, errMessageRetryBudgetExceeded) {
		workflow.GetLogger(ctx).Info("tell command discarded", "command", req.Command, "error", err)
		return
	}
	if errors.Is(err, actors.ErrStopLoop) {
		wfCtx.requestStop()
		return
	}
	workflow.GetLogger(ctx).Error("tell command failed", "command", req.Command, "error", err)
}

func (i *temporalInstance) sendAskReply(ctx workflow.Context, req askRequest, reply askReply) {
	target := req.ReplySignal
	if target == "" {
		target = askReplySignal
	}
	if req.ReplyWorkflow == "" {
		workflow.GetLogger(ctx).Error("ask reply missing workflow target", "command", req.Command)
		return
	}
	fut := workflow.SignalExternalWorkflow(ctx, req.ReplyWorkflow, req.ReplyRunID, target, reply)
	if err := fut.Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("ask reply delivery failed", "workflow_id", req.ReplyWorkflow, "signal", target, "error", err)
	}
}

func (i *temporalInstance) sendContinueReply(ctx workflow.Context, req continueRequest, reply continueReply) {
	target := req.ReplySignal
	if target == "" {
		target = continueReplySignal
	}
	if req.ReplyWorkflow == "" {
		workflow.GetLogger(ctx).Error("continue reply missing workflow target")
		return
	}
	fut := workflow.SignalExternalWorkflow(ctx, req.ReplyWorkflow, req.ReplyRunID, target, reply)
	if err := fut.Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("continue reply delivery failed", "workflow_id", req.ReplyWorkflow, "signal", target, "error", err)
	}
}
