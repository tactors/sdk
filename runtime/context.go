package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"github.com/tactors/sdk/observability"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	maxPendingRequests             = 1024
	defaultAskWaitTimeout          = time.Minute
	defaultQueryTimeout            = time.Minute
	effectsMemoKey                 = "__actors_effects"
	queryCacheNilKey               = "__actors_query_nil_payload"
	defaultActivityStartToClose    = 30 * time.Second
	defaultActivityScheduleToClose = 2 * time.Minute
)

var (
	errMessageDeadlineExceeded    = errors.New("actors: message deadline exceeded")
	errMessageRetryBudgetExceeded = errors.New("actors: message retry budget exhausted")
)

type wfContext struct {
	workflowCtx       workflow.Context
	ref               actors.Ref
	logger            actors.Logger
	parent            actors.Ref
	childSeq          int
	querySeq          int
	queryWaiters      map[string]workflow.Channel
	askSeq            int
	askWaiters        map[string]workflow.Channel
	continueSeq       int
	continueWaiters   map[string]workflow.Channel
	shouldStop        bool
	activityDecoders  map[string]func(any) (any, error)
	activityDefaults  map[string]actors.ActivityCallOptions
	activityQueue     string
	activityObservers []func(string, actors.ActivityCallOptions)
	activityNames     map[string]string
	messageMeta       actors.MessageMetadata
	correlation       actors.CorrelationData
	messageSeq        int
	callSeq           int
	effects           map[string]effectEntry
	tracer            observability.Tracer
	snapshotInfo      actors.SnapshotInfo
	queryCache        map[string]map[string]queryCacheEntry
}

func (c *wfContext) ActorID() string { return c.ref.ID }
func (c *wfContext) Now() time.Time  { return workflow.Now(c.workflowCtx) }
func (c *wfContext) Sleep(d time.Duration) error {
	return workflow.NewTimer(c.workflowCtx, d).Get(c.workflowCtx, nil)
}
func (c *wfContext) Version(changeID string, defaultVersion, newVersion int) int {
	return int(workflow.GetVersion(
		c.workflowCtx,
		changeID,
		workflow.Version(defaultVersion),
		workflow.Version(newVersion),
	))
}
func (c *wfContext) Activity(name string, payload any) actors.ActivityFuture {
	return c.ActivityWithOptions(name, payload, actors.ActivityCallOptions{})
}

func (c *wfContext) ActivityWithOptions(name string, payload any, opts actors.ActivityCallOptions) actors.ActivityFuture {
	return c.activityWithContext(c.workflowCtx, name, payload, opts)
}

func (c *wfContext) activityWithContext(wctx workflow.Context, name string, payload any, opts actors.ActivityCallOptions) actors.ActivityFuture {
	merged := mergeActivityOptions(c.activityDefaults[name], opts)
	if len(c.activityObservers) > 0 {
		for _, obs := range c.activityObservers {
			obs(name, merged)
		}
	}
	// honor the activity queue provided by workflow/session context
	taskQueue := merged.TaskQueue
	if taskQueue == "" {
		taskQueue = c.activityQueue
	}
	callOpts := workflow.ActivityOptions{
		ScheduleToCloseTimeout: merged.ScheduleToClose,
		ScheduleToStartTimeout: merged.ScheduleToStart,
		StartToCloseTimeout:    merged.StartToClose,
		HeartbeatTimeout:       merged.Heartbeat,
		TaskQueue:              taskQueue,
	}
	if hasRetryConfig(merged.Retry) {
		callOpts.RetryPolicy = toTemporalRetry(merged.Retry)
	}
	ctx := workflow.WithActivityOptions(wctx, callOpts)
	future := workflow.ExecuteActivity(ctx, name, payload)
	return wfActivityFuture{activityCtx: ctx, future: future}
}

func (c *wfContext) BackgroundActivity(name string, payload any) {
	bgCtx, cancel := workflow.NewDisconnectedContext(c.workflowCtx)
	workflow.Go(bgCtx, func(ctx workflow.Context) {
		defer cancel()
		future := c.activityWithContext(ctx, name, payload, actors.ActivityCallOptions{})
		if _, err := future.Get(); err != nil {
			if logger := c.Logger(); logger != nil {
				logger.Error("background activity failed", "activity", name, "error", err)
			}
		}
	})
}

func (c *wfContext) DecodeActivityResult(name string, value any) (any, error) {
	if c.activityDecoders == nil {
		return value, nil
	}
	if decode, ok := c.activityDecoders[name]; ok && decode != nil {
		return decode(value)
	}
	return value, nil
}
func (c *wfContext) ActivityName(typeKey string) (string, bool) {
	if len(c.activityNames) == 0 {
		return "", false
	}
	name, ok := c.activityNames[typeKey]
	return name, ok
}

func mergeActivityOptions(base, override actors.ActivityCallOptions) actors.ActivityCallOptions {
	out := base
	if override.ScheduleToClose > 0 {
		out.ScheduleToClose = override.ScheduleToClose
	}
	if override.ScheduleToStart > 0 {
		out.ScheduleToStart = override.ScheduleToStart
	}
	if override.StartToClose > 0 {
		out.StartToClose = override.StartToClose
	}
	if override.Heartbeat > 0 {
		out.Heartbeat = override.Heartbeat
	}
	if hasRetryConfig(override.Retry) {
		out.Retry = override.Retry
	}
	if strings.TrimSpace(override.TaskQueue) != "" {
		out.TaskQueue = strings.TrimSpace(override.TaskQueue)
	}
	if out.StartToClose == 0 && out.ScheduleToClose == 0 {
		out.StartToClose = defaultActivityStartToClose
		out.ScheduleToClose = defaultActivityScheduleToClose
	}
	if !hasRetryConfig(out.Retry) {
		out.Retry = actors.RetryPolicy{MaxAttempts: 1}
	}
	return out
}
func (c *wfContext) Logger() actors.Logger {
	if c.logger == nil {
		l := workflow.GetLogger(c.workflowCtx)
		if l == nil {
			return noopLogger{}
		}
		c.logger = workflowLogger{
			logger: l,
			ctx:    c,
		}
	}
	return c.logger
}

func (c *wfContext) Self() actors.Ref {
	return c.ref
}

func (c *wfContext) MessageMetadata() actors.MessageMetadata {
	return c.messageMeta
}

func (c *wfContext) Correlation() actors.CorrelationData {
	return c.correlation.Clone()
}

func (c *wfContext) SetCorrelation(data actors.CorrelationData) {
	c.correlation = data.Clone()
}

func (c *wfContext) SnapshotInfo() actors.SnapshotInfo {
	info := c.snapshotInfo
	return info
}

type temporalSessionHandle struct {
	ctx workflow.Context
}

type queryCacheEntry struct {
	value   any
	expires time.Time
}

func (c *wfContext) invalidateQueryCache() {
	if len(c.queryCache) == 0 {
		return
	}
	c.queryCache = nil
}

func (c *wfContext) cachedQuery(name, key string, now time.Time) (any, bool) {
	if len(c.queryCache) == 0 {
		return nil, false
	}
	entries := c.queryCache[name]
	if len(entries) == 0 {
		return nil, false
	}
	entry, ok := entries[key]
	if !ok {
		return nil, false
	}
	if !entry.expires.IsZero() && now.After(entry.expires) {
		delete(entries, key)
		return nil, false
	}
	return entry.value, true
}

func (c *wfContext) storeQueryCache(name, key string, value any, now time.Time, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	if c.queryCache == nil {
		c.queryCache = make(map[string]map[string]queryCacheEntry)
	}
	entries := c.queryCache[name]
	if entries == nil {
		entries = make(map[string]queryCacheEntry)
	}
	entry := queryCacheEntry{value: value}
	if ttl > 0 {
		entry.expires = now.Add(ttl)
	}
	entries[key] = entry
	c.queryCache[name] = entries
}

func queryCacheKey(raw []byte) string {
	if len(raw) == 0 {
		return queryCacheNilKey
	}
	return string(raw)
}

func (c *wfContext) captureSnapshotStats(now time.Time, snapshotEvery int) snapshotStats {
	stats := snapshotStats{
		SnapshotEvery: snapshotEvery,
	}
	c.snapshotInfo.ContinueAsNewCount++
	stats.ContinueCount = c.snapshotInfo.ContinueAsNewCount
	if snapshotEvery > 0 {
		c.snapshotInfo.Enabled = true
		c.snapshotInfo.SnapshotEvery = snapshotEvery
	}
	if c.snapshotInfo.Enabled {
		c.snapshotInfo.SnapshotsTaken++
		stats.SnapshotsTaken = c.snapshotInfo.SnapshotsTaken
		c.snapshotInfo.LastSnapshotTime = now
		stats.LastSnapshotTime = now
	} else {
		stats.SnapshotsTaken = c.snapshotInfo.SnapshotsTaken
		stats.LastSnapshotTime = c.snapshotInfo.LastSnapshotTime
	}
	c.snapshotInfo.CommandsSinceSnapshot = 0
	return stats
}

func (c *wfContext) restoreSnapshotStats(stats snapshotStats) {
	if stats.SnapshotEvery > 0 {
		c.snapshotInfo.SnapshotEvery = stats.SnapshotEvery
		c.snapshotInfo.Enabled = true
	}
	if stats.SnapshotsTaken > 0 {
		c.snapshotInfo.SnapshotsTaken = stats.SnapshotsTaken
	}
	if stats.ContinueCount > 0 {
		c.snapshotInfo.ContinueAsNewCount = stats.ContinueCount
	}
	if !stats.LastSnapshotTime.IsZero() {
		c.snapshotInfo.LastSnapshotTime = stats.LastSnapshotTime
	}
	c.snapshotInfo.CommandsSinceSnapshot = 0
}

func (c *wfContext) requestStop() {
	c.shouldStop = true
}

func (c *wfContext) stopRequested() bool {
	return c.shouldStop
}

func (c *wfContext) Parent() actors.Ref {
	return c.parent
}

func (c *wfContext) SpawnChild(kind string, init any, opts ...actors.SpawnOption) (actors.Ref, error) {
	if kind == "" {
		return actors.Ref{}, fmt.Errorf("actors: child kind must be non-empty")
	}
	cfg := applySpawnOptions(opts...)
	desc := actors.LookupDescription(kind)
	if desc == nil {
		return actors.Ref{}, fmt.Errorf("actors: actor kind %q not registered", kind)
	}
	childID := cfg.Name
	if childID == "" {
		childID = c.nextChildID(kind)
	}
	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID: childID,
	}
	queue := cfg.TaskQueue
	if queue == "" {
		queue = workflowQueueFor(kind, desc)
	}
	childOpts.TaskQueue = queue
	if cfg.Timeout > 0 {
		childOpts.WorkflowRunTimeout = cfg.Timeout
	}
	childCtx := workflow.WithChildOptions(c.workflowCtx, childOpts)
	env := startEnvelope{
		Parent:      c.ref,
		Payload:     init,
		Envelope:    true,
		Correlation: c.correlation.Clone(),
	}
	future := workflow.ExecuteChildWorkflow(childCtx, kind, childID, env)
	execFuture := future.GetChildWorkflowExecution()
	var exec workflow.Execution
	if err := execFuture.Get(childCtx, &exec); err != nil {
		return actors.Ref{}, err
	}
	return actors.Ref{Kind: kind, ID: childID}, nil
}

func (c *wfContext) SpawnOneShot(payload any, opts ...actors.SpawnOption) (any, error) {
	cfg := applySpawnOptions(opts...)
	if cfg.Kind == "" {
		return nil, fmt.Errorf("actors: WithChildKind is required for SpawnOneShot")
	}
	desc := actors.LookupDescription(cfg.Kind)
	if desc == nil {
		return nil, fmt.Errorf("actors: actor kind %q is not registered", cfg.Kind)
	}
	typeKey := actors.TypeKeyOf(payload)
	if typeKey == "" {
		return nil, fmt.Errorf("actors: cannot infer command type for payload %T", payload)
	}
	cmdName, ok := desc.CommandTypes[typeKey]
	if !ok {
		return nil, fmt.Errorf("actors: no command registered for payload %s on kind %s", typeKey, cfg.Kind)
	}
	childID := cfg.Name
	if childID == "" {
		childID = c.nextChildID(cfg.Kind)
	}
	childOpts := workflow.ChildWorkflowOptions{WorkflowID: childID}
	queue := cfg.TaskQueue
	if queue == "" {
		queue = workflowQueueFor(cfg.Kind, desc)
	}
	childOpts.TaskQueue = queue
	if cfg.Timeout > 0 {
		childOpts.WorkflowRunTimeout = cfg.Timeout
	}
	childCtx := workflow.WithChildOptions(c.workflowCtx, childOpts)
	env := startEnvelope{
		Parent:      c.ref,
		Envelope:    true,
		Correlation: c.correlation.Clone(),
		OneShot: &oneShotCommand{
			Name:    cmdName,
			Payload: payload,
		},
	}
	future := workflow.ExecuteChildWorkflow(childCtx, cfg.Kind, childID, env)
	var result any
	if err := future.Get(childCtx, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *wfContext) nextChildID(kind string) string {
	c.childSeq++
	return fmt.Sprintf("%s-%s-%d", c.ref.ID, kind, c.childSeq)
}

func (c *wfContext) QueryActor(ref actors.Ref, payload any) (any, error) {
	if ref.ID == "" {
		return nil, fmt.Errorf("actors: target ref ID is empty")
	}
	desc := actors.LookupDescription(ref.Kind)
	if desc == nil {
		return nil, fmt.Errorf("actors: actor kind %q not registered", ref.Kind)
	}
	name, err := queryNameFor(desc, payload)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	tracer := c.tracer
	if tracer == nil {
		tracer = observability.ActiveTracer()
	}
	attrs := []observability.Attribute{
		observability.String("actor.kind", c.ref.Kind),
		observability.String("actor.target_kind", ref.Kind),
		observability.String("actor.target_id", ref.ID),
		observability.String("actor.query", name),
	}
	var queryEvent observability.QueryEvent
	var haveQueryEvent bool
	_, span := tracer.Start(context.Background(), "actor.query", attrs...)
	defer func() {
		duration := time.Since(start)
		if span != nil {
			if err != nil {
				span.RecordError(err)
			}
			span.End(err)
		}
		if haveQueryEvent {
			emitQueryFinish(c.workflowCtx, queryEvent, err, duration)
		}
		observability.RecordQueryMetrics(c.ref.Kind, ref.Kind, name, duration, err)
	}()
	id, ch, err := c.registerQueryWaiter()
	if err != nil {
		return nil, err
	}
	queryEvent = queryEventFrom(c.workflowCtx, c.ref, ref, name, id, attrs)
	haveQueryEvent = true
	emitQueryStart(c.workflowCtx, queryEvent)
	meta := c.newOutgoingMetadata("query", id)
	req := queryRequest{
		ID:            id,
		Query:         name,
		Payload:       payload,
		ReplyWorkflow: c.targetWorkflowID(),
		ReplySignal:   queryReplySignal,
		Envelope:      meta,
	}
	if err := c.signalWorkflow(ref, queryRequestSignal, req); err != nil {
		c.removeQueryWaiter(id)
		return nil, err
	}
	reply, err := c.waitForQueryReply(ch, name, id)
	if err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, fmt.Errorf(reply.Error)
	}
	return reply.Payload, nil
}

func (c *wfContext) AskActor(ref actors.Ref, payload any) (any, error) {
	if currentAskRoutingMode() == AskRouteUpdate {
		result, err := c.askViaUpdate(ref, payload)
		if !errors.Is(err, errAskUpdateUnsupported) {
			return result, err
		}
	}
	return c.askViaSignal(ref, payload)
}

var errAskUpdateUnsupported = errors.New("actors: ask update routing not supported")

func (c *wfContext) askViaSignal(ref actors.Ref, payload any) (any, error) {
	if ref.ID == "" {
		return nil, fmt.Errorf("actors: target ref ID is empty")
	}
	desc := actors.LookupDescription(ref.Kind)
	if desc == nil {
		return nil, fmt.Errorf("actors: actor kind %q not registered", ref.Kind)
	}
	name, err := commandNameFor(desc, payload)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	tracer := c.tracer
	if tracer == nil {
		tracer = observability.ActiveTracer()
	}
	attrs := []observability.Attribute{
		observability.String("actor.kind", c.ref.Kind),
		observability.String("actor.target_kind", ref.Kind),
		observability.String("actor.target_id", ref.ID),
		observability.String("actor.command", name),
	}
	var askEvent observability.AskEvent
	var haveAskEvent bool
	_, span := tracer.Start(context.Background(), "actor.ask", attrs...)
	defer func() {
		duration := time.Since(start)
		if span != nil {
			if err != nil {
				span.RecordError(err)
			}
			span.End(err)
		}
		if haveAskEvent {
			emitAskFinish(c.workflowCtx, askEvent, err, duration)
		}
		observability.RecordAskMetrics(c.ref.Kind, ref.Kind, name, duration, err)
	}()
	id, ch, err := c.registerAskWaiter()
	if err != nil {
		return nil, err
	}
	askEvent = askEventFrom(c.workflowCtx, c.ref, ref, name, id, attrs)
	haveAskEvent = true
	emitAskStart(c.workflowCtx, askEvent)
	meta := c.newOutgoingMetadata("ask", id)
	c.applySignalDeadline(desc, name, &meta)
	req := askRequest{
		ID:            id,
		Command:       name,
		Payload:       payload,
		ReplyWorkflow: c.targetWorkflowID(),
		ReplySignal:   askReplySignal,
		Envelope:      meta,
	}
	if err := c.signalWorkflow(ref, askRequestSignal, req); err != nil {
		c.removeAskWaiter(id)
		return nil, err
	}
	reply, err := c.waitForAskReply(ch, name, id)
	if err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, fmt.Errorf(reply.Error)
	}
	return reply.Payload, nil
}

func (c *wfContext) askViaUpdate(ref actors.Ref, payload any) (any, error) {
	return nil, errAskUpdateUnsupported
}

func queryEventFrom(ctx workflow.Context, caller, target actors.Ref, name, correlationID string, attrs []observability.Attribute) observability.QueryEvent {
	info := workflow.GetInfo(ctx)
	return observability.QueryEvent{
		CallerKind:      caller.Kind,
		CallerID:        caller.ID,
		CallerWorkflow:  info.WorkflowExecution.ID,
		CallerRunID:     info.WorkflowExecution.RunID,
		CallerTaskQueue: info.TaskQueueName,
		TargetKind:      target.Kind,
		TargetID:        target.ID,
		Query:           name,
		CorrelationID:   correlationID,
		Attributes:      append([]observability.Attribute(nil), attrs...),
	}
}

func askEventFrom(ctx workflow.Context, caller, target actors.Ref, name, correlationID string, attrs []observability.Attribute) observability.AskEvent {
	info := workflow.GetInfo(ctx)
	return observability.AskEvent{
		CallerKind:      caller.Kind,
		CallerID:        caller.ID,
		CallerWorkflow:  info.WorkflowExecution.ID,
		CallerRunID:     info.WorkflowExecution.RunID,
		CallerTaskQueue: info.TaskQueueName,
		TargetKind:      target.Kind,
		TargetID:        target.ID,
		Command:         name,
		CorrelationID:   correlationID,
		Attributes:      append([]observability.Attribute(nil), attrs...),
	}
}

func (c *wfContext) RequestContinueAsNew(ref actors.Ref, opts actors.ContinueAsNewOptions) error {
	if ref.ID == "" {
		return fmt.Errorf("actors: target ref ID is empty")
	}
	id, ch, err := c.registerContinueWaiter()
	if err != nil {
		return err
	}
	req := continueRequest{
		ID:            id,
		Init:          opts.Init,
		ReplyWorkflow: c.targetWorkflowID(),
		ReplySignal:   continueReplySignal,
		Envelope:      c.newOutgoingMetadata("continue", id),
	}
	if err := c.signalWorkflow(ref, continueRequestSignal, req); err != nil {
		c.removeContinueWaiter(id)
		return err
	}
	reply, err := c.waitForContinueReply(ch, id)
	if err != nil {
		return err
	}
	if reply.Error != "" {
		return fmt.Errorf(reply.Error)
	}
	return nil
}

func (c *wfContext) registerQueryWaiter() (string, workflow.Channel, error) {
	if c.queryWaiters == nil {
		c.queryWaiters = make(map[string]workflow.Channel)
	}
	if len(c.queryWaiters) >= maxPendingRequests {
		return "", nil, fmt.Errorf("actors: too many pending query requests")
	}
	c.querySeq++
	id := fmt.Sprintf("%s-query-%d", c.ref.ID, c.querySeq)
	ch := workflow.NewChannel(c.workflowCtx)
	c.queryWaiters[id] = ch
	return id, ch, nil
}

func (c *wfContext) removeQueryWaiter(id string) workflow.Channel {
	if c.queryWaiters == nil {
		return nil
	}
	ch := c.queryWaiters[id]
	delete(c.queryWaiters, id)
	return ch
}

func (c *wfContext) deliverQueryReply(reply queryReply) {
	ch := c.removeQueryWaiter(reply.ID)
	if ch == nil {
		return
	}
	workflow.Go(c.workflowCtx, func(ctx workflow.Context) {
		ch.Send(ctx, reply)
	})
}

func (c *wfContext) registerAskWaiter() (string, workflow.Channel, error) {
	if c.askWaiters == nil {
		c.askWaiters = make(map[string]workflow.Channel)
	}
	if len(c.askWaiters) >= maxPendingRequests {
		return "", nil, fmt.Errorf("actors: too many pending ask requests")
	}
	c.askSeq++
	id := fmt.Sprintf("%s-ask-%d", c.ref.ID, c.askSeq)
	ch := workflow.NewChannel(c.workflowCtx)
	c.askWaiters[id] = ch
	return id, ch, nil
}

func (c *wfContext) removeAskWaiter(id string) workflow.Channel {
	if c.askWaiters == nil {
		return nil
	}
	ch := c.askWaiters[id]
	delete(c.askWaiters, id)
	return ch
}

func (c *wfContext) deliverAskReply(reply askReply) {
	ch := c.removeAskWaiter(reply.ID)
	if ch == nil {
		return
	}
	workflow.Go(c.workflowCtx, func(ctx workflow.Context) {
		ch.Send(ctx, reply)
	})
}

func (c *wfContext) registerContinueWaiter() (string, workflow.Channel, error) {
	if c.continueWaiters == nil {
		c.continueWaiters = make(map[string]workflow.Channel)
	}
	if len(c.continueWaiters) >= maxPendingRequests {
		return "", nil, fmt.Errorf("actors: too many pending continue requests")
	}
	c.continueSeq++
	id := fmt.Sprintf("%s-continue-%d", c.ref.ID, c.continueSeq)
	ch := workflow.NewChannel(c.workflowCtx)
	c.continueWaiters[id] = ch
	return id, ch, nil
}

func (c *wfContext) removeContinueWaiter(id string) workflow.Channel {
	if c.continueWaiters == nil {
		return nil
	}
	ch := c.continueWaiters[id]
	delete(c.continueWaiters, id)
	return ch
}

func (c *wfContext) deliverContinueReply(reply continueReply) {
	ch := c.removeContinueWaiter(reply.ID)
	if ch == nil {
		return
	}
	workflow.Go(c.workflowCtx, func(ctx workflow.Context) {
		ch.Send(ctx, reply)
	})
}

func (c *wfContext) waitForQueryReply(ch workflow.Channel, name, id string) (queryReply, error) {
	timeout := queryTimeout()
	var (
		timerCtx workflow.Context
		cancel   workflow.CancelFunc
		timer    workflow.Future
	)
	if timeout > 0 {
		timerCtx, cancel = workflow.WithCancel(c.workflowCtx)
		timer = workflow.NewTimer(timerCtx, timeout)
		defer cancel()
	}
	selector := workflow.NewSelector(c.workflowCtx)
	var reply queryReply
	var err error
	selector.AddReceive(ch, func(rc workflow.ReceiveChannel, more bool) {
		rc.Receive(c.workflowCtx, &reply)
		if cancel != nil {
			cancel()
		}
	})
	if timer != nil {
		selector.AddFuture(timer, func(workflow.Future) {
			err = fmt.Errorf("actors: query %s timed out waiting for reply", name)
		})
	}
	selector.Select(c.workflowCtx)
	c.removeQueryWaiter(id)
	if err != nil {
		return queryReply{}, err
	}
	return reply, nil
}

func (c *wfContext) waitForAskReply(ch workflow.Channel, name, id string) (askReply, error) {
	timeout := askTimeout()
	var (
		timerCtx workflow.Context
		cancel   workflow.CancelFunc
		timer    workflow.Future
	)
	if timeout > 0 {
		timerCtx, cancel = workflow.WithCancel(c.workflowCtx)
		timer = workflow.NewTimer(timerCtx, timeout)
		defer cancel()
	}
	selector := workflow.NewSelector(c.workflowCtx)
	var reply askReply
	var err error
	selector.AddReceive(ch, func(rc workflow.ReceiveChannel, more bool) {
		rc.Receive(c.workflowCtx, &reply)
		if cancel != nil {
			cancel()
		}
	})
	if timer != nil {
		selector.AddFuture(timer, func(workflow.Future) {
			err = fmt.Errorf("actors: ask %s timed out waiting for reply", name)
		})
	}
	selector.Select(c.workflowCtx)
	c.removeAskWaiter(id)
	if err != nil {
		return askReply{}, err
	}
	return reply, nil
}

func (c *wfContext) waitForContinueReply(ch workflow.Channel, id string) (continueReply, error) {
	timeout := askTimeout()
	var (
		timerCtx workflow.Context
		cancel   workflow.CancelFunc
		timer    workflow.Future
	)
	if timeout > 0 {
		timerCtx, cancel = workflow.WithCancel(c.workflowCtx)
		timer = workflow.NewTimer(timerCtx, timeout)
		defer cancel()
	}
	selector := workflow.NewSelector(c.workflowCtx)
	var reply continueReply
	var err error
	selector.AddReceive(ch, func(rc workflow.ReceiveChannel, more bool) {
		rc.Receive(c.workflowCtx, &reply)
		if cancel != nil {
			cancel()
		}
	})
	if timer != nil {
		selector.AddFuture(timer, func(workflow.Future) {
			err = fmt.Errorf("actors: continue-as-new request timed out waiting for reply")
		})
	}
	selector.Select(c.workflowCtx)
	c.removeContinueWaiter(id)
	if err != nil {
		return continueReply{}, err
	}
	return reply, nil
}

func (c *wfContext) SearchAttributes() map[string]any {
	info := workflow.GetInfo(c.workflowCtx)
	if info == nil || info.SearchAttributes == nil {
		return nil
	}
	fields := info.SearchAttributes.IndexedFields
	if len(fields) == 0 {
		return nil
	}
	dc := dataConverter()
	out := make(map[string]any, len(fields))
	for k, payload := range fields {
		var value any
		if err := dc.FromPayload(payload, &value); err == nil {
			out[k] = value
		}
	}
	return out
}

func (c *wfContext) UpsertSearchAttributes(attrs map[string]any) error {
	if len(attrs) == 0 {
		return nil
	}
	return workflow.UpsertSearchAttributes(c.workflowCtx, attrs)
}

func (c *wfContext) Memo() map[string]any {
	info := workflow.GetInfo(c.workflowCtx)
	if info == nil || info.Memo == nil || info.Memo.Fields == nil {
		return nil
	}
	dc := dataConverter()
	out := make(map[string]any, len(info.Memo.Fields))
	for k, payload := range info.Memo.Fields {
		var value any
		if err := dc.FromPayload(payload, &value); err == nil {
			out[k] = value
		}
	}
	return out
}

func (c *wfContext) ContinueAsNew(payload any) error {
	info := workflow.GetInfo(c.workflowCtx)
	if info == nil {
		return fmt.Errorf("actors: workflow info unavailable")
	}
	env := startEnvelope{
		Parent:      c.parent,
		Payload:     payload,
		Envelope:    true,
		Correlation: c.correlation.Clone(),
	}
	return workflow.NewContinueAsNewError(c.workflowCtx, info.WorkflowType.Name, c.ref.ID, env)
}

func (c *wfContext) StartSession(opts actors.SessionOptions) (actors.Session, error) {
	if opts.CreationTimeout <= 0 {
		opts.CreationTimeout = time.Minute
	}
	if opts.ExecutionTimeout <= 0 {
		opts.ExecutionTimeout = time.Hour
	}
	sessionCtx, err := workflow.CreateSession(c.workflowCtx, &workflow.SessionOptions{
		CreationTimeout:  opts.CreationTimeout,
		ExecutionTimeout: opts.ExecutionTimeout,
		HeartbeatTimeout: opts.HeartbeatTimeout,
	})
	if err != nil {
		return actors.Session{}, err
	}
	info := workflow.GetSessionInfo(sessionCtx)
	handle := &temporalSessionHandle{ctx: sessionCtx}
	return actors.NewSessionHandle(info.SessionID, c, handle), nil
}

func (c *wfContext) InvokeSessionActivity(handle any, name string, payload any, opts actors.ActivityCallOptions) (actors.ActivityFuture, error) {
	h, ok := handle.(*temporalSessionHandle)
	if !ok {
		return nil, fmt.Errorf("runtime: invalid session handle")
	}
	return c.activityWithContext(h.ctx, name, payload, opts), nil
}

func (c *wfContext) CompleteSessionHandle(handle any) error {
	h, ok := handle.(*temporalSessionHandle)
	if !ok {
		return fmt.Errorf("runtime: invalid session handle")
	}
	workflow.CompleteSession(h.ctx)
	return nil
}

func toTemporalRetry(policy actors.RetryPolicy) *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:    policy.InitialInterval,
		BackoffCoefficient: policy.BackoffCoefficient,
		MaximumAttempts:    int32(policy.MaxAttempts),
	}
}

func (c *wfContext) SendCommand(ref actors.Ref, payload any) error {
	if ref.ID == "" {
		return fmt.Errorf("actors: target ref ID is empty")
	}
	desc := actors.LookupDescription(ref.Kind)
	if desc == nil {
		return fmt.Errorf("actors: actor kind %q not registered", ref.Kind)
	}
	name, err := commandNameFor(desc, payload)
	if err != nil {
		return err
	}
	req := tellRequest{
		Command:  name,
		Payload:  payload,
		Envelope: c.newOutgoingMetadata("tell"),
	}
	c.applySignalDeadline(desc, name, &req.Envelope)
	fut := workflow.SignalExternalWorkflow(c.workflowCtx, ref.ID, "", tellRequestSignal, req)
	return fut.Get(c.workflowCtx, nil)
}

func (c *wfContext) applySignalDeadline(desc *actors.Description, command string, meta *actors.MessageMetadata) {
	if desc == nil || meta == nil {
		return
	}
	timeout := desc.SignalTimeouts[command]
	if timeout <= 0 {
		return
	}
	deadline := workflow.Now(c.workflowCtx).Add(timeout)
	if !meta.HasDeadline() || deadline.Before(meta.Deadline) {
		meta.Deadline = deadline
	}
}

func (c *wfContext) withWorkflowContext(ctx workflow.Context, fn func() (any, error)) (any, error) {
	prevCtx := c.workflowCtx
	prevLogger := c.logger
	c.workflowCtx = ctx
	if l := workflow.GetLogger(ctx); l != nil {
		c.logger = workflowLogger{logger: l, ctx: c}
	} else {
		c.logger = noopLogger{}
	}
	defer func() {
		c.workflowCtx = prevCtx
		c.logger = prevLogger
	}()
	return fn()
}

func (c *wfContext) withMessageMetadata(meta actors.MessageMetadata, fn func() (any, error)) (any, error) {
	prevMeta := c.messageMeta
	prevCorr := c.correlation
	meta = c.ensureMessageMetadata(meta)
	if err := c.enforceMessageMetadata(&meta); err != nil {
		return nil, err
	}
	c.messageMeta = meta
	c.correlation = meta.Correlation
	defer func() {
		c.messageMeta = prevMeta
		c.correlation = prevCorr
	}()
	return fn()
}

func (c *wfContext) ensureMessageMetadata(meta actors.MessageMetadata) actors.MessageMetadata {
	if meta.ID == "" {
		c.messageSeq++
		meta.ID = fmt.Sprintf("%s-msg-%d", c.ref.ID, c.messageSeq)
	}
	if meta.CorrelationID == "" {
		meta.CorrelationID = meta.ID
	}
	meta.Correlation = c.normalizeCorrelation(meta)
	return meta
}

func (c *wfContext) normalizeCorrelation(meta actors.MessageMetadata) actors.CorrelationData {
	corr := meta.Correlation.Clone()
	parent := c.correlation.Clone()
	if corr.SagaID == "" {
		corr.SagaID = parent.SagaID
	}
	if corr.TraceID == "" {
		switch {
		case parent.TraceID != "":
			corr.TraceID = parent.TraceID
		case meta.CorrelationID != "":
			corr.TraceID = meta.CorrelationID
		default:
			corr.TraceID = meta.ID
		}
	}
	if corr.ParentID == "" {
		if parent.TraceID != "" {
			corr.ParentID = parent.TraceID
		} else if meta.CorrelationID != "" {
			corr.ParentID = meta.CorrelationID
		}
	}
	if len(parent.Attributes) > 0 {
		if corr.Attributes == nil {
			corr.Attributes = make(map[string]string, len(parent.Attributes))
		}
		for k, v := range parent.Attributes {
			if _, ok := corr.Attributes[k]; !ok {
				corr.Attributes[k] = v
			}
		}
	}
	return corr
}

func (c *wfContext) enforceMessageMetadata(meta *actors.MessageMetadata) error {
	if meta == nil {
		return nil
	}
	if meta.HasDeadline() {
		now := workflow.Now(c.workflowCtx)
		if !now.Before(meta.Deadline) {
			return actors.NonRetryable(fmt.Errorf("%w: message %s", errMessageDeadlineExceeded, meta.ID))
		}
	}
	if meta.RetryBudgetSet {
		if meta.RetryBudget <= 0 {
			return actors.NonRetryable(fmt.Errorf("%w: message %s", errMessageRetryBudgetExceeded, meta.ID))
		}
		meta.RetryBudget--
	}
	return nil
}

func (c *wfContext) newOutgoingMetadata(prefix string, overrideID ...string) actors.MessageMetadata {
	c.callSeq++
	id := fmt.Sprintf("%s-%s-%d", c.ref.ID, prefix, c.callSeq)
	if len(overrideID) > 0 && overrideID[0] != "" {
		id = overrideID[0]
	}
	meta := actors.MessageMetadata{
		ID:            id,
		CorrelationID: id,
		Caller:        c.ref,
		Correlation:   c.correlation.Clone(),
	}
	if !c.messageMeta.Deadline.IsZero() {
		meta.Deadline = c.messageMeta.Deadline
	}
	if c.messageMeta.RetryBudgetSet {
		meta.RetryBudgetSet = true
		meta.RetryBudget = c.messageMeta.RetryBudget
	}
	if meta.Correlation.TraceID == "" {
		if parent := c.messageMeta.Correlation.TraceID; parent != "" {
			meta.Correlation.TraceID = parent
		} else {
			meta.Correlation.TraceID = id
		}
	}
	if meta.Correlation.ParentID == "" {
		if parent := c.messageMeta.Correlation.TraceID; parent != "" {
			meta.Correlation.ParentID = parent
		} else if corrID := c.messageMeta.CorrelationID; corrID != "" {
			meta.Correlation.ParentID = corrID
		}
	}
	if meta.Correlation.SagaID == "" {
		meta.Correlation.SagaID = c.correlation.SagaID
	}
	if meta.RetryBudget > 0 && !meta.RetryBudgetSet {
		meta.RetryBudgetSet = true
	}
	if len(c.correlation.Attributes) > 0 {
		if meta.Correlation.Attributes == nil {
			meta.Correlation.Attributes = make(map[string]string, len(c.correlation.Attributes))
		}
		for k, v := range c.correlation.Attributes {
			if _, ok := meta.Correlation.Attributes[k]; !ok {
				meta.Correlation.Attributes[k] = v
			}
		}
	}
	return meta
}

func (c *wfContext) Effect(key string, fn actors.EffectFunc, opts ...actors.EffectOption) (any, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("actors: effect key must be non-empty")
	}
	if fn == nil {
		return nil, fmt.Errorf("actors: effect function is nil")
	}
	c.ensureEffectLedger()
	now := workflow.Now(c.workflowCtx)
	if entry, ok := c.effects[key]; ok {
		if entry.ExpiresAt.IsZero() || entry.ExpiresAt.After(now) {
			return decodeEffectValue(entry.Payload)
		}
		delete(c.effects, key)
	}
	result, err := fn(c)
	if err != nil {
		return result, err
	}
	payload, err := encodeEffectValue(result)
	if err != nil {
		return nil, err
	}
	cfg := actors.EffectOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	var expires time.Time
	if cfg.TTL > 0 {
		expires = now.Add(cfg.TTL)
	}
	if c.effects == nil {
		c.effects = make(map[string]effectEntry)
	}
	c.effects[key] = effectEntry{Payload: payload, ExpiresAt: expires}
	if err := c.persistEffects(now); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *wfContext) ensureEffectLedger() {
	if c.effects != nil {
		return
	}
	c.effects = make(map[string]effectEntry)
	info := workflow.GetInfo(c.workflowCtx)
	if info == nil || info.Memo == nil || info.Memo.Fields == nil {
		return
	}
	payload, ok := info.Memo.Fields[effectsMemoKey]
	if !ok {
		return
	}
	var memo effectMemo
	if err := dataConverter().FromPayload(payload, &memo); err != nil {
		if logger := c.Logger(); logger != nil {
			logger.Error("decode effects memo", "error", err)
		}
		return
	}
	now := workflow.Now(c.workflowCtx)
	for _, entry := range memo.Entries {
		if entry.Key == "" {
			continue
		}
		if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now) {
			continue
		}
		c.effects[entry.Key] = effectEntry{Payload: entry.Payload, ExpiresAt: entry.ExpiresAt}
	}
}

func (c *wfContext) persistEffects(now time.Time) error {
	if c.effects == nil {
		return nil
	}
	memo := effectMemo{}
	for key, entry := range c.effects {
		if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now) {
			delete(c.effects, key)
			continue
		}
		memo.Entries = append(memo.Entries, effectMemoEntry{
			Key:       key,
			Payload:   entry.Payload,
			ExpiresAt: entry.ExpiresAt,
		})
	}
	return workflow.UpsertMemo(c.workflowCtx, map[string]any{effectsMemoKey: memo})
}

func encodeEffectValue(val any) ([]byte, error) {
	if val == nil {
		return nil, nil
	}
	return codec.Marshal(val)
}

func decodeEffectValue(payload []byte) (any, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var out any
	if err := codec.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

type effectEntry struct {
	Payload   []byte
	ExpiresAt time.Time
}

type effectMemo struct {
	Entries []effectMemoEntry
}

type effectMemoEntry struct {
	Key       string
	Payload   []byte
	ExpiresAt time.Time
}

type workflowLogger struct {
	logger log.Logger
	ctx    *wfContext
}

func (l workflowLogger) Debug(msg string, kv ...any) { l.logger.Debug(msg, l.withMeta(kv...)...) }
func (l workflowLogger) Info(msg string, kv ...any)  { l.logger.Info(msg, l.withMeta(kv...)...) }
func (l workflowLogger) Warn(msg string, kv ...any)  { l.logger.Warn(msg, l.withMeta(kv...)...) }
func (l workflowLogger) Error(msg string, kv ...any) { l.logger.Error(msg, l.withMeta(kv...)...) }

func (l workflowLogger) withMeta(kv ...any) []any {
	meta := []any{
		"actor.kind", l.ctx.ref.Kind,
		"actor.id", l.ctx.ref.ID,
	}
	if msgID := l.ctx.messageMeta.ID; msgID != "" {
		meta = append(meta, "actor.message_id", msgID)
	}
	if corr := l.ctx.messageMeta.CorrelationID; corr != "" {
		meta = append(meta, "actor.correlation_id", corr)
	}
	return append(meta, kv...)
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

func applySpawnOptions(opts ...actors.SpawnOption) actors.SpawnConfig {
	cfg := actors.SpawnConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func (c *wfContext) signalWorkflow(ref actors.Ref, signalName string, payload any) error {
	target := strings.TrimSpace(ref.Workflow)
	if target == "" {
		target = strings.TrimSpace(ref.ID)
	}
	if target == "" {
		return fmt.Errorf("actors: target ref is empty")
	}
	fut := workflow.SignalExternalWorkflow(c.workflowCtx, target, ref.RunID, signalName, payload)
	return fut.Get(c.workflowCtx, nil)
}

func (c *wfContext) targetWorkflowID() string {
	if wf := strings.TrimSpace(c.ref.Workflow); wf != "" {
		return wf
	}
	return strings.TrimSpace(c.ref.ID)
}

func commandNameFor(desc *actors.Description, payload any) (string, error) {
	typeKey := actors.TypeKeyOf(payload)
	if typeKey == "" {
		return "", fmt.Errorf("actors: cannot infer command type from payload %T", payload)
	}
	if name, ok := desc.CommandTypes[typeKey]; ok {
		return name, nil
	}
	return "", fmt.Errorf("actors: no command registered for %s on kind %s", typeKey, desc.Kind)
}

func queryNameFor(desc *actors.Description, payload any) (string, error) {
	typeKey := actors.TypeKeyOf(payload)
	if typeKey == "" {
		return "", fmt.Errorf("actors: cannot infer query type from payload %T", payload)
	}
	if name, ok := desc.QueryTypes[typeKey]; ok {
		return name, nil
	}
	return "", fmt.Errorf("actors: no query registered for %s on kind %s", typeKey, desc.Kind)
}
