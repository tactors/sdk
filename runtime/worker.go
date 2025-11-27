package runtime

import (
	"context"
	"fmt"
	"sort"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/observability"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type temporalWorker interface {
	Start() error
	Stop()
	RegisterWorkflowWithOptions(interface{}, workflow.RegisterOptions)
	RegisterActivityWithOptions(interface{}, activity.RegisterOptions)
}

type WorkerConfig struct {
	WorkflowQueue         string
	ActivityQueue         string
	WorkerOptions         *worker.Options
	ActivityWorkerOptions *worker.Options
}

type WorkerSet struct {
	workflowWorkers  map[string]temporalWorker
	activityWorkers  map[string]temporalWorker
	workflowBindings map[string]*queueBinding
	activityBindings map[string]*queueBinding
	defaults         WorkerConfig
	newWorker        func(string, worker.Options) temporalWorker
}

type queueBinding struct {
	queue    string
	kinds    map[string]struct{}
	running  bool
	disabled bool
	reason   string
}

func newQueueBinding(queue string) *queueBinding {
	return &queueBinding{
		queue: queue,
		kinds: make(map[string]struct{}),
	}
}

func (b *queueBinding) kindList() []string {
	if b == nil || len(b.kinds) == 0 {
		return nil
	}
	out := make([]string, 0, len(b.kinds))
	for kind := range b.kinds {
		out = append(out, kind)
	}
	sort.Strings(out)
	return out
}

func (s *WorkerSet) ensureWorkflowBinding(queue string) *queueBinding {
	b := s.workflowBindings[queue]
	if b == nil {
		b = newQueueBinding(queue)
		s.workflowBindings[queue] = b
	}
	return b
}

func (s *WorkerSet) ensureActivityBinding(queue string) *queueBinding {
	b := s.activityBindings[queue]
	if b == nil {
		b = newQueueBinding(queue)
		s.activityBindings[queue] = b
	}
	return b
}

// WorkerQueueStatus captures queue-level health metrics.
type WorkerQueueStatus struct {
	Queue      string
	Running    bool
	Disabled   bool
	ActorKinds []string
}

// WorkerHealthSnapshot summarizes the worker fleet at a point in time.
type WorkerHealthSnapshot struct {
	WorkflowQueues []WorkerQueueStatus
	ActivityQueues []WorkerQueueStatus
}

// HealthSnapshot reports queue-level status for workflow and activity workers.
func (s *WorkerSet) HealthSnapshot() WorkerHealthSnapshot {
	return WorkerHealthSnapshot{
		WorkflowQueues: snapshotBindings(s.workflowBindings),
		ActivityQueues: snapshotBindings(s.activityBindings),
	}
}

func snapshotBindings(bindings map[string]*queueBinding) []WorkerQueueStatus {
	if len(bindings) == 0 {
		return nil
	}
	queues := make([]string, 0, len(bindings))
	for queue := range bindings {
		queues = append(queues, queue)
	}
	sort.Strings(queues)
	out := make([]WorkerQueueStatus, 0, len(queues))
	for _, queue := range queues {
		b := bindings[queue]
		out = append(out, WorkerQueueStatus{
			Queue:      queue,
			Running:    b.running,
			Disabled:   b.disabled,
			ActorKinds: b.kindList(),
		})
	}
	return out
}

type Registration struct {
	Kind          string
	WorkflowQueue string
	ActivityQueue string
}

func NewWorkerSet(c client.Client, defaults ...WorkerConfig) *WorkerSet {
	if c == nil {
		panic("temporal: client is nil")
	}
	var cfg WorkerConfig
	if len(defaults) > 0 {
		cfg = defaults[0]
	}
	return newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		return worker.New(c, queue, opts)
	}, cfg)
}

func newWorkerSetWithFactory(factory func(string, worker.Options) temporalWorker, defaults WorkerConfig) *WorkerSet {
	return &WorkerSet{
		workflowWorkers:  make(map[string]temporalWorker),
		activityWorkers:  make(map[string]temporalWorker),
		workflowBindings: make(map[string]*queueBinding),
		activityBindings: make(map[string]*queueBinding),
		defaults:         defaults,
		newWorker:        factory,
	}
}

func mergeWorkerOptions(primary, fallback *worker.Options) worker.Options {
	if primary != nil {
		return *primary
	}
	if fallback != nil {
		return *fallback
	}
	return worker.Options{}
}

func sharedWorkerOptions(cfg WorkerConfig, defaults WorkerConfig) worker.Options {
	if cfg.WorkerOptions != nil {
		return *cfg.WorkerOptions
	}
	if cfg.ActivityWorkerOptions != nil {
		return *cfg.ActivityWorkerOptions
	}
	if defaults.WorkerOptions != nil {
		return *defaults.WorkerOptions
	}
	if defaults.ActivityWorkerOptions != nil {
		return *defaults.ActivityWorkerOptions
	}
	return worker.Options{}
}

func resolveWorkflowQueue(kind string, desc *actors.Description, cfg WorkerConfig, defaults WorkerConfig) string {
	queue := cfg.WorkflowQueue
	if queue == "" && desc != nil {
		queue = desc.WorkflowQueue
	}
	if queue == "" {
		queue = defaults.WorkflowQueue
	}
	if queue == "" {
		queue = workflowQueueFor(kind, desc)
	}
	return queue
}

func resolveActivityQueue(kind string, desc *actors.Description, cfg WorkerConfig, defaults WorkerConfig) string {
	queue := cfg.ActivityQueue
	if queue == "" && desc != nil {
		queue = desc.ActivityQueue
	}
	if queue == "" {
		queue = defaults.ActivityQueue
	}
	if queue == "" {
		queue = activityQueueFor(kind, desc)
	}
	return queue
}

func (b *queueBinding) markEnabled(kind string) {
	b.disabled = false
	b.reason = ""
	b.kinds[kind] = struct{}{}
}

func (b *queueBinding) markDisabled(kind, reason string) {
	b.disabled = true
	b.reason = reason
	b.kinds[kind] = struct{}{}
}

func (s *WorkerSet) Register(actor actors.Actor, cfg WorkerConfig) (Registration, error) {
	if actor == nil {
		return Registration{}, fmt.Errorf("temporal: actor is nil")
	}
	runner := NewRunner(actor)
	desc := runner.Description()
	kind := desc.Kind
	activities := runner.Activities()
	hasActivities := len(activities) > 0
	hasWorkflow := desc.HasWorkflow()

	wfQueue := resolveWorkflowQueue(kind, desc, cfg, s.defaults)
	actQueue := resolveActivityQueue(kind, desc, cfg, s.defaults)
	sharedQueue := hasWorkflow && hasActivities && wfQueue == actQueue

	switch {
	case sharedQueue:
		opts := sharedWorkerOptions(cfg, s.defaults)
		opts.DisableWorkflowWorker = false
		worker := s.workflowWorkers[wfQueue]
		if worker == nil {
			worker = s.activityWorkers[actQueue]
		}
		if worker == nil {
			worker = s.newWorker(wfQueue, opts)
		}
		s.workflowWorkers[wfQueue] = worker
		s.activityWorkers[actQueue] = worker

		worker.RegisterWorkflowWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: kind})
		for name, fn := range activities {
			worker.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
		}
		s.ensureWorkflowBinding(wfQueue).markEnabled(kind)
		s.ensureActivityBinding(actQueue).markEnabled(kind)
	default:
		wfOpts := mergeWorkerOptions(cfg.WorkerOptions, s.defaults.WorkerOptions)
		if hasWorkflow {
			wfOpts.DisableWorkflowWorker = false
			wfWorker := s.workflowWorkers[wfQueue]
			if wfWorker == nil {
				wfWorker = s.newWorker(wfQueue, wfOpts)
				s.workflowWorkers[wfQueue] = wfWorker
			}
			wfWorker.RegisterWorkflowWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: kind})
			s.ensureWorkflowBinding(wfQueue).markEnabled(kind)
		} else {
			wfOpts.DisableWorkflowWorker = true
			s.ensureWorkflowBinding(wfQueue).markDisabled(kind, "no workflows registered")
		}

		actOpts := mergeWorkerOptions(cfg.ActivityWorkerOptions, s.defaults.ActivityWorkerOptions)
		if hasActivities {
			actOpts.DisableWorkflowWorker = !hasWorkflow
			actWorker := s.activityWorkers[actQueue]
			if actWorker == nil {
				actWorker = s.newWorker(actQueue, actOpts)
				s.activityWorkers[actQueue] = actWorker
			}
			for name, fn := range activities {
				actWorker.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
			}
			s.ensureActivityBinding(actQueue).markEnabled(kind)
		} else {
			actOpts.DisableWorkflowWorker = true
			s.ensureActivityBinding(actQueue).markDisabled(kind, "no activities registered")
		}
	}

	reg := Registration{Kind: kind, WorkflowQueue: wfQueue, ActivityQueue: actQueue}
	if !hasWorkflow {
		reg.WorkflowQueue = ""
	}
	if !hasActivities {
		reg.ActivityQueue = ""
	}
	return reg, nil
}

func (s *WorkerSet) StartAll(ctx context.Context) error {
	started := make(map[string]bool)
	for queue, w := range s.workflowWorkers {
		if w == nil {
			continue
		}
		if !started[queue] {
			if err := w.Start(); err != nil {
				observability.EmitWorkerEvent(observability.WorkerEvent{
					Queue: queue,
					Role:  observability.WorkerRoleWorkflow,
					Type:  observability.WorkerEventError,
					Error: err,
				})
				return fmt.Errorf("temporal: start workflow worker %q: %w", queue, err)
			}
			started[queue] = true
		}
		if binding := s.workflowBindings[queue]; binding != nil {
			if !binding.running {
				binding.running = true
				observability.EmitWorkerEvent(observability.WorkerEvent{
					Queue:      queue,
					Role:       observability.WorkerRoleWorkflow,
					Type:       observability.WorkerEventStart,
					ActorKinds: binding.kindList(),
				})
			}
		}
	}
	for queue, binding := range s.workflowBindings {
		if binding == nil || !binding.disabled {
			continue
		}
		observability.EmitWorkerEvent(observability.WorkerEvent{
			Queue:      queue,
			Role:       observability.WorkerRoleWorkflow,
			Type:       observability.WorkerEventDisabled,
			Reason:     binding.reason,
			ActorKinds: binding.kindList(),
		})
	}
	for queue, w := range s.activityWorkers {
		if w == nil {
			continue
		}
		if !started[queue] {
			if err := w.Start(); err != nil {
				observability.EmitWorkerEvent(observability.WorkerEvent{
					Queue: queue,
					Role:  observability.WorkerRoleActivity,
					Type:  observability.WorkerEventError,
					Error: err,
				})
				return fmt.Errorf("temporal: start activity worker %q: %w", queue, err)
			}
			started[queue] = true
		}
		if binding := s.activityBindings[queue]; binding != nil {
			if !binding.running {
				binding.running = true
				observability.EmitWorkerEvent(observability.WorkerEvent{
					Queue:      queue,
					Role:       observability.WorkerRoleActivity,
					Type:       observability.WorkerEventStart,
					ActorKinds: binding.kindList(),
				})
			}
		}
	}
	for queue, binding := range s.activityBindings {
		if binding == nil || !binding.disabled {
			continue
		}
		observability.EmitWorkerEvent(observability.WorkerEvent{
			Queue:      queue,
			Role:       observability.WorkerRoleActivity,
			Type:       observability.WorkerEventDisabled,
			Reason:     binding.reason,
			ActorKinds: binding.kindList(),
		})
	}
	if ctx != nil {
		go func() {
			<-ctx.Done()
			s.StopAll()
		}()
	}
	return nil
}

func (s *WorkerSet) StopAll() {
	stopped := make(map[string]bool)
	for queue, w := range s.workflowWorkers {
		if w != nil {
			if !stopped[queue] {
				w.Stop()
				stopped[queue] = true
			}
		}
		if binding := s.workflowBindings[queue]; binding != nil {
			if binding.running {
				binding.running = false
				observability.EmitWorkerEvent(observability.WorkerEvent{
					Queue:      queue,
					Role:       observability.WorkerRoleWorkflow,
					Type:       observability.WorkerEventStop,
					ActorKinds: binding.kindList(),
				})
			}
		}
	}
	for queue, w := range s.activityWorkers {
		if w != nil {
			if !stopped[queue] {
				w.Stop()
				stopped[queue] = true
			}
		}
		if binding := s.activityBindings[queue]; binding != nil {
			if binding.running {
				binding.running = false
				observability.EmitWorkerEvent(observability.WorkerEvent{
					Queue:      queue,
					Role:       observability.WorkerRoleActivity,
					Type:       observability.WorkerEventStop,
					ActorKinds: binding.kindList(),
				})
			}
		}
	}
}
