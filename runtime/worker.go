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
	queue   string
	kinds   map[string]struct{}
	running bool
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

func (s *WorkerSet) Register(actor actors.Actor, cfg WorkerConfig) (Registration, error) {
	if actor == nil {
		return Registration{}, fmt.Errorf("temporal: actor is nil")
	}
	runner := NewRunner(actor)
	desc := runner.Description()
	kind := desc.Kind
	wfQueue := cfg.WorkflowQueue
	if wfQueue == "" {
		wfQueue = desc.WorkflowQueue
	}
	if wfQueue == "" {
		wfQueue = s.defaults.WorkflowQueue
	}
	if wfQueue == "" {
		wfQueue = workflowQueueFor(kind, desc)
	}
	wfWorker := s.workflowWorkers[wfQueue]
	if wfWorker == nil {
		opts := worker.Options{}
		if cfg.WorkerOptions != nil {
			opts = *cfg.WorkerOptions
		} else if s.defaults.WorkerOptions != nil {
			opts = *s.defaults.WorkerOptions
		}
		wfWorker = s.newWorker(wfQueue, opts)
		s.workflowWorkers[wfQueue] = wfWorker
	}
	wfWorker.RegisterWorkflowWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: kind})
	s.ensureWorkflowBinding(wfQueue).kinds[kind] = struct{}{}

	var actQueue string
	if len(runner.Activities()) > 0 {
		actQueue = cfg.ActivityQueue
		if actQueue == "" {
			actQueue = desc.ActivityQueue
		}
		if actQueue == "" {
			actQueue = s.defaults.ActivityQueue
		}
		if actQueue == "" {
			actQueue = activityQueueFor(kind, desc)
		}
		actWorker := s.activityWorkers[actQueue]
		if actWorker == nil {
			opts := worker.Options{}
			if cfg.ActivityWorkerOptions != nil {
				opts = *cfg.ActivityWorkerOptions
			} else if s.defaults.ActivityWorkerOptions != nil {
				opts = *s.defaults.ActivityWorkerOptions
			}
			actWorker = s.newWorker(actQueue, opts)
			s.activityWorkers[actQueue] = actWorker
		}
		for name, fn := range runner.Activities() {
			actWorker.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
		}
		s.ensureActivityBinding(actQueue).kinds[kind] = struct{}{}
	}
	return Registration{Kind: kind, WorkflowQueue: wfQueue, ActivityQueue: actQueue}, nil
}

func (s *WorkerSet) StartAll(ctx context.Context) error {
	for queue, w := range s.workflowWorkers {
		if err := w.Start(); err != nil {
			observability.EmitWorkerEvent(observability.WorkerEvent{
				Queue: queue,
				Role:  observability.WorkerRoleWorkflow,
				Type:  observability.WorkerEventError,
				Error: err,
			})
			return fmt.Errorf("temporal: start workflow worker %q: %w", queue, err)
		}
		if binding := s.workflowBindings[queue]; binding != nil {
			binding.running = true
			observability.EmitWorkerEvent(observability.WorkerEvent{
				Queue:      queue,
				Role:       observability.WorkerRoleWorkflow,
				Type:       observability.WorkerEventStart,
				ActorKinds: binding.kindList(),
			})
		}
	}
	for queue, w := range s.activityWorkers {
		if w == nil {
			continue
		}
		if err := w.Start(); err != nil {
			observability.EmitWorkerEvent(observability.WorkerEvent{
				Queue: queue,
				Role:  observability.WorkerRoleActivity,
				Type:  observability.WorkerEventError,
				Error: err,
			})
			return fmt.Errorf("temporal: start activity worker %q: %w", queue, err)
		}
		if binding := s.activityBindings[queue]; binding != nil {
			binding.running = true
			observability.EmitWorkerEvent(observability.WorkerEvent{
				Queue:      queue,
				Role:       observability.WorkerRoleActivity,
				Type:       observability.WorkerEventStart,
				ActorKinds: binding.kindList(),
			})
		}
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
	for queue, w := range s.workflowWorkers {
		if w != nil {
			w.Stop()
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
			w.Stop()
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
