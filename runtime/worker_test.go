package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tactors/sdk/actors"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestWorkerSetReusesQueues(t *testing.T) {
	stubs := map[string]*stubWorker{}
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		w := &stubWorker{queue: queue, opts: opts}
		stubs[queue] = w
		return w
	}, WorkerConfig{})

	alpha := actors.NewStateful("alpha", func() struct{} { return struct{}{} }).
		WithWorkflowQueue("shared").
		Build()
	bravo := actors.NewStateful("bravo", func() struct{} { return struct{}{} }).
		WithWorkflowQueue("shared").
		Build()

	if _, err := set.Register(alpha, WorkerConfig{}); err != nil {
		t.Fatalf("register alpha: %v", err)
	}
	if _, err := set.Register(bravo, WorkerConfig{}); err != nil {
		t.Fatalf("register bravo: %v", err)
	}
	if len(set.workflowWorkers) != 1 {
		t.Fatalf("expected single workflow worker, got %d", len(set.workflowWorkers))
	}
	if stubs["shared"] == nil {
		t.Fatalf("shared worker was not created")
	}
}

func TestWorkerSetStartAllStopsOnCancel(t *testing.T) {
	stubs := []*stubWorker{{}, {}}
	next := 0
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		w := stubs[next]
		w.opts = opts
		next++
		return w
	}, WorkerConfig{})

	actor := actors.NewStateful("queue-test", func() struct{} { return struct{}{} }).
		With(
			actors.ActivityNamed("noop", func(ctx context.Context, _ struct{}) (struct{}, error) {
				return struct{}{}, nil
			}),
		).
		Build()
	if _, err := set.Register(actor, WorkerConfig{}); err != nil {
		t.Fatalf("register actor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := set.StartAll(ctx); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	cancel()
	time.Sleep(10 * time.Millisecond)
	if stubs[0].startCount != 1 || stubs[0].stopCount == 0 {
		t.Fatalf("workflow worker start/stop mismatch: %+v", stubs[0])
	}
	if stubs[1].startCount != 1 || stubs[1].stopCount == 0 {
		t.Fatalf("activity worker start/stop mismatch: %+v", stubs[1])
	}
}

func TestWorkerSetStartAllIdempotent(t *testing.T) {
	stubs := []*stubWorker{{}, {}}
	next := 0
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		w := stubs[next]
		w.opts = opts
		next++
		return w
	}, WorkerConfig{})
	actor := actors.NewStateful("idempotent", func() struct{} { return struct{}{} }).
		With(
			actors.ActivityNamed("noop", func(ctx context.Context, _ struct{}) (struct{}, error) {
				return struct{}{}, nil
			}),
		).
		Build()
	if _, err := set.Register(actor, WorkerConfig{}); err != nil {
		t.Fatalf("register actor: %v", err)
	}
	ctx := context.Background()
	if err := set.StartAll(ctx); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	set.StopAll()
	if err := set.StartAll(ctx); err != nil {
		t.Fatalf("StartAll after stop: %v", err)
	}
	set.StopAll()
	if stubs[0].startCount != 2 || stubs[1].startCount != 2 {
		t.Fatalf("expected start per cycle, got %+v %+v", stubs[0], stubs[1])
	}
	if stubs[0].stopCount != 2 || stubs[1].stopCount != 2 {
		t.Fatalf("expected stop per cycle, got %+v %+v", stubs[0], stubs[1])
	}
}

func TestWorkerSetHealthSnapshot(t *testing.T) {
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		return &stubWorker{queue: queue, opts: opts}
	}, WorkerConfig{})

	alpha := actors.NewStateful("alpha", func() struct{} { return struct{}{} }).
		WithActivityQueue("alpha-activities").
		With(
			actors.ActivityNamed("noop", func(ctx context.Context, _ struct{}) (struct{}, error) {
				return struct{}{}, nil
			}),
		).Build()
	bravo := actors.NewStateful("bravo", func() struct{} { return struct{}{} }).
		WithWorkflowQueue("shared-wf").
		Build()
	if _, err := set.Register(alpha, WorkerConfig{}); err != nil {
		t.Fatalf("register alpha: %v", err)
	}
	if _, err := set.Register(bravo, WorkerConfig{}); err != nil {
		t.Fatalf("register bravo: %v", err)
	}
	snap := set.HealthSnapshot()
	if len(snap.WorkflowQueues) != 2 {
		t.Fatalf("expected two workflow queues, got %#v", snap.WorkflowQueues)
	}
	if len(snap.ActivityQueues) != 2 {
		t.Fatalf("expected two activity queues (one disabled), got %#v", snap.ActivityQueues)
	}
	for _, status := range snap.WorkflowQueues {
		if status.Running {
			t.Fatalf("expected workflow queue %s to be stopped initially", status.Queue)
		}
	}
	if err := set.StartAll(context.Background()); err != nil {
		t.Fatalf("start workers: %v", err)
	}
	snap = set.HealthSnapshot()
	for _, status := range snap.WorkflowQueues {
		if !status.Running {
			t.Fatalf("expected workflow queue %s running", status.Queue)
		}
		if status.Disabled {
			t.Fatalf("expected workflow queue %s enabled", status.Queue)
		}
		if len(status.ActorKinds) == 0 {
			t.Fatalf("workflow queue missing actor metadata: %#v", status)
		}
	}
	disabledActivity := 0
	for _, status := range snap.ActivityQueues {
		if status.Disabled {
			disabledActivity++
			if status.Running {
				t.Fatalf("expected disabled activity queue %s to stay stopped", status.Queue)
			}
		}
	}
	if disabledActivity != 1 {
		t.Fatalf("expected one disabled activity queue, got %d", disabledActivity)
	}
	set.StopAll()
	snap = set.HealthSnapshot()
	for _, status := range snap.WorkflowQueues {
		if status.Running {
			t.Fatalf("expected workflow queue %s stopped", status.Queue)
		}
	}
}

func TestWorkerSetStartError(t *testing.T) {
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		return &stubWorker{queue: queue, failStart: true, opts: opts}
	}, WorkerConfig{})
	actor := actors.NewStateful("fail-start", func() struct{} { return struct{}{} }).
		Build()
	if _, err := set.Register(actor, WorkerConfig{}); err != nil {
		t.Fatalf("register actor: %v", err)
	}
	err := set.StartAll(context.Background())
	if err == nil || !strings.Contains(err.Error(), "fail to start") {
		t.Fatalf("expected start error, got %v", err)
	}
}

func TestWorkerSetAutoDisablesActivityPollerForWorkflowOnlyActor(t *testing.T) {
	stubs := map[string]*stubWorker{}
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		w := &stubWorker{queue: queue, opts: opts}
		stubs[queue] = w
		return w
	}, WorkerConfig{})
	actor := actors.NewStateful("workflow-only", func() struct{} { return struct{}{} }).Build()

	reg, err := set.Register(actor, WorkerConfig{})
	if err != nil {
		t.Fatalf("register actor: %v", err)
	}
	if reg.ActivityQueue != "" {
		t.Fatalf("expected no activity queue, got %q", reg.ActivityQueue)
	}
	if len(set.activityWorkers) != 0 {
		t.Fatalf("expected no activity workers, got %d", len(set.activityWorkers))
	}
	wf := stubs[reg.WorkflowQueue]
	if wf == nil {
		t.Fatalf("workflow worker not created")
	}
	actQueue := activityQueueFor(actor.Kind(), actor.Spec())
	binding := set.activityBindings[actQueue]
	if binding == nil || !binding.disabled {
		t.Fatalf("expected activity queue %s to be marked disabled, got %#v", actQueue, binding)
	}
}

func TestWorkerSetAutoDisablesWorkflowPollerForActivityOnlyActor(t *testing.T) {
	stubs := map[string]*stubWorker{}
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		w := &stubWorker{queue: queue, opts: opts}
		stubs[queue] = w
		return w
	}, WorkerConfig{})
	actor := actors.New("activity-only").
		WithActivity("noop", func(ctx context.Context, _ any) (any, error) {
			return nil, nil
		}).
		Build()

	reg, err := set.Register(actor, WorkerConfig{})
	if err != nil {
		t.Fatalf("register actor: %v", err)
	}
	if reg.WorkflowQueue != "" {
		t.Fatalf("expected workflow queue omitted, got %q", reg.WorkflowQueue)
	}
	if len(set.workflowWorkers) != 0 {
		t.Fatalf("expected no workflow workers, got %d", len(set.workflowWorkers))
	}
	act := stubs[reg.ActivityQueue]
	if act == nil {
		t.Fatalf("activity worker not created")
	}
	if !act.opts.DisableWorkflowWorker {
		t.Fatalf("activity worker should disable workflow poller, opts=%+v", act.opts)
	}
	wfQueue := workflowQueueFor(actor.Kind(), actor.Spec())
	binding := set.workflowBindings[wfQueue]
	if binding == nil || !binding.disabled {
		t.Fatalf("expected workflow queue %s to be marked disabled, got %#v", wfQueue, binding)
	}
}

func TestWorkerSetSharedQueueUsesSingleWorker(t *testing.T) {
	stubs := map[string]*stubWorker{}
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		w := &stubWorker{queue: queue, opts: opts}
		stubs[queue] = w
		return w
	}, WorkerConfig{})
	actor := actors.NewStateful("shared", func() struct{} { return struct{}{} }).
		WithWorkflowQueue("shared").
		WithActivityQueue("shared").
		With(
			actors.ActivityNamed("noop", func(ctx context.Context, _ struct{}) (struct{}, error) {
				return struct{}{}, nil
			}),
		).
		Build()

	reg, err := set.Register(actor, WorkerConfig{})
	if err != nil {
		t.Fatalf("register actor: %v", err)
	}
	if reg.WorkflowQueue != "shared" || reg.ActivityQueue != "shared" {
		t.Fatalf("expected shared queues, got %+v", reg)
	}
	wf := set.workflowWorkers["shared"]
	act := set.activityWorkers["shared"]
	if wf == nil || act == nil {
		t.Fatalf("workers not created for shared queue")
	}
	if wf != act {
		t.Fatalf("expected single worker instance for shared queue")
	}
	stub, ok := wf.(*stubWorker)
	if !ok {
		t.Fatalf("worker type assertion failed: %T", wf)
	}
	if err := set.StartAll(context.Background()); err != nil {
		t.Fatalf("start workers: %v", err)
	}
	set.StopAll()
	if stub.startCount != 1 || stub.stopCount != 1 {
		t.Fatalf("expected single start/stop, got start=%d stop=%d", stub.startCount, stub.stopCount)
	}
	if len(stub.workflows) != 1 || len(stub.activities) != 1 {
		t.Fatalf("expected both workflow and activity registration, got %+v", stub)
	}
}

func TestWorkerSetDoesNotEnableSessionsByDefault(t *testing.T) {
	var created *stubWorker
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		created = &stubWorker{queue: queue, opts: opts}
		return created
	}, WorkerConfig{})
	actor := actors.NewStateful("with-session-default", func() struct{} { return struct{}{} }).
		Build()
	if _, err := set.Register(actor, WorkerConfig{}); err != nil {
		t.Fatalf("register actor: %v", err)
	}
	if created == nil {
		t.Fatalf("workflow worker not created")
	}
	if created.opts.EnableSessionWorker {
		t.Fatalf("expected session worker disabled by default, opts=%+v", created.opts)
	}
}

func TestWorkerSetRespectsSessionOptIn(t *testing.T) {
	var created *stubWorker
	set := newWorkerSetWithFactory(func(queue string, opts worker.Options) temporalWorker {
		created = &stubWorker{queue: queue, opts: opts}
		return created
	}, WorkerConfig{})
	actor := actors.NewStateful("session-optout", func() struct{} { return struct{}{} }).
		Build()
	cfg := WorkerConfig{
		WorkerOptions: &worker.Options{EnableSessionWorker: true},
	}
	if _, err := set.Register(actor, cfg); err != nil {
		t.Fatalf("register actor: %v", err)
	}
	if created == nil {
		t.Fatalf("workflow worker not created")
	}
	if !created.opts.EnableSessionWorker {
		t.Fatalf("expected session worker enabled when explicitly requested; opts=%+v", created.opts)
	}
}

type stubWorker struct {
	queue      string
	startCount int
	stopCount  int
	workflows  []string
	activities []string
	failStart  bool
	opts       worker.Options
}

func (s *stubWorker) RegisterWorkflow(w interface{}) {}

func (s *stubWorker) RegisterWorkflowWithOptions(w interface{}, options workflow.RegisterOptions) {
	s.workflows = append(s.workflows, options.Name)
}

func (s *stubWorker) RegisterActivity(a interface{}) {}

func (s *stubWorker) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	s.activities = append(s.activities, options.Name)
}

func (s *stubWorker) Start() error {
	s.startCount++
	if s.failStart {
		return fmt.Errorf("fail to start queue %s", s.queue)
	}
	return nil
}

func (s *stubWorker) Run(<-chan interface{}) error { return nil }

func (s *stubWorker) Stop() {
	s.stopCount++
}
