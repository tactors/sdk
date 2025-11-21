package runtime

import (
	"context"

	"github.com/tactors/sdk/actors"
	"go.temporal.io/sdk/workflow"
)

// Runner wires an actor description into a Temporal workflow entrypoint.
type Runner struct {
	desc *actors.Description
}

// NewRunner clones the actor description so the runtime can own its copy.
func NewRunner(actor actors.Actor) *Runner {
	clone := actor.Spec().Clone()
	actors.RegisterDescription(clone)
	return &Runner{desc: clone}
}

// Workflow returns a Temporal workflow function that drives the actor description.
func (r *Runner) Workflow() interface{} {
	return func(ctx workflow.Context, id string, init any) (any, error) {
		inst := newTemporalInstance(r.desc)
		return inst.run(ctx, id, init)
	}
}

// RegisterWorkflow is a helper that registers the workflow with a worker/tests suite.
func (r *Runner) RegisterWorkflow(register func(workflow interface{})) {
	register(r.Workflow())
}

// Activities returns wrappers suitable for registering with Temporal workers or tests.
func (r *Runner) Activities() map[string]interface{} {
	acts := make(map[string]interface{}, len(r.desc.Activities))
	for name, spec := range r.desc.Activities {
		localFn := spec.Handler
		acts[name] = func(ctx context.Context, payload any) (any, error) {
			return localFn(ctx, payload)
		}
	}
	return acts
}

// Description returns a clone of the underlying description.
func (r *Runner) Description() *actors.Description {
	return r.desc.Clone()
}

// AddActivityObserver registers a hook invoked with merged activity call options before execution.
func (r *Runner) AddActivityObserver(observer func(string, actors.ActivityCallOptions)) {
	if observer == nil {
		return
	}
	r.desc.ActivityObservers = append(r.desc.ActivityObservers, observer)
}
