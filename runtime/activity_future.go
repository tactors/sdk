package runtime

import "go.temporal.io/sdk/workflow"

type wfActivityFuture struct {
	activityCtx workflow.Context
	future      workflow.Future
}

func (f wfActivityFuture) Get() (any, error) {
	var out any
	err := f.future.Get(f.activityCtx, &out)
	return out, err
}
