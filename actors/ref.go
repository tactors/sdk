package actors

import (
	"fmt"
	"runtime"
	"strings"
)

// Ref uniquely identifies an actor instance.
type Ref struct {
	Workflow     string
	Kind         string
	ID           string
	Tenant       string
	WorkflowType string
	TaskQueue    string
	RunID        string
	StartArgs    []any
}

// RefOption customizes a Ref while constructing helper references.
type RefOption func(*Ref)

// ARef constructs a Ref whose workflow type and task queue default to the actor kind.
func ARef(kind, id string, opts ...RefOption) Ref {
	if isSystemKind(kind) && !systemKindCallerAllowed() {
		panic(fmt.Sprintf("actors: ref kind %q is reserved for system actors", kind))
	}
	ref := Ref{
		Workflow:     id,
		Kind:         kind,
		ID:           id,
		WorkflowType: kind,
		TaskQueue:    kind,
		StartArgs:    []any{id},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&ref)
		}
	}
	return ref
}

// TRef constructs a tenant-aware reference (workflow type equals kind).
func TRef(tenant, kind, id string, opts ...RefOption) Ref {
	ref := ARef(kind, id, opts...)
	ref.Tenant = tenant
	return ref
}

// WithWorkflowType overrides the workflow type used when auto-starting actors.
func WithWorkflowType(name string) RefOption {
	return func(r *Ref) {
		r.WorkflowType = name
	}
}

// WithTaskQueue overrides the task queue used when auto-starting actors.
func WithTaskQueue(queue string) RefOption {
	return func(r *Ref) {
		r.TaskQueue = queue
	}
}

// WithStartArgs overrides the arguments used when starting actor workflows.
func WithStartArgs(args ...any) RefOption {
	return func(r *Ref) {
		if len(args) == 0 {
			r.StartArgs = nil
			return
		}
		r.StartArgs = append([]any(nil), args...)
	}
}

// WithRunID pins the reference to a specific workflow run.
func WithRunID(runID string) RefOption {
	return func(r *Ref) {
		r.RunID = runID
	}
}

// StartPayload returns the arguments that should be supplied if the workflow needs to be started.
func (r Ref) StartPayload() []any {
	if len(r.StartArgs) > 0 {
		return append([]any(nil), r.StartArgs...)
	}
	if r.ID == "" {
		return nil
	}
	return []any{r.ID}
}

// Empty reports whether the reference is unset.
func (r Ref) Empty() bool {
	return r.Workflow == "" && r.Kind == "" && r.ID == ""
}

func isSystemKind(kind string) bool {
	return strings.HasPrefix(kind, "sys.")
}

var systemKindAllowedPrefixes = []string{
	"github.com/tactors/sdk/runtime",
}

func systemKindCallerAllowed() bool {
	pcs := make([]uintptr, 16)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		fn := frame.Function
		for _, prefix := range systemKindAllowedPrefixes {
			if strings.HasPrefix(fn, prefix) {
				return true
			}
		}
		if !more {
			break
		}
	}
	return false
}
