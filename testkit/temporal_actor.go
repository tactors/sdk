package testkit

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"github.com/tactors/sdk/runtime"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// ActorTemporalScenario wraps TemporalScenario for actor descriptions.
type ActorTemporalScenario struct {
	desc                 *actors.Description
	scenario             *TemporalScenario
	activityAsserts      map[string][]func(testing.TB, actors.ActivityCallOptions)
	activityObservations []activityObservation
	mu                   sync.Mutex
}

type activityObservation struct {
	name string
	opts actors.ActivityCallOptions
}

// NewActorTemporalScenario constructs a TemporalScenario backed by the given actor.
func NewActorTemporalScenario(actor actors.Actor, id string, init any) *ActorTemporalScenario {
	runner := runtime.NewRunner(actor)
	sc := NewTemporalScenarioWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: runner.Description().Kind}, id, init)
	for name, fn := range runner.Activities() {
		sc.Env().RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	sc.Env().SetStartWorkflowOptions(client.StartWorkflowOptions{ID: id})
	out := &ActorTemporalScenario{desc: runner.Description(), scenario: sc}
	runner.AddActivityObserver(out.observeActivityOptions)
	return out
}

// Env exposes the underlying TemporalScenario environment for additional setup.
func (s *ActorTemporalScenario) Env() *testsuite.TestWorkflowEnvironment {
	return s.scenario.Env()
}

// WhenCommand sends a typed command to the workflow.
func (s *ActorTemporalScenario) WhenCommand(payload any) *ActorTemporalScenario {
	name, err := s.commandNameForPayload(payload)
	if err != nil {
		panic(err)
	}
	return s.WhenCommandNamed(name, payload)
}

// WhenCommandNamed sends a command to an explicit runtime route name.
func (s *ActorTemporalScenario) WhenCommandNamed(name string, payload any) *ActorTemporalScenario {
	s.scenario.When(name, payload)
	return s
}

// Advance advances the Temporal clock.
func (s *ActorTemporalScenario) Advance(d time.Duration) *ActorTemporalScenario {
	s.scenario.Advance(d)
	return s
}

// Then registers assertions.
func (s *ActorTemporalScenario) Then(asserts ...TemporalAssertion) *ActorTemporalScenario {
	s.scenario.Then(asserts...)
	return s
}

// Run executes the scenario.
func (s *ActorTemporalScenario) Run(t testing.TB) TemporalOutcome {
	outcome := s.scenario.Run(t)
	s.applyActivityAsserts(t)
	return outcome
}

// WhenActivity overrides a registered activity.
// Accepts either (name, fn) or a typed func(context.Context, Payload) to infer the name from the payload type.
func (s *ActorTemporalScenario) WhenActivity(nameOrFn interface{}, fnOpt ...interface{}) *ActorTemporalScenario {
	if len(fnOpt) == 0 {
		name, err := s.activityNameForFunction(nameOrFn)
		if err != nil {
			panic(err)
		}
		s.Env().RegisterActivityWithOptions(nameOrFn, activity.RegisterOptions{Name: name})
		return s
	}
	name, ok := nameOrFn.(string)
	if !ok {
		panic("testkit: WhenActivity first argument must be name string when passing two parameters")
	}
	if len(fnOpt) != 1 {
		panic("testkit: WhenActivity expects exactly one function argument")
	}
	s.Env().RegisterActivityWithOptions(fnOpt[0], activity.RegisterOptions{Name: name})
	return s
}

// WhenActivityForPayload overrides an activity by inferred payload type.
func (s *ActorTemporalScenario) WhenActivityForPayload(payload any, fn interface{}) *ActorTemporalScenario {
	name, err := s.activityNameForPayload(payload)
	if err != nil {
		panic(err)
	}
	return s.WhenActivity(name, fn)
}

// ExpectActivityOptions asserts the merged Temporal options for an activity name.
func (s *ActorTemporalScenario) ExpectActivityOptions(name string, assert func(testing.TB, actors.ActivityCallOptions)) *ActorTemporalScenario {
	if s.activityAsserts == nil {
		s.activityAsserts = make(map[string][]func(testing.TB, actors.ActivityCallOptions))
	}
	s.activityAsserts[name] = append(s.activityAsserts[name], assert)
	return s
}

// ExpectActivityOptionsForPayload asserts merged options using the payload type to resolve the activity name.
func (s *ActorTemporalScenario) ExpectActivityOptionsForPayload(payload any, assert func(testing.TB, actors.ActivityCallOptions)) *ActorTemporalScenario {
	name, err := s.activityNameForPayload(payload)
	if err != nil {
		panic(err)
	}
	return s.ExpectActivityOptions(name, assert)
}

func (s *ActorTemporalScenario) activityNameForPayload(payload any) (string, error) {
	typ := actors.TypeKeyOf(payload)
	if typ == "" {
		return "", fmt.Errorf("testkit: payload %T missing type metadata", payload)
	}
	if name, ok := s.desc.ActivityNames[typ]; ok && name != "" {
		return name, nil
	}
	return "", fmt.Errorf("testkit: no activity for payload type %s", typ)
}

func (s *ActorTemporalScenario) activityNameForFunction(fn interface{}) (string, error) {
	if fn == nil {
		return "", fmt.Errorf("testkit: activity function is nil")
	}
	typ := reflect.TypeOf(fn)
	if typ.Kind() != reflect.Func {
		return "", fmt.Errorf("testkit: activity override must be a function, got %T", fn)
	}
	if typ.NumIn() < 2 {
		return "", fmt.Errorf("testkit: activity override must accept context and payload, got %d args", typ.NumIn())
	}
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !typ.In(0).Implements(ctxType) {
		return "", fmt.Errorf("testkit: activity override first arg must implement context.Context, got %s", typ.In(0))
	}
	payloadZero := reflect.Zero(typ.In(1)).Interface()
	return s.activityNameForPayload(payloadZero)
}

func (s *ActorTemporalScenario) observeActivityOptions(name string, opts actors.ActivityCallOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activityObservations = append(s.activityObservations, activityObservation{name: name, opts: opts})
}

func (s *ActorTemporalScenario) commandNameForPayload(payload any) (string, error) {
	typ := actors.TypeKeyOf(payload)
	if typ == "" {
		return "", fmt.Errorf("testkit: payload %T missing type metadata", payload)
	}
	if name, ok := s.desc.CommandTypes[typ]; ok {
		return name, nil
	}
	return "", fmt.Errorf("testkit: no command for payload type %s", typ)
}

func (s *ActorTemporalScenario) queryNameForPayload(payload any) (string, error) {
	typ := actors.TypeKeyOf(payload)
	if typ == "" {
		return "", fmt.Errorf("testkit: payload %T missing type metadata", payload)
	}
	if name, ok := s.desc.QueryTypes[typ]; ok {
		return name, nil
	}
	return "", fmt.Errorf("testkit: no query for payload type %s", typ)
}

// QueryWorkflow executes a typed query against the workflow.
func (s *ActorTemporalScenario) QueryWorkflow(payload any) (converter.EncodedValue, error) {
	name, err := s.queryNameForPayload(payload)
	if err != nil {
		var empty converter.EncodedValue
		return empty, err
	}
	return s.QueryWorkflowNamed(name, payload)
}

// QueryWorkflowNamed executes a query against an explicit runtime route name.
func (s *ActorTemporalScenario) QueryWorkflowNamed(name string, payload any) (converter.EncodedValue, error) {
	var blob []byte
	var err error
	if payload != nil {
		blob, err = codec.Marshal(payload)
		if err != nil {
			var empty converter.EncodedValue
			return empty, err
		}
	}
	return s.Env().QueryWorkflow(name, blob)
}

func (s *ActorTemporalScenario) applyActivityAsserts(t testing.TB) {
	t.Helper()
	if len(s.activityAsserts) == 0 {
		return
	}
	s.mu.Lock()
	obs := append([]activityObservation(nil), s.activityObservations...)
	s.mu.Unlock()
	for name, asserts := range s.activityAsserts {
		var seen []actors.ActivityCallOptions
		for _, ob := range obs {
			if ob.name == name {
				seen = append(seen, ob.opts)
			}
		}
		if len(seen) < len(asserts) {
			t.Fatalf("expected at least %d calls for activity %q, saw %d", len(asserts), name, len(seen))
		}
		for i, assert := range asserts {
			assert(t, seen[i])
		}
	}
}
