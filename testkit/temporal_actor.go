package testkit

import (
	"fmt"
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
	desc     *actors.Description
	scenario *TemporalScenario
}

// NewActorTemporalScenario constructs a TemporalScenario backed by the given actor.
func NewActorTemporalScenario(actor actors.Actor, id string, init any) *ActorTemporalScenario {
	runner := runtime.NewRunner(actor)
	sc := NewTemporalScenarioWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: runner.Description().Kind}, id, init)
	for name, fn := range runner.Activities() {
		sc.Env().RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	sc.Env().SetStartWorkflowOptions(client.StartWorkflowOptions{ID: id})
	return &ActorTemporalScenario{desc: runner.Description(), scenario: sc}
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
	return outcome
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
	var blob []byte
	if payload != nil {
		blob, err = codec.Marshal(payload)
		if err != nil {
			var empty converter.EncodedValue
			return empty, err
		}
	}
	return s.Env().QueryWorkflow(name, blob)
}
