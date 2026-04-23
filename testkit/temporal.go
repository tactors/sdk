package testkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/tactors/sdk/runtime"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// TemporalScenario orchestrates a real Temporal workflow inside the
// Temporal testsuite environment using a declarative step API.
type TemporalScenario struct {
	env           *testsuite.TestWorkflowEnvironment
	workflow      interface{}
	args          []any
	steps         []temporalStep
	assertions    []TemporalAssertion
	signalsMocked bool
}

type temporalStepKind int

const (
	stepTemporalSignal temporalStepKind = iota
	stepTemporalAdvance
)

type temporalStep struct {
	kind     temporalStepKind
	name     string
	payload  any
	duration time.Duration
}

// NewTemporalScenario constructs a scenario for the provided workflow.
func NewTemporalScenario(wf interface{}, args ...any) *TemporalScenario {
	return NewTemporalScenarioWithOptions(wf, workflow.RegisterOptions{}, args...)
}

// NewTemporalScenarioWithOptions registers the workflow using Temporal register options.
func NewTemporalScenarioWithOptions(workflowFn interface{}, opts workflow.RegisterOptions, args ...any) *TemporalScenario {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(runtime.DataConverter())
	if opts.Name != "" {
		env.RegisterWorkflowWithOptions(workflowFn, opts)
	} else {
		env.RegisterWorkflow(workflowFn)
	}
	return &TemporalScenario{env: env, workflow: workflowFn, args: args}
}

// Env exposes the underlying TestWorkflowEnvironment for additional setup.
func (s *TemporalScenario) Env() *testsuite.TestWorkflowEnvironment {
	return s.env
}

// When schedules a signal (command) delivery.
func (s *TemporalScenario) When(name string, payload any) *TemporalScenario {
	s.steps = append(s.steps, temporalStep{kind: stepTemporalSignal, name: name, payload: payload})
	return s
}

// Advance moves the fake clock forward deterministically.
func (s *TemporalScenario) Advance(d time.Duration) *TemporalScenario {
	if d < 0 {
		d = 0
	}
	s.steps = append(s.steps, temporalStep{kind: stepTemporalAdvance, duration: d})
	return s
}

// Then registers assertions executed after workflow completion.
func (s *TemporalScenario) Then(asserts ...TemporalAssertion) *TemporalScenario {
	s.assertions = append(s.assertions, asserts...)
	return s
}

// Run executes the scenario and returns the outcome.
func (s *TemporalScenario) Run(t testing.TB) TemporalOutcome {
	t.Helper()
	s.ensureSignalMock()
	s.scheduleSteps()
	s.env.ExecuteWorkflow(s.workflow, s.args...)
	outcome := TemporalOutcome{Env: s.env, Error: s.env.GetWorkflowError()}
	for _, assert := range s.assertions {
		assert(t, outcome)
	}
	return outcome
}

func (s *TemporalScenario) scheduleSteps() {
	const epsilon = time.Millisecond
	offset := epsilon
	for _, st := range s.steps {
		switch st.kind {
		case stepTemporalSignal:
			name := st.name
			payload := st.payload
			delay := offset
			s.env.RegisterDelayedCallback(func() {
				s.env.SignalWorkflow(name, payload)
			}, delay)
			offset += epsilon
		case stepTemporalAdvance:
			offset += st.duration
			delay := offset
			s.env.RegisterDelayedCallback(func() {}, delay)
			offset += epsilon
		}
	}
}

func (s *TemporalScenario) ensureSignalMock() {
	if s.signalsMocked {
		return
	}
	s.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.signalsMocked = true
}

// TemporalOutcome captures workflow execution artifacts for assertions.
type TemporalOutcome struct {
	Env   *testsuite.TestWorkflowEnvironment
	Error error
}

// TemporalAssertion inspects the workflow outcome.
type TemporalAssertion func(testing.TB, TemporalOutcome)
