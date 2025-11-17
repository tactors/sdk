package runtime

import (
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/tactors/sdk/actors"
)

func BenchmarkTemporalCommandLoop(b *testing.B) {
	suite := testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	defer env.AssertExpectations(b)

	runner := registerActorWorkflow(b, env, newSelectorBenchActor())
	incSignal := actors.TypeKeyOf(selectorBenchCommand{})
	stopSignal := actors.TypeKeyOf(stopLoopCommand{})

	env.RegisterDelayedCallback(func() {
		for i := 0; i < b.N; i++ {
			env.SignalWorkflow(incSignal, selectorBenchCommand{})
		}
		env.SignalWorkflow(stopSignal, stopLoopCommand{})
	}, time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()
	env.ExecuteWorkflow(runner.Workflow(), fmt.Sprintf("selector-bench-%d", b.N), struct{}{})
	b.StopTimer()
}
