package hellosystem

import (
	"testing"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/testkit"
)

func TestHelloSystemWorkflowScenario(t *testing.T) {
	scenario := testkit.NewActorTemporalScenario(SystemActor(), "system-wf", struct{}{})
	scenario.
		WhenCommand(SetValueCommand{Value: "hello"}).
		WhenCommand(actors.StopCommand{}).
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("workflow error: %v", err)
			}
			value, err := scenario.QueryWorkflow(GetValueQuery{})
			if err != nil {
				tb.Fatalf("query: %v", err)
			}
			var resp GetValueResponse
			if err := value.Get(&resp); err != nil {
				tb.Fatalf("decode: %v", err)
			}
			if resp.Value != "hello" {
				tb.Fatalf("unexpected value: %s", resp.Value)
			}
		}).
		Run(t)
}
