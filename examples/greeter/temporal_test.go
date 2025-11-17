package greeter

import (
	"testing"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/testkit"
)

func TestGreeterTemporalWorkflow(t *testing.T) {
	scenario := testkit.NewActorTemporalScenario(GreeterActor(), "greeter-wf", struct{}{})
	scenario.WhenCommand(HelloCommand{Name: "Neo"}).
		WhenCommand(HelloCommand{Name: "Trinity"}).
		WhenCommand(actors.StopCommand{}).
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("workflow error: %v", err)
			}
			queryResult, err := scenario.QueryWorkflow(StatusQuery{})
			if err != nil {
				tb.Fatalf("query: %v", err)
			}
			var status StatusResponse
			if err := queryResult.Get(&status); err != nil {
				tb.Fatalf("decode query: %v", err)
			}
			if status.Count != 2 {
				tb.Fatalf("expected count 2, got %d", status.Count)
			}
		}).
		Run(t)
}
