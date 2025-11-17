package ticketing

import (
	"testing"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/testkit"
)

func TestTicketingWorkflowScenario(t *testing.T) {
	scenario := testkit.NewActorTemporalScenario(TicketActor(), "ticket-wf", struct{}{})
	scenario.
		WhenCommand(OpenTicketCommand{ID: "ticket-1", Customer: "Neo"}).
		WhenCommand(AddIssueCommand{Issue: "Cannot find Oracle"}).
		WhenCommand(AssignAgentCommand{Agent: "Trinity"}).
		WhenCommand(CloseTicketCommand{}).
		WhenCommand(actors.StopCommand{}).
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("workflow error: %v", err)
			}
			value, err := scenario.QueryWorkflow(TicketStatusQuery{})
			if err != nil {
				tb.Fatalf("query: %v", err)
			}
			var status TicketStatusResponse
			if err := value.Get(&status); err != nil {
				tb.Fatalf("decode: %v", err)
			}
			if !status.Closed || status.ClosedAt.IsZero() {
				tb.Fatalf("expected closed ticket, got %+v", status)
			}
			if status.Assigned != "Trinity" {
				tb.Fatalf("unexpected agent: %s", status.Assigned)
			}
		}).
		Run(t)
}
