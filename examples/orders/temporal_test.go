package orders

import (
	"testing"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/testkit"
)

func TestOrderWorkflowScenario(t *testing.T) {
	scenario := testkit.NewActorTemporalScenario(OrderActor(), "orders-wf", struct{}{})
	scenario.
		WhenCommand(StartOrderCommand{ID: "order-test", Customer: "Neo"}).
		WhenCommand(AddItemCommand{Item: "Red Pill"}).
		WhenCommand(SubmitOrderCommand{}).
		WhenCommand(actors.StopCommand{}).
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("workflow error: %v", err)
			}
			resp, err := scenario.QueryWorkflow(OrderStatusQuery{})
			if err != nil {
				tb.Fatalf("query status: %v", err)
			}
			var status OrderStatusResponse
			if err := resp.Get(&status); err != nil {
				tb.Fatalf("decode status: %v", err)
			}
			if !status.Submitted || status.SubmittedAt.IsZero() {
				tb.Fatalf("expected submitted status, got %+v", status)
			}
			if len(status.Items) != 1 {
				tb.Fatalf("expected 1 item, got %d", len(status.Items))
			}
		}).
		Run(t)
}
