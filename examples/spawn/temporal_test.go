package spawn

import (
	"testing"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/runtime"
	"github.com/tactors/sdk/testkit"
	"go.temporal.io/sdk/workflow"
)

func TestSpawnParentTemporal(t *testing.T) {
	parent := ParentActor()
	child := ChildActor()

	// Scenario 1: long-lived child + notify.
	scenario := testkit.NewActorTemporalScenario(parent, "spawn-parent-long", struct{}{})
	childRunner := runtime.NewRunner(child)
	scenario.Env().RegisterWorkflowWithOptions(childRunner.Workflow(), workflow.RegisterOptions{Name: childRunner.Description().Kind})

	scenario.
		WhenCommand(SpawnChildCommand{ChildID: "child-1", Value: "neo"}).
		WhenCommand(NotifyChildCommand{ChildID: "child-1", Value: "trinity"}).
		WhenCommand(SnapshotChildCommand{ChildID: "child-1"}).
		WhenCommand(ComputeViaChildCommand{ChildID: "child-1", Value: "neo"}).
		WhenCommand(actors.StopCommand{}).
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("parent workflow error: %v", err)
			}
			resp, err := scenario.QueryWorkflow(ParentStateQuery{})
			if err != nil {
				tb.Fatalf("query parent: %v", err)
			}
			var state ParentStateResponse
			if err := resp.Get(&state); err != nil {
				tb.Fatalf("decode parent state: %v", err)
			}
			if len(state.Children) != 1 || state.Children[0] != "child-1" {
				tb.Fatalf("expected children [child-1], got %v", state.Children)
			}
			if state.LastResult != "NEO" {
				tb.Fatalf("expected last result NEO, got %s", state.LastResult)
			}
			if state.LastNotify != "TRINITY" {
				tb.Fatalf("expected last notify TRINITY, got %s", state.LastNotify)
			}
			if state.LastSnapshot != "TRINITY" {
				tb.Fatalf("expected last snapshot TRINITY, got %s", state.LastSnapshot)
			}
		}).
		Run(t)

	// Scenario 2: one-shot child returns typed response without keeping child alive.
	oneshotScenario := testkit.NewActorTemporalScenario(parent, "spawn-parent-oneshot", struct{}{})
	oneshotScenario.Env().RegisterWorkflowWithOptions(childRunner.Workflow(), workflow.RegisterOptions{Name: childRunner.Description().Kind})
	oneshotScenario.
		WhenCommand(ComputeViaChildCommand{Value: "morpheus"}).
		WhenCommand(actors.StopCommand{}).
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("oneshot workflow error: %v", err)
			}
			resp, err := oneshotScenario.QueryWorkflow(ParentStateQuery{})
			if err != nil {
				tb.Fatalf("query oneshot parent: %v", err)
			}
			var state ParentStateResponse
			if err := resp.Get(&state); err != nil {
				tb.Fatalf("decode oneshot parent: %v", err)
			}
			if state.LastResult != "MORPHEUS" {
				tb.Fatalf("expected MORPHEUS, got %s", state.LastResult)
			}
		}).
		Run(t)
}
