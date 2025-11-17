package controlactor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/testkit"
)

func TestIntervalActorTemporalScenario(t *testing.T) {
	actor := IntervalActor()
	scenario := testkit.NewActorTemporalScenario(actor, "control-actor", struct{}{})
	scenario.
		WhenCommand(TickCommand{Schedule: "heartbeat", Every: time.Minute}).
		Advance(time.Minute).
		WhenCommand(TickCommand{Schedule: "heartbeat", Every: time.Minute}).
		WhenCommand(actors.StopCommand{}).
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("control workflow error: %v", err)
			}
			resp, err := scenario.QueryWorkflow(StatusQuery{})
			require.NoError(tb, err)
			var state State
			require.NoError(tb, resp.Get(&state))
			require.Equal(tb, 2, state.RunCount)
			require.False(tb, state.LastRun.IsZero())
		}).
		Run(t)
}
