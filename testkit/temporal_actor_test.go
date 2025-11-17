package testkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
)

func TestActorTemporalScenarioAdvanceTriggersTimers(t *testing.T) {
	scenario := NewActorTemporalScenario(scenarioTestActor(), "scenario-advance", struct{}{})
	outcome := scenario.
		WhenCommand(scenarioSleepCommand{Delay: 5 * time.Second}).
		Advance(5 * time.Second).
		WhenCommand(scenarioStopCommand{}).
		Run(t)
	require.NoError(t, outcome.Error)
	value, err := scenario.QueryWorkflow(scenarioQuery{})
	require.NoError(t, err)
	var state scenarioState
	require.NoError(t, value.Get(&state))
	require.Equal(t, 1, state.Sleeps)
	require.Equal(t, 5*time.Second, state.LastDelay)
}

func TestActorTemporalScenarioSupportsRestartsAndAutoRegistration(t *testing.T) {
	actor := scenarioTestActor()
	first := NewActorTemporalScenario(actor, "scenario-restart", struct{}{})
	firstOutcome := first.
		WhenCommand(scenarioSleepCommand{Delay: time.Second}).
		Advance(time.Second).
		WhenCommand(scenarioStopCommand{}).
		Run(t)
	require.NoError(t, firstOutcome.Error)
	second := NewActorTemporalScenario(actor, "scenario-restart", struct{}{})
	secondOutcome := second.
		WhenCommand(scenarioStopCommand{}).
		Run(t)
	require.NoError(t, secondOutcome.Error)
	value, err := second.QueryWorkflow(scenarioQuery{})
	require.NoError(t, err)
	var state scenarioState
	require.NoError(t, value.Get(&state))
	require.Equal(t, []string{"stop"}, state.Commands)
}

type scenarioSleepCommand struct {
	actors.CommandMsg[struct{}]
	Delay time.Duration
}

type scenarioStopCommand struct {
	actors.CommandMsg[struct{}]
}

type scenarioQuery struct {
	actors.QueryMsg[scenarioState]
}

type scenarioState struct {
	Sleeps    int
	LastDelay time.Duration
	Commands  []string
}

func scenarioTestActor() actors.Actor {
	return actors.NewStateful("scenario-test", func() scenarioState { return scenarioState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *scenarioState, cmd scenarioSleepCommand) (struct{}, error) {
				if cmd.Delay > 0 {
					if err := ctx.Sleep(cmd.Delay); err != nil {
						return struct{}{}, err
					}
				}
				st.Sleeps++
				st.LastDelay = cmd.Delay
				st.Commands = append(st.Commands, "sleep")
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *scenarioState, _ scenarioStopCommand) (struct{}, error) {
				st.Commands = append(st.Commands, "stop")
				return struct{}{}, actors.ErrStopLoop
			}),
			actors.Query(func(ctx actors.Ctx, st scenarioState, _ scenarioQuery) (scenarioState, error) {
				return st, nil
			}),
		).
		Build()
}
