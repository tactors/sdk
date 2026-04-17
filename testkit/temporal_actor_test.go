package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
)

type stubActivity struct {
	actors.ActivityMsg[string]
	Value string
}

type stubCommand struct {
	actors.CommandMsg[struct{}]
	Value string
}

type stubQuery struct {
	actors.QueryMsg[string]
}

type stubState struct {
	Last string
}

func TestActorScenarioStubsActivitiesAndAssertsOptions(t *testing.T) {
	actor := actors.NewStateful("stub-actor", func() stubState { return stubState{} }).
		With(
			actors.Activity(func(ctx context.Context, a stubActivity) (string, error) {
				return "real-" + a.Value, nil
			}, actors.WithActivityDefaults(
				actors.WithActivityStartToClose(3*time.Second),
				actors.WithActivityRetry(actors.RetryPolicy{MaxAttempts: 2, InitialInterval: time.Second}),
				actors.WithActivityTaskQueue("custom-activity"),
			)),
			actors.Command(func(ctx actors.Ctx, st *stubState, cmd stubCommand) (struct{}, error) {
				val, err := actors.RunActivity(ctx, stubActivity{Value: cmd.Value}, actors.WithActivityScheduleToClose(5*time.Second))
				if err != nil {
					return struct{}{}, err
				}
				st.Last = val
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st stubState, _ stubQuery) (string, error) {
				return st.Last, nil
			}),
		).
		Build()

	scenario := NewActorTemporalScenario(actor, "stub-id", struct{}{})
	scenario.
		WhenActivity(func(ctx context.Context, a stubActivity) (string, error) {
			return "stubbed-" + a.Value, nil
		}).
		ExpectActivityOptionsForPayload(stubActivity{}, func(t testing.TB, opts actors.ActivityCallOptions) {
			require.Equal(t, 5*time.Second, opts.ScheduleToClose)
			require.Equal(t, 3*time.Second, opts.StartToClose)
			require.Equal(t, "custom-activity", opts.TaskQueue)
			require.Equal(t, 2, opts.Retry.MaxAttempts)
		}).
		WhenCommand(stubCommand{Value: "ok"}).
		Run(t)

	value, err := scenario.QueryWorkflow(stubQuery{})
	require.NoError(t, err)
	var last string
	require.NoError(t, value.Get(&last))
	require.Equal(t, "stubbed-ok", last)
}

func TestActorScenarioSupportsNamedCommandsAndQueries(t *testing.T) {
	type namedPayload struct {
		Value string
	}
	actor := actors.NewStateful("named-scenario", func() stubState { return stubState{} }).
		With(
			actors.CommandNamed("Record", func(ctx actors.Ctx, st *stubState, cmd namedPayload) (struct{}, error) {
				st.Last = cmd.Value
				return struct{}{}, nil
			}),
			actors.QueryNamed("Read", func(ctx actors.Ctx, st stubState, _ namedPayload) (string, error) {
				return st.Last, nil
			}),
		).
		Build()

	scenario := NewActorTemporalScenario(actor, "named-id", struct{}{})
	scenario.WhenCommandNamed("Record", namedPayload{Value: "named"}).Run(t)

	value, err := scenario.QueryWorkflowNamed("Read", namedPayload{})
	require.NoError(t, err)
	var last string
	require.NoError(t, value.Get(&last))
	require.Equal(t, "named", last)
}
