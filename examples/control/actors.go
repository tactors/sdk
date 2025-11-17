package controlactor

import (
	"time"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/control"
)

// State captures basic telemetry for the control actor.
type State struct {
	RunCount int
	LastRun  time.Time
}

// TickCommand triggers one scheduling pass.
type TickCommand struct {
	actors.CommandMsg[TickResult]
	Schedule string
	Every    time.Duration
}

// TickResult echoes the latest tick metadata back to the caller.
type TickResult struct {
	RunCount int
	NextRun  time.Time
}

// StatusQuery returns the current state snapshot.
type StatusQuery struct {
	actors.QueryMsg[State]
}

// IntervalActor returns a control workflow that executes TickCommand deterministically.
func IntervalActor() actors.Actor {
	return actors.NewStateful("control_interval", func() State { return State{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *State, cmd TickCommand) (TickResult, error) {
				if err := control.AwaitInterval(ctx, cmd.Schedule, control.ScheduleConfig{Every: cmd.Every}); err != nil {
					return TickResult{}, err
				}
				st.RunCount++
				st.LastRun = ctx.Now()
				return TickResult{
					RunCount: st.RunCount,
					NextRun:  st.LastRun.Add(cmd.Every),
				}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st State, _ StatusQuery) (State, error) {
				return st, nil
			}),
			actors.StopCommandAction[State](),
		).
		WithSnapshot(actors.SnapshotConfig[State]{
			Every: 25,
			ContinueArgs: func(st State) (any, error) {
				return st, nil
			},
		}).
		Build()
}
