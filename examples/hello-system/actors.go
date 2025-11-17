package hellosystem

import "github.com/tactors/sdk/actors"

type SystemState struct {
	Value string
}

type SetValueCommand struct {
	actors.CommandMsg[SetValueResponse]
	Value string
}

type SetValueResponse struct{}

type GetValueQuery struct {
	actors.QueryMsg[GetValueResponse]
}

type GetValueResponse struct {
	Value string
}

func SystemActor() actors.Actor {
	return actors.NewStateful("hello_system", func() SystemState { return SystemState{} }).
		With(
			actors.Command[SystemState, SetValueCommand, SetValueResponse](func(ctx actors.Ctx, st *SystemState, cmd SetValueCommand) (SetValueResponse, error) {
				st.Value = cmd.Value
				return SetValueResponse{}, nil
			}),
			actors.Query[SystemState, GetValueQuery, GetValueResponse](func(ctx actors.Ctx, st SystemState, _ GetValueQuery) (GetValueResponse, error) {
				return GetValueResponse{Value: st.Value}, nil
			}),
			actors.StopCommandAction[SystemState](),
		).
		Build()
}
