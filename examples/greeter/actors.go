package greeter

import "github.com/tactors/sdk/actors"

type GreeterState struct {
	Count int
}

type HelloCommand struct {
	actors.CommandMsg[HelloResponse]
	Name string
}

type HelloResponse struct {
	Count int
}

type StatusQuery struct {
	actors.QueryMsg[StatusResponse]
}

type StatusResponse struct {
	Count int
}

func GreeterActor() actors.Actor {
	return actors.NewStateful("greeter", func() GreeterState { return GreeterState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *GreeterState, cmd HelloCommand) (HelloResponse, error) {
				st.Count++
				return HelloResponse{Count: st.Count}, nil
			}),
			actors.StopCommandAction[GreeterState](),
			actors.Query(func(ctx actors.Ctx, st GreeterState, _ StatusQuery) (StatusResponse, error) {
				return StatusResponse{Count: st.Count}, nil
			}),
		).
		Build()
}
