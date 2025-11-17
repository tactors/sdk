package spawn

import (
	"strings"

	"github.com/tactors/sdk/actors"
)

type ParentState struct {
	Children     []string
	LastResult   string
	LastNotify   string
	LastSnapshot string
}

type SpawnChildCommand struct {
	actors.CommandMsg[SpawnChildResponse]
	ChildID string
	Value   string
}

type SpawnChildResponse struct {
	Child actors.Ref
}

type ParentStateQuery struct {
	actors.QueryMsg[ParentStateResponse]
}

type ParentStateResponse struct {
	Children     []string
	LastResult   string
	LastNotify   string
	LastSnapshot string
}

type ComputeViaChildCommand struct {
	actors.CommandMsg[ChildComputeResponse]
	ChildID string
	Value   string
}

type ChildComputeCommand struct {
	actors.CommandMsg[ChildComputeResponse]
	Value string
}

type ChildComputeResponse struct {
	Result string
}

type NotifyChildCommand struct {
	actors.CommandMsg[NotifyChildResponse]
	ChildID string
	Value   string
}

type NotifyChildResponse struct{}

type SnapshotChildCommand struct {
	actors.CommandMsg[SnapshotChildResponse]
	ChildID string
}

type SnapshotChildResponse struct {
	Snapshot ChildStateResponse
}

type ChildStateQuery struct {
	actors.QueryMsg[ChildStateResponse]
}

type ChildStateResponse struct {
	LastResult string
}

func ParentActor() actors.Actor {
	return actors.NewStateful("spawn_parent", func() ParentState { return ParentState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *ParentState, cmd SpawnChildCommand) (SpawnChildResponse, error) {
				ref, err := actors.Spawn(ctx, "spawn_child", ChildInit{Value: cmd.Value}, actors.WithChildName(cmd.ChildID))
				if err != nil {
					return SpawnChildResponse{}, err
				}
				st.Children = append(st.Children, ref.ID)
				return SpawnChildResponse{Child: ref}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *ParentState, cmd ComputeViaChildCommand) (ChildComputeResponse, error) {
				var (
					resp ChildComputeResponse
					err  error
				)
				if cmd.ChildID != "" {
					resp, err = actors.Ask[ChildComputeCommand, ChildComputeResponse](ctx, actors.Ref{Kind: "spawn_child", ID: cmd.ChildID}, ChildComputeCommand{Value: cmd.Value})
				} else {
					resp, err = actors.SpawnOneShot(ctx, ChildComputeCommand{Value: cmd.Value}, actors.WithChildKind("spawn_child"))
				}
				if err != nil {
					return ChildComputeResponse{}, err
				}
				st.LastResult = resp.Result
				return resp, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *ParentState, cmd NotifyChildCommand) (NotifyChildResponse, error) {
				ref := actors.Ref{Kind: "spawn_child", ID: cmd.ChildID}
				if err := actors.Tell(ctx, ref, ChildComputeCommand{Value: cmd.Value}); err != nil {
					return NotifyChildResponse{}, err
				}
				st.LastNotify = strings.ToUpper(cmd.Value)
				return NotifyChildResponse{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *ParentState, cmd SnapshotChildCommand) (SnapshotChildResponse, error) {
				ref := actors.Ref{Kind: "spawn_child", ID: cmd.ChildID}
				resp, err := actors.QueryActor(ctx, ref, ChildStateQuery{})
				if err != nil {
					return SnapshotChildResponse{}, err
				}
				st.LastSnapshot = resp.LastResult
				return SnapshotChildResponse{Snapshot: resp}, nil
			}),
			actors.StopCommandAction[ParentState](),
			actors.Query(func(ctx actors.Ctx, st ParentState, _ ParentStateQuery) (ParentStateResponse, error) {
				return ParentStateResponse{
					Children:     append([]string(nil), st.Children...),
					LastResult:   st.LastResult,
					LastNotify:   st.LastNotify,
					LastSnapshot: st.LastSnapshot,
				}, nil
			}),
		).
		Build()
}

type ChildInit struct {
	Value string
}

type ChildState struct {
	Value      string
	LastResult string
}

func ChildActor() actors.Actor {
	return actors.NewStateful("spawn_child", func() ChildState { return ChildState{} }).
		With(
			actors.Start(func(ctx actors.Ctx, init ChildInit) (ChildState, error) {
				return ChildState{Value: init.Value}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *ChildState, cmd ChildComputeCommand) (ChildComputeResponse, error) {
				result := strings.ToUpper(cmd.Value)
				st.LastResult = result
				return ChildComputeResponse{Result: result}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st ChildState, _ ChildStateQuery) (ChildStateResponse, error) {
				return ChildStateResponse{LastResult: st.LastResult}, nil
			}),
		).
		Build()
}
