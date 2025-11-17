package ticketing

import (
	"fmt"
	"time"

	"github.com/tactors/sdk/actors"
)

type TicketState struct {
	ID        string
	Customer  string
	Issues    []string
	Assigned  string
	Escalated bool
	Closed    bool
	ClosedAt  time.Time
}

type OpenTicketCommand struct {
	actors.CommandMsg[OpenTicketResponse]
	ID       string
	Customer string
}

type OpenTicketResponse struct {
	ID string
}

type AddIssueCommand struct {
	actors.CommandMsg[AddIssueResponse]
	Issue string
}

type AddIssueResponse struct {
	Count int
}

type AssignAgentCommand struct {
	actors.CommandMsg[AssignAgentResponse]
	Agent string
}

type AssignAgentResponse struct{}

type CloseTicketCommand struct {
	actors.CommandMsg[CloseTicketResponse]
}

type CloseTicketResponse struct {
	ClosedAt time.Time
}

type TicketStatusQuery struct {
	actors.QueryMsg[TicketStatusResponse]
}

type TicketStatusResponse struct {
	ID        string
	Customer  string
	Issues    []string
	Assigned  string
	Escalated bool
	Closed    bool
	ClosedAt  time.Time
}

func TicketActor() actors.Actor {
	return actors.NewStateful("ticket", func() TicketState { return TicketState{} }).
		With(
			actors.Command(func(ctx actors.Ctx, st *TicketState, cmd OpenTicketCommand) (OpenTicketResponse, error) {
				st.ID = cmd.ID
				st.Customer = cmd.Customer
				st.Issues = nil
				st.Assigned = ""
				st.Escalated = false
				st.Closed = false
				return OpenTicketResponse{ID: cmd.ID}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *TicketState, cmd AddIssueCommand) (AddIssueResponse, error) {
				if st.Closed {
					return AddIssueResponse{}, fmt.Errorf("ticket already closed")
				}
				st.Issues = append(st.Issues, cmd.Issue)
				if len(st.Issues) > 3 {
					st.Escalated = true
				}
				return AddIssueResponse{Count: len(st.Issues)}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *TicketState, cmd AssignAgentCommand) (AssignAgentResponse, error) {
				if st.Closed {
					return AssignAgentResponse{}, fmt.Errorf("ticket already closed")
				}
				st.Assigned = cmd.Agent
				return AssignAgentResponse{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *TicketState, _ CloseTicketCommand) (CloseTicketResponse, error) {
				if st.Closed {
					return CloseTicketResponse{ClosedAt: st.ClosedAt}, nil
				}
				st.Closed = true
				st.ClosedAt = ctx.Now()
				return CloseTicketResponse{ClosedAt: st.ClosedAt}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st TicketState, _ TicketStatusQuery) (TicketStatusResponse, error) {
				return TicketStatusResponse{
					ID:        st.ID,
					Customer:  st.Customer,
					Issues:    append([]string(nil), st.Issues...),
					Assigned:  st.Assigned,
					Escalated: st.Escalated,
					Closed:    st.Closed,
					ClosedAt:  st.ClosedAt,
				}, nil
			}),
			actors.StopCommandAction[TicketState](),
		).
		Build()
}
