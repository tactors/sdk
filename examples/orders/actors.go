package orders

import (
	"context"
	"fmt"
	"time"

	"github.com/tactors/sdk/actors"
)

type OrderState struct {
	ID          string
	Customer    string
	Items       []string
	Submitted   bool
	SubmittedAt time.Time
}

type StartOrderCommand struct {
	actors.CommandMsg[StartOrderResponse]
	ID       string
	Customer string
}

type StartOrderResponse struct {
	ID string
}

type AddItemCommand struct {
	actors.CommandMsg[AddItemResponse]
	Item string
}

type AddItemResponse struct {
	Count int
}

type SubmitOrderCommand struct {
	actors.CommandMsg[SubmitOrderResponse]
}

type SubmitOrderResponse struct {
	SubmittedAt  time.Time
	Confirmation string
}

type OrderStatusQuery struct {
	actors.QueryMsg[OrderStatusResponse]
}

type OrderStatusResponse struct {
	ID          string
	Customer    string
	Items       []string
	Submitted   bool
	SubmittedAt time.Time
}

type SendConfirmationActivity struct {
	actors.ActivityMsg[string]
	OrderID string
}

func OrderActor() actors.Actor {
	return actors.NewStateful("order", func() OrderState { return OrderState{} }).
		With(
			actors.Activity(func(ctx context.Context, req SendConfirmationActivity) (string, error) {
				return fmt.Sprintf("order-%s-confirmed", req.OrderID), nil
			}),
			actors.Command(func(ctx actors.Ctx, st *OrderState, cmd StartOrderCommand) (StartOrderResponse, error) {
				st.ID = cmd.ID
				st.Customer = cmd.Customer
				st.Items = nil
				st.Submitted = false
				return StartOrderResponse{ID: cmd.ID}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *OrderState, cmd AddItemCommand) (AddItemResponse, error) {
				if st.Submitted {
					return AddItemResponse{}, fmt.Errorf("order already submitted")
				}
				st.Items = append(st.Items, cmd.Item)
				return AddItemResponse{Count: len(st.Items)}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *OrderState, _ SubmitOrderCommand) (SubmitOrderResponse, error) {
				if st.Submitted {
					return SubmitOrderResponse{SubmittedAt: st.SubmittedAt, Confirmation: "already-submitted"}, nil
				}
				st.Submitted = true
				st.SubmittedAt = ctx.Now()
				confirmation, err := actors.RunActivity(ctx, SendConfirmationActivity{OrderID: st.ID},
					actors.WithActivityStartToClose(5*time.Second))
				if err != nil {
					return SubmitOrderResponse{}, err
				}
				return SubmitOrderResponse{SubmittedAt: st.SubmittedAt, Confirmation: confirmation}, nil
			}, actors.WithRetry(actors.RetryPolicy{MaxAttempts: 3})),
			actors.Query(func(ctx actors.Ctx, st OrderState, _ OrderStatusQuery) (OrderStatusResponse, error) {
				return OrderStatusResponse{
					ID:          st.ID,
					Customer:    st.Customer,
					Items:       append([]string(nil), st.Items...),
					Submitted:   st.Submitted,
					SubmittedAt: st.SubmittedAt,
				}, nil
			}),
			actors.StopCommandAction[OrderState](),
		).
		Build()
}
