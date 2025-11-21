package telegram

import (
	"context"
	"fmt"
	"time"

	"github.com/tactors/sdk/actors"
)

type TicketState struct {
	Threads map[string]string
	Agents  []string
	SLA     time.Duration
}

type Init struct {
	Threads map[string]string
}

type CommandAssign struct {
	actors.CommandMsg[ResponseAssign]
	ThreadID string
	Agent    string
}

type ResponseAssign struct {
	OK bool
}

type CommandAnswer struct {
	actors.CommandMsg[ResponseAnswer]
	ThreadID string
	Body     string
}

type ResponseAnswer struct {
	Delivered bool
}

type QueryTranscript struct {
	actors.QueryMsg[ResponseTranscript]
	ThreadID string
}

type ResponseTranscript struct {
	Transcript string
}

type SendTelegramActivity struct {
	actors.ActivityMsg[struct{}]
	ThreadID string
	Body     string
}

func TelegramSupportActor() actors.Actor {
	return actors.NewStateful("telegram_support", func() TicketState {
		return TicketState{Threads: map[string]string{}}
	}).
		WithTimeout(4*24*time.Hour).
		WithSignalTimeout("assign", 3*time.Second).
		With(
			actors.ActivityNoResultNamed("sendTelegram", sendTelegramMessage),
			actors.Start(func(ctx actors.Ctx, init Init) (TicketState, error) {
				state := TicketState{Threads: init.Threads, SLA: 2 * time.Hour}
				return state, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *TicketState, msg CommandAssign) (ResponseAssign, error) {
				if st.Threads == nil {
					st.Threads = make(map[string]string)
				}
				st.Threads[msg.ThreadID] = msg.Agent
				return ResponseAssign{OK: true}, nil
			}, actors.WithTimeout(3*time.Second)),
			actors.Command(func(ctx actors.Ctx, st *TicketState, msg CommandAnswer) (ResponseAnswer, error) {
				err := actors.RunActivityNoResultNamed(ctx, "sendTelegram", SendTelegramActivity{
					ThreadID: msg.ThreadID,
					Body:     msg.Body,
				}, actors.WithActivityStartToClose(5*time.Second))
				return ResponseAnswer{Delivered: err == nil}, err
			}, actors.WithRetry(actors.RetryPolicy{MaxAttempts: 3})),
			actors.Query(func(ctx actors.Ctx, st TicketState, req QueryTranscript) (ResponseTranscript, error) {
				agent := st.Threads[req.ThreadID]
				return ResponseTranscript{Transcript: fmt.Sprintf("%s answered by %s", req.ThreadID, agent)}, nil
			}, actors.WithCache(1*time.Minute)),
			actors.StopCommandAction[TicketState](),
		).
		Build()
}

func sendTelegramMessage(ctx context.Context, msg SendTelegramActivity) error {
	fmt.Printf("sending response to %s: %s\n", msg.ThreadID, msg.Body)
	return nil
}
