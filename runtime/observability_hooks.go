package runtime

import (
	"context"
	"time"

	"github.com/tactors/sdk/observability"
	"go.temporal.io/sdk/workflow"
)

func emitCommandStart(ctx workflow.Context, evt observability.CommandEvent) {
	if workflow.IsReplaying(ctx) {
		return
	}
	if listener := observability.ActiveListener(); listener != nil {
		listener.CommandStart(context.Background(), evt)
	}
}

func emitCommandFinish(ctx workflow.Context, evt observability.CommandEvent, err error, duration time.Duration) {
	if workflow.IsReplaying(ctx) {
		return
	}
	if listener := observability.ActiveListener(); listener != nil {
		listener.CommandFinish(context.Background(), evt, err, duration)
	}
}

func emitAskStart(ctx workflow.Context, evt observability.AskEvent) {
	if workflow.IsReplaying(ctx) {
		return
	}
	if listener := observability.ActiveListener(); listener != nil {
		listener.AskStart(context.Background(), evt)
	}
}

func emitAskFinish(ctx workflow.Context, evt observability.AskEvent, err error, duration time.Duration) {
	if workflow.IsReplaying(ctx) {
		return
	}
	if listener := observability.ActiveListener(); listener != nil {
		listener.AskFinish(context.Background(), evt, err, duration)
	}
}

func emitQueryStart(ctx workflow.Context, evt observability.QueryEvent) {
	if workflow.IsReplaying(ctx) {
		return
	}
	if listener := observability.ActiveListener(); listener != nil {
		listener.QueryStart(context.Background(), evt)
	}
}

func emitQueryFinish(ctx workflow.Context, evt observability.QueryEvent, err error, duration time.Duration) {
	if workflow.IsReplaying(ctx) {
		return
	}
	if listener := observability.ActiveListener(); listener != nil {
		listener.QueryFinish(context.Background(), evt, err, duration)
	}
}
