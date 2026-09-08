package runtime

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type replayAskCommand struct {
	actors.CommandMsg[struct{}]
	Target  string
	Timeout time.Duration
}

type replayAskEchoCommand struct {
	actors.CommandMsg[string]
}

// newReplayAskActor issues one AskWithTimeout at a *different* actor id, so
// the caller's own history contains only the outgoing request signal, the
// deadline timer and the incoming reply signal -- the exact command sequence
// the deadline is responsible for.
func newReplayAskActor() actors.Actor {
	return actors.NewStateful("ask-replay", func() struct{} { return struct{}{} }).
		With(
			actors.Command(func(ctx actors.Ctx, _ *struct{}, cmd replayAskCommand) (struct{}, error) {
				target := actors.Ref{Kind: ctx.Self().Kind, ID: cmd.Target}
				if _, err := actors.AskWithTimeout[replayAskEchoCommand, string](ctx, target, replayAskEchoCommand{}, cmd.Timeout); err != nil {
					return struct{}{}, err
				}
				return struct{}{}, actors.ErrStopLoop
			}),
			actors.Command(func(ctx actors.Ctx, _ *struct{}, _ replayAskEchoCommand) (string, error) {
				return "echo", nil
			}),
		).
		Build()
}

// buildAskWithTimeoutHistory hand-builds the history a real server would record
// for a bounded ask whose reply wins the race:
//
//	start
//	-> "ask" command signal
//	-> SignalExternalWorkflowExecution (the ask request) + its ack
//	-> TimerStarted (the deadline)
//	-> the reply signal arrives
//	-> TimerCanceled + WorkflowExecutionCompleted
//
// Replaying it proves AskActorWithTimeout issues exactly these commands, in
// this order, on replay: any wall-clock reading, goroutine-ordering dependency
// or map-iteration dependency in the deadline would surface as a
// non-determinism error.
func buildAskWithTimeoutHistory(t *testing.T, kind, workflowID, target string, timeout time.Duration) *historypb.History {
	t.Helper()
	dc := dataConverter()
	startInput, err := dc.ToPayloads(workflowID, struct{}{})
	require.NoError(t, err)
	commandInput, err := dc.ToPayloads(replayAskCommand{Target: target, Timeout: timeout})
	require.NoError(t, err)
	// The waiter id is deterministic: "<actorID>-ask-<n>", n starting at 1.
	replyInput, err := dc.ToPayloads(askReply{ID: workflowID + "-ask-1", Payload: "echo"})
	require.NoError(t, err)

	h := newReplayHistory(kind)

	// 1: started
	h.started(startInput)
	// 2-4: first task, no commands (the loop parks in the command selector)
	h.workflowTask()
	// 5: the command that calls AskWithTimeout
	h.signalEvent(actors.TypeKeyOf(replayAskCommand{}), commandInput)
	// 6-8: the task that sends the ask request; the handler then blocks on the
	// signal's own future, so this task emits nothing else.
	taskCompleted := h.workflowTask()
	// 9: SignalExternalWorkflowExecutionInitiated
	initiated := h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
			SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: taskCompleted,
				WorkflowExecution:            &commonpb.WorkflowExecution{WorkflowId: target},
				SignalName:                   askRequestSignal,
			},
		},
	})
	// Control is how the replayer matches this event back to the command. The
	// SDK derives it, like a timer id, from the predicted event id -- which is
	// this event's own id.
	initiated.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().Control = strconv.FormatInt(initiated.EventId, 10)
	// 10: the server confirms delivery, unblocking the handler
	h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
			ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
				InitiatedEventId:  initiated.EventId,
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: target},
			},
		},
	})
	// 11-13: the task where the deadline timer is started
	taskCompleted = h.workflowTask()
	// 14: TimerStarted; the SDK derives the timer id from the predicted event id.
	timer := h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
				TimerId:                      "14",
				StartToFireTimeout:           durationpb.New(timeout),
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	// 15: the reply beats the deadline
	h.signalEvent(askReplySignal, replyInput)
	// 16-18: the task that receives the reply, cancels the timer and completes
	taskCompleted = h.workflowTask()
	// 19: TimerCanceled
	h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_CANCELED,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
				TimerId:                      "14",
				StartedEventId:               timer.EventId,
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	// 20: completed
	h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	return h.history()
}

// dropEvents rebuilds a history without the events keep rejects, renumbering
// as it goes, so the negative twins below can remove exactly one command's
// worth of history.
func dropEvents(history *historypb.History, drop func(*historypb.HistoryEvent) bool) *historypb.History {
	var kept []*historypb.HistoryEvent
	for _, evt := range history.Events {
		if drop(evt) {
			continue
		}
		evt.EventId = int64(len(kept) + 1)
		kept = append(kept, evt)
	}
	history.Events = kept
	return history
}

// Replay determinism: the recorded history must replay cleanly against the
// current AskActorWithTimeout implementation.
func TestAskWithTimeoutReplaysDeterministically(t *testing.T) {
	runner := NewRunner(newReplayAskActor())
	history := buildAskWithTimeoutHistory(t, runner.Description().Kind, "ask-replay-1", "ask-replay-target", time.Hour)
	replayer := newEventReplayer(t, runner)
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, history))
}

// Sanity check that the replay assertion has teeth: a history in which the ask
// never scheduled a timer (as if the deadline had been ignored, which is what
// the roadmap gap describes) must not replay against code that schedules one.
func TestAskWithTimeoutReplayDetectsMissingTimer(t *testing.T) {
	runner := NewRunner(newReplayAskActor())
	history := buildAskWithTimeoutHistory(t, runner.Description().Kind, "ask-replay-2", "ask-replay-target", time.Hour)
	history = dropEvents(history, func(evt *historypb.HistoryEvent) bool {
		return evt.GetTimerStartedEventAttributes() != nil || evt.GetTimerCanceledEventAttributes() != nil
	})
	replayer := newEventReplayer(t, runner)
	err := replayer.ReplayWorkflowHistory(nil, history)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nondeterministic")
}

// The twin above proves a missing StartTimer is caught. It does not prove the
// deadline timer is cancelled when the reply wins: the replayer tolerates a
// trailing CancelTimer the history lacks, so a leaked timer passed it.
// Dropping only TimerCanceled makes the correct code's cancel an "extra replay
// command" -- exactly the error a leaked timer must NOT produce, so this fails
// when the cancel is missing.
func TestAskWithTimeoutReplayDetectsLeakedTimer(t *testing.T) {
	runner := NewRunner(newReplayAskActor())
	history := buildAskWithTimeoutHistory(t, runner.Description().Kind, "ask-replay-3", "ask-replay-target", time.Hour)
	history = dropEvents(history, func(evt *historypb.HistoryEvent) bool {
		return evt.GetTimerCanceledEventAttributes() != nil
	})
	replayer := newEventReplayer(t, runner)
	err := replayer.ReplayWorkflowHistory(nil, history)
	require.Error(t, err)
	require.Contains(t, err.Error(), "extra replay command for CancelTimer")
}
