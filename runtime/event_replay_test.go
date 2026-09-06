package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type replayWaitCommand struct {
	actors.CommandMsg[struct{}]
	Event   string
	Timeout time.Duration
}

func newReplayEventActor() actors.Actor {
	return actors.NewStateful("event-replay", func() struct{} { return struct{}{} }).
		With(
			actors.Command(func(ctx actors.Ctx, _ *struct{}, cmd replayWaitCommand) (struct{}, error) {
				if _, err := ctx.WaitForEvent(cmd.Event, cmd.Timeout); err != nil {
					return struct{}{}, err
				}
				return struct{}{}, actors.ErrStopLoop
			}),
		).
		Build()
}

// buildWaitForEventHistory hand-builds the history a real server would record
// for: start -> "wait" command signal -> timer started -> event signal ->
// timer cancelled -> workflow completed. Replaying it through the SDK's
// replayer proves WaitForEvent issues exactly the same commands on replay
// (StartTimer, then CancelTimer + CompleteWorkflowExecution) in the same
// order; any wall-clock or goroutine-driven divergence would surface as a
// non-determinism error.
func buildWaitForEventHistory(t *testing.T, kind, workflowID string, timeout time.Duration) *historypb.History {
	t.Helper()
	dc := dataConverter()
	startInput, err := dc.ToPayloads(workflowID, struct{}{})
	require.NoError(t, err)
	commandInput, err := dc.ToPayloads(replayWaitCommand{Event: "approve", Timeout: timeout})
	require.NoError(t, err)
	eventInput, err := dc.ToPayloads(approvalEvent{Approver: "alice", OK: true})
	require.NoError(t, err)
	signal, err := actors.EventSignalName("approve")
	require.NoError(t, err)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var events []*historypb.HistoryEvent
	add := func(evt *historypb.HistoryEvent) *historypb.HistoryEvent {
		evt.EventId = int64(len(events) + 1)
		evt.EventTime = timestamppb.New(base.Add(time.Duration(len(events)) * time.Second))
		events = append(events, evt)
		return evt
	}
	workflowTask := func() int64 {
		scheduled := add(&historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
				WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue: &taskqueuepb.TaskQueue{Name: kind},
				},
			},
		})
		started := add(&historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
				WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: scheduled.EventId,
				},
			},
		})
		completed := add(&historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: scheduled.EventId,
					StartedEventId:   started.EventId,
				},
			},
		})
		return completed.EventId
	}
	signalEvent := func(name string, input *commonpb.Payloads) {
		add(&historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
				WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: name,
					Input:      input,
				},
			},
		})
	}

	// 1: started
	add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:           &commonpb.WorkflowType{Name: kind},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: kind},
				Input:                  startInput,
				Attempt:                1,
				FirstExecutionRunId:    "run-1",
				OriginalExecutionRunId: "run-1",
				WorkflowTaskTimeout:    durationpb.New(10 * time.Second),
			},
		},
	})
	// 2-4: first task, no commands (loop parks in the command selector)
	workflowTask()
	// 5: the command that calls WaitForEvent
	signalEvent(actors.TypeKeyOf(replayWaitCommand{}), commandInput)
	// 6-8: task that starts the wait timer
	taskCompleted := workflowTask()
	// 9: TimerStarted; the SDK derives the timer id from the predicted event id.
	timer := add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
				TimerId:                      "9",
				StartToFireTimeout:           durationpb.New(timeout),
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	// 10: the event arrives on the namespaced signal
	signalEvent(signal, eventInput)
	// 11-13: task that receives the event, cancels the timer and completes
	taskCompleted = workflowTask()
	// 14: TimerCanceled
	add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_CANCELED,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
				TimerId:                      "9",
				StartedEventId:               timer.EventId,
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	// 15: completed
	add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	return &historypb.History{Events: events}
}

func newEventReplayer(t *testing.T, runner *Runner) worker.WorkflowReplayer {
	t.Helper()
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		DataConverter: dataConverter(),
	})
	require.NoError(t, err)
	replayer.RegisterWorkflowWithOptions(runner.Workflow(), workflow.RegisterOptions{Name: runner.Description().Kind})
	return replayer
}

// Replay determinism: the recorded history must replay cleanly against the
// current WaitForEvent implementation.
func TestWaitForEventReplaysDeterministically(t *testing.T) {
	runner := NewRunner(newReplayEventActor())
	history := buildWaitForEventHistory(t, runner.Description().Kind, "event-replay-1", time.Hour)
	replayer := newEventReplayer(t, runner)
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, history))
}

// Sanity check that the replay assertion has teeth: a history in which the
// wait never scheduled a timer (as if WaitForEvent had been called with no
// timeout) must not replay against code that does schedule one.
func TestWaitForEventReplayDetectsDivergence(t *testing.T) {
	runner := NewRunner(newReplayEventActor())
	history := buildWaitForEventHistory(t, runner.Description().Kind, "event-replay-2", time.Hour)
	var kept []*historypb.HistoryEvent
	for _, evt := range history.Events {
		if evt.GetTimerStartedEventAttributes() != nil || evt.GetTimerCanceledEventAttributes() != nil {
			continue
		}
		evt.EventId = int64(len(kept) + 1)
		kept = append(kept, evt)
	}
	history.Events = kept
	replayer := newEventReplayer(t, runner)
	err := replayer.ReplayWorkflowHistory(nil, history)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nondeterministic")
}

// The twin above proves a missing StartTimer is caught. It does not prove the
// timer is cancelled when the event wins: the replayer tolerates a trailing
// CancelTimer that the history lacks, so a leaked timer passed it. Dropping
// only TimerCanceled from the history makes the correct code's cancel an
// "extra replay command" -- which is exactly the error a leaked timer must
// NOT produce, so this fails when the cancel is missing.
func TestWaitForEventReplayDetectsLeakedTimer(t *testing.T) {
	runner := NewRunner(newReplayEventActor())
	history := buildWaitForEventHistory(t, runner.Description().Kind, "event-replay-3", time.Hour)
	var kept []*historypb.HistoryEvent
	for _, evt := range history.Events {
		if evt.GetTimerCanceledEventAttributes() != nil {
			continue
		}
		evt.EventId = int64(len(kept) + 1)
		kept = append(kept, evt)
	}
	history.Events = kept
	replayer := newEventReplayer(t, runner)
	err := replayer.ReplayWorkflowHistory(nil, history)
	require.Error(t, err)
	require.Contains(t, err.Error(), "extra replay command for CancelTimer")
}
