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

// replayHistory accumulates hand-built history events with correct ids and
// monotonic timestamps, so several fixtures can share the boilerplate.
type replayHistory struct {
	kind   string
	base   time.Time
	events []*historypb.HistoryEvent
}

func newReplayHistory(kind string) *replayHistory {
	return &replayHistory{kind: kind, base: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (h *replayHistory) add(evt *historypb.HistoryEvent) *historypb.HistoryEvent {
	evt.EventId = int64(len(h.events) + 1)
	evt.EventTime = timestamppb.New(h.base.Add(time.Duration(len(h.events)) * time.Second))
	h.events = append(h.events, evt)
	return evt
}

// workflowTask appends scheduled/started/completed and returns the completed
// event id, which every command-generated event has to point back at.
func (h *replayHistory) workflowTask() int64 {
	scheduled := h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue: &taskqueuepb.TaskQueue{Name: h.kind},
			},
		},
	})
	started := h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: scheduled.EventId,
			},
		},
	})
	completed := h.add(&historypb.HistoryEvent{
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

func (h *replayHistory) signalEvent(name string, input *commonpb.Payloads) {
	h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
			WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: name,
				Input:      input,
			},
		},
	})
}

func (h *replayHistory) started(input *commonpb.Payloads) {
	h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:           &commonpb.WorkflowType{Name: h.kind},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: h.kind},
				Input:                  input,
				Attempt:                1,
				FirstExecutionRunId:    "run-1",
				OriginalExecutionRunId: "run-1",
				WorkflowTaskTimeout:    durationpb.New(10 * time.Second),
			},
		},
	})
}

func (h *replayHistory) history() *historypb.History {
	return &historypb.History{Events: h.events}
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

	h := newReplayHistory(kind)
	add := h.add
	workflowTask := h.workflowTask
	signalEvent := h.signalEvent

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
	return h.history()
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

type replayDeferWaitCommand struct {
	actors.CommandMsg[struct{}]
	Event   string
	Timeout time.Duration
}

type replayDeferNoteCommand struct {
	actors.CommandMsg[struct{}]
}

type replayDeferState struct {
	Notes int
}

// newReplayDeferActor rotates after every command (SnapshotEvery=1) and has a
// command that parks in WaitForEvent, which is the exact shape the deferral
// guard exists for.
func newReplayDeferActor() actors.Actor {
	return actors.NewStateful("event-defer", func() replayDeferState { return replayDeferState{} }).
		WithSnapshot(actors.SnapshotConfig[replayDeferState]{
			Every: 1,
			ContinueArgs: func(st replayDeferState) (any, error) {
				return struct{}{}, nil
			},
		}).
		With(
			actors.Command(func(ctx actors.Ctx, _ *replayDeferState, cmd replayDeferWaitCommand) (struct{}, error) {
				_, _ = ctx.WaitForEvent(cmd.Event, cmd.Timeout)
				return struct{}{}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *replayDeferState, _ replayDeferNoteCommand) (struct{}, error) {
				st.Notes++
				return struct{}{}, nil
			}),
		).
		Build()
}

// buildDeferredRotationHistory hand-builds the history of a rotation deferred
// for a parked handler:
//
//	start -> tell request parks a handler in WaitForEvent (StartTimer)
//	      -> "note" command signal completes and trips SnapshotEvery=1
//	      -> the task that follows emits NO commands: the rotation is deferred
//	      -> the event arrives, the handler returns, and only now does the
//	         rotation run (CancelTimer, ModifyWorkflowProperties, ContinueAsNew)
//
// `rotateEarly` builds the pre-fix history instead, where the rotation fires in
// the note-command task while the handler is still parked. That is the
// negative twin: the replayer must reject it, which is what proves this
// fixture can fail at all.
func buildDeferredRotationHistory(t *testing.T, kind, workflowID string, rotateEarly bool) *historypb.History {
	t.Helper()
	dc := dataConverter()
	startInput, err := dc.ToPayloads(workflowID, struct{}{})
	require.NoError(t, err)
	tellInput, err := dc.ToPayloads(tellRequest{
		Command: actors.TypeKeyOf(replayDeferWaitCommand{}),
		Payload: replayDeferWaitCommand{Event: "approve", Timeout: time.Hour},
	})
	require.NoError(t, err)
	noteInput, err := dc.ToPayloads(replayDeferNoteCommand{})
	require.NoError(t, err)
	eventInput, err := dc.ToPayloads(approvalEvent{Approver: "alice", OK: true})
	require.NoError(t, err)
	eventSignalName, err := actors.EventSignalName("approve")
	require.NoError(t, err)

	h := newReplayHistory(kind)
	rotationEvents := func(taskCompleted int64) {
		h.add(&historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED,
			Attributes: &historypb.HistoryEvent_WorkflowPropertiesModifiedEventAttributes{
				WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{
					WorkflowTaskCompletedEventId: taskCompleted,
					// The replayer merges this into workflow info, so it must
					// not be nil even though the contents are not compared.
					UpsertedMemo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{}},
				},
			},
		})
		h.add(&historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
				WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
					NewExecutionRunId:            "run-2",
					WorkflowType:                 &commonpb.WorkflowType{Name: kind},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: kind},
					WorkflowTaskCompletedEventId: taskCompleted,
				},
			},
		})
	}

	// 1: started
	h.started(startInput)
	// 2-4: first task, no commands (the loop parks in the command selector)
	h.workflowTask()
	// 5: the tell request that parks a handler off the command loop
	h.signalEvent(tellRequestSignal, tellInput)
	// 6-8: task that starts the wait timer
	taskCompleted := h.workflowTask()
	// 9: TimerStarted; the SDK derives the timer id from the predicted event id.
	timer := h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
				TimerId:                      "9",
				StartToFireTimeout:           durationpb.New(time.Hour),
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	// 10: the command whose completion trips SnapshotEvery=1
	h.signalEvent(actors.TypeKeyOf(replayDeferNoteCommand{}), noteInput)
	// 11-13: the task that runs it. The rotation belongs *after* this task.
	taskCompleted = h.workflowTask()
	if rotateEarly {
		rotationEvents(taskCompleted)
		return h.history()
	}
	// 14: the event that releases the parked handler
	h.signalEvent(eventSignalName, eventInput)
	// 15-17: the task where the handler returns and the rotation finally runs
	taskCompleted = h.workflowTask()
	// 18: TimerCanceled, emitted by the wait when the event wins
	h.add(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_TIMER_CANCELED,
		Attributes: &historypb.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
				TimerId:                      "9",
				StartedEventId:               timer.EventId,
				WorkflowTaskCompletedEventId: taskCompleted,
			},
		},
	})
	// 19-20: snapshot memo upsert + continue-as-new
	rotationEvents(taskCompleted)
	return h.history()
}

// Replay determinism for the deferral: a rotation that waited for a parked
// Tell handler must replay cleanly, i.e. the deferral decision is a pure
// function of workflow state and the workflow.Channel wake-up adds no history.
func TestDeferredRotationReplaysDeterministically(t *testing.T) {
	runner := NewRunner(newReplayDeferActor())
	history := buildDeferredRotationHistory(t, runner.Description().Kind, "defer-replay-1", false)
	replayer := newEventReplayer(t, runner)
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, history))
}

// The negative twin: the pre-fix history, where the rotation fired in the
// note-command task while the Tell handler was still parked. Replaying it
// against the fixed code must fail -- if it passed, the test above would prove
// nothing about *when* the rotation happens.
func TestDeferredRotationReplayRejectsEarlyRotation(t *testing.T) {
	runner := NewRunner(newReplayDeferActor())
	history := buildDeferredRotationHistory(t, runner.Description().Kind, "defer-replay-2", true)
	replayer := newEventReplayer(t, runner)
	err := replayer.ReplayWorkflowHistory(nil, history)
	require.Error(t, err, "the abandoning history must not replay against the deferring code")
	require.Contains(t, err.Error(), "nondeterministic workflow")
	require.Contains(t, err.Error(), "missing replay command for WorkflowPropertiesModified",
		"the fixed code must emit no snapshot/rotation commands while the handler is parked")
}
