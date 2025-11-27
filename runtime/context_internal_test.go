package runtime

import (
	"testing"
	"time"

	"github.com/tactors/sdk/actors"
)

func TestMergeActivityOptions(t *testing.T) {
	base := actors.ActivityCallOptions{
		StartToClose: time.Minute,
		Heartbeat:    10 * time.Second,
		TaskQueue:    "base-queue",
		Retry:        actors.RetryPolicy{MaxAttempts: 1, InitialInterval: time.Second},
	}
	override := actors.ActivityCallOptions{
		ScheduleToClose: 5 * time.Second,
		TaskQueue:       " custom ",
		Retry:           actors.RetryPolicy{MaxAttempts: 3},
	}

	merged := mergeActivityOptions(base, override)
	if merged.ScheduleToClose != 5*time.Second {
		t.Fatalf("schedule-to-close not overridden: %+v", merged)
	}
	if merged.StartToClose != time.Minute || merged.Heartbeat != 10*time.Second {
		t.Fatalf("base options lost: %+v", merged)
	}
	if merged.TaskQueue != "custom" {
		t.Fatalf("task queue not trimmed/merged: %q", merged.TaskQueue)
	}
	if merged.Retry.MaxAttempts != 3 {
		t.Fatalf("retry not overridden: %+v", merged.Retry)
	}
}

func TestMergeActivityOptionsAppliesDefaultTimeouts(t *testing.T) {
	base := actors.ActivityCallOptions{}
	override := actors.ActivityCallOptions{}

	merged := mergeActivityOptions(base, override)
	if merged.StartToClose != defaultActivityStartToClose {
		t.Fatalf("expected default start-to-close timeout, got %s", merged.StartToClose)
	}
	if merged.ScheduleToClose != defaultActivityScheduleToClose {
		t.Fatalf("expected default schedule-to-close timeout, got %s", merged.ScheduleToClose)
	}
	if merged.Retry.MaxAttempts != 1 {
		t.Fatalf("expected default retry policy to disable retries, got %+v", merged.Retry)
	}
}
