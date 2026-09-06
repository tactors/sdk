package control

import (
	"testing"
	"time"

	"github.com/tactors/sdk/actors"
)

func TestAwaitIntervalSchedulesNextRun(t *testing.T) {
	ctx := &scheduleCtx{
		now:   time.Unix(0, 0),
		attrs: map[string]any{},
	}
	cfg := ScheduleConfig{Every: time.Minute}
	if err := AwaitInterval(ctx, "cleanup", cfg); err != nil {
		t.Fatalf("await: %v", err)
	}
	if ctx.slept != 0 {
		t.Fatalf("unexpected sleep for first run: %v", ctx.slept)
	}
	nextRaw, ok := ctx.attrs["__actors_control_schedule.cleanup"]
	if !ok {
		t.Fatalf("next run not stored")
	}
	next, err := time.Parse(time.RFC3339Nano, nextRaw.(string))
	if err != nil {
		t.Fatalf("parse stored time: %v", err)
	}
	if !next.Equal(time.Unix(0, 0).Add(time.Minute)) {
		t.Fatalf("unexpected next run: %v", next)
	}
}

func TestAwaitIntervalSleepsUntilReady(t *testing.T) {
	start := time.Unix(0, 0)
	ctx := &scheduleCtx{
		now: start,
		attrs: map[string]any{
			"__actors_control_schedule.cleanup": start.Add(30 * time.Second).Format(time.RFC3339Nano),
		},
	}
	cfg := ScheduleConfig{Every: time.Minute}
	if err := AwaitInterval(ctx, "cleanup", cfg); err != nil {
		t.Fatalf("await: %v", err)
	}
	if ctx.slept != 30*time.Second {
		t.Fatalf("expected sleep 30s, got %v", ctx.slept)
	}
	nextRaw := ctx.attrs["__actors_control_schedule.cleanup"].(string)
	next, _ := time.Parse(time.RFC3339Nano, nextRaw)
	if !next.Equal(start.Add(30 * time.Second).Add(time.Minute)) {
		t.Fatalf("unexpected next run after sleep: %v", next)
	}
}

type scheduleCtx struct {
	now         time.Time
	slept       time.Duration
	attrs       map[string]any
	correlation actors.CorrelationData
}

func (s *scheduleCtx) ActorID() string                            { return "" }
func (s *scheduleCtx) Now() time.Time                             { return s.now }
func (s *scheduleCtx) Sleep(d time.Duration) error                { s.slept += d; s.now = s.now.Add(d); return nil }
func (s *scheduleCtx) Version(string, int, int) int               { return 0 }
func (s *scheduleCtx) Activity(string, any) actors.ActivityFuture { return nil }
func (s *scheduleCtx) ActivityWithOptions(string, any, actors.ActivityCallOptions) actors.ActivityFuture {
	return nil
}
func (s *scheduleCtx) BackgroundActivity(string, any) {}
func (s *scheduleCtx) Logger() actors.Logger          { return nil }
func (s *scheduleCtx) Self() actors.Ref               { return actors.Ref{} }
func (s *scheduleCtx) UpsertSearchAttributes(attrs map[string]any) error {
	for k, v := range attrs {
		s.attrs[k] = v
	}
	return nil
}
func (s *scheduleCtx) SearchAttributes() map[string]any { return s.attrs }
func (s *scheduleCtx) MessageMetadata() actors.MessageMetadata {
	return actors.MessageMetadata{}
}
func (s *scheduleCtx) Effect(string, actors.EffectFunc, ...actors.EffectOption) (any, error) {
	return nil, nil
}
func (s *scheduleCtx) Correlation() actors.CorrelationData { return s.correlation }
func (s *scheduleCtx) SetCorrelation(data actors.CorrelationData) {
	s.correlation = data
}
func (s *scheduleCtx) SnapshotInfo() actors.SnapshotInfo { return actors.SnapshotInfo{} }
func (s *scheduleCtx) WaitForEvent(string, time.Duration) (any, error) {
	return nil, actors.ErrUnsupported
}
