package control

import (
	"fmt"
	"strings"
	"time"

	"github.com/tactors/sdk/actors"
)

const scheduleAttrPrefix = "__actors_control_schedule."

// ScheduleConfig describes a durable interval gate for control workflows.
type ScheduleConfig struct {
	Every time.Duration
}

// AwaitInterval sleeps until the named schedule is due and persists the next run timestamp using
// search attributes so the cadence survives Continue-As-New.
func AwaitInterval(ctx actors.Ctx, name string, cfg ScheduleConfig) error {
	if ctx == nil {
		return actors.ErrUnsupported
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("control: schedule name must be non-empty")
	}
	if cfg.Every <= 0 {
		return fmt.Errorf("control: interval must be > 0")
	}
	now := ctx.Now()
	next := nextRun(ctx.SearchAttributes(), name)
	if next.After(now) {
		if err := ctx.Sleep(next.Sub(now)); err != nil {
			return err
		}
		now = ctx.Now()
	}
	next = now.Add(cfg.Every)
	return ctx.UpsertSearchAttributes(map[string]any{
		attrKey(name): next.Format(time.RFC3339Nano),
	})
}

func nextRun(attrs map[string]any, name string) time.Time {
	if len(attrs) == 0 {
		return time.Time{}
	}
	raw, ok := attrs[attrKey(name)]
	if !ok {
		return time.Time{}
	}
	switch val := raw.(type) {
	case time.Time:
		return val
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func attrKey(name string) string {
	return scheduleAttrPrefix + name
}
