# Control Workflows & Durable Scheduling

Framework-owned actors (registry sync, saga cleanup, metrics exporters) often need to run on a
deterministic cadence without relying on external cron services. This page walks through the
`control` helpers that keep those loops deterministic and Temporal-friendly.

## Durable Interval Scheduling

- Use `control.AwaitInterval(ctx, name, control.ScheduleConfig{Every: d})` to block until the named
  schedule is due. The helper stores the next run timestamp in workflow search attributes so the
  cadence survives Continue-As-New and history replay.
- Each schedule name is scoped to the workflow. Choose descriptive names (e.g., `registry-sync`) so
  you can inspect the attributes in Temporal’s UI if needed.
- Because the helper uses `ctx.Sleep` under the hood, the workflow stays deterministic under fake
  time or Temporal’s test suite.

Example:

```go
func registryActor() actors.Actor {
	return actors.NewStateful("registry_sync", func() struct{} { return struct{}{} }).
		With(
			actors.Command(func(ctx actors.Ctx, _ *struct{}, _ struct{}) (struct{}, error) {
				if err := control.AwaitInterval(ctx, "registry-sync", control.ScheduleConfig{Every: time.Minute}); err != nil {
					return struct{}{}, err
				}
				// Refresh metadata, publish stats, etc.
				return struct{}{}, nil
			}),
		).
		WithSnapshot(actors.SnapshotConfig[struct{}]{Every: 100, ContinueArgs: func(struct{}) (any, error) { return struct{}{}, nil }}).
		Build()
}
```

## Best Practices

- **Record cadence in search attributes.** `AwaitInterval` already writes the next run timestamp to a
  deterministic key (`__actors_control_schedule.<name>`). You can query it via Temporal search or
  add it to custom telemetry.
- **Combine with snapshotting.** Long-running control actors should configure `WithSnapshot` so
  history stays compact. Snapshot stats (exposed via `actors.Snapshot(ctx)` or the diagnostics
  queries) make it easy to alert on stuck cadences.
- **Surface patch guards.** When rolling out new maintenance logic, declare patches via
  `builder.DeclarePatch("my-change", false)` and gate code using `actors.Patch(ctx, "my-change")`.
  The diagnostics queries make it easy to audit which workers have flipped the patch.
- **Study the sample.** [`examples/control`](https://github.com/tactors/samples/tree/main/examples/control)
  contains a runnable Temporal scenario that exercises `control.AwaitInterval`, snapshotting, and the
  standard stop-command pattern.
