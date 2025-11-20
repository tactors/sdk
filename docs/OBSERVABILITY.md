# Observability

The SDK instruments every command plus cross-actor `Ask`/`Query` exchange with spans and metrics.
You can wire these signals into the telemetry system of your choice by implementing the lightweight
hooks exposed under `observability/`.

## Runtime signals

- **Command execution.** Each command handler emits a span named `actor.command` along with
  histogram/counter metrics (`actor_command_duration`, `actor_command_success_total`,
  `actor_command_errors_total`).
- **Cross-actor calls.** `actors.Ask` and `actors.QueryActor` now produce spans
  (`actor.ask`, `actor.query`) and duration/success metrics for the caller/callee pair. This makes it
  easy to attribute slow or failing fan-out traffic across actor kinds.

## Wiring telemetry providers

The SDK ships with two interfaces:

- `observability.Tracer` for spans (`Start`, `Span.SetAttributes`, `Span.RecordError`, `Span.End`).
- `observability.Meter` for histograms/counters (`RecordHistogram`, `AddCounter`).

Set them globally when your process starts:

```go
observability.SetTracer(myTracer)
observability.SetMeter(myMeter)
```

The Temporal runtime and cross-actor helpers call these hooks automatically whenever a command,
query, or ask executes. If you never set the hooks, the SDK falls back to a no-op implementation.

## Example: OpenTelemetry bridge

The framework (or your service) can wrap OpenTelemetry without the SDK importing it directly. A
minimal bridge looks like:

```go
type otelTracer struct{ tracer trace.Tracer }

func (t otelTracer) Start(ctx context.Context, name string, attrs ...observability.Attribute) (context.Context, observability.Span) {
    ctx, span := t.tracer.Start(ctx, name, trace.WithAttributes(convert(attrs)...))
    return ctx, otelSpan{span: span}
}

type otelSpan struct{ span trace.Span }

func (s otelSpan) End(error)                  { s.span.End() }
func (s otelSpan) SetAttributes(attrs ...observability.Attribute) {
    s.span.SetAttributes(convert(attrs)...)
}
func (s otelSpan) RecordError(err error) {
    if err != nil {
        s.span.RecordError(err)
    }
}

type otelMeter struct{ meter metric.Meter }

func (m otelMeter) RecordHistogram(ctx context.Context, name string, value time.Duration, attrs ...observability.Attribute) {
    hist, _ := m.meter.Float64Histogram(name, metric.WithUnit("ms"))
    hist.Record(ctx, float64(value)/float64(time.Millisecond), metric.WithAttributes(convert(attrs)...))
}

func initTelemetry(tp trace.TracerProvider, mp metric.MeterProvider) {
    observability.SetTracer(otelTracer{tracer: tp.Tracer("github.com/tactors/sdk")})
    observability.SetMeter(otelMeter{meter: mp.Meter("github.com/tactors/sdk")})
}

func convert(attrs []observability.Attribute) []attribute.KeyValue {
    converted := make([]attribute.KeyValue, 0, len(attrs))
    for _, attr := range attrs {
        converted = append(converted, attribute.String(attr.Key, fmt.Sprint(attr.Value)))
    }
    return converted
}
```

Frameworks can swap in any tracer/meter (Datadog, Honeycomb, Prometheus, etc.) by implementing the
same interfaces. This keeps the SDK minimal while still emitting structured spans and metrics.

### Span and metric attributes (current default set)

- `actor.kind`, `actor.workflow_id`, `actor.run_id`, `actor.queue` for commands, asks, queries.
- `command.name`, `ask.target_kind`, `query.target_kind`, `message_id`, `correlation_id`.
- Outcome tags: success/failure, error type (business/non-retryable), duration histograms per verb.

Tip: reuse the same tracer/meter as your existing Temporal worker instrumentation so traces stitch
across SDK spans and platform spans.

## Lifecycle events

For richer glue (registry updates, saga tracing, custom metrics), implement `observability.Listener`
and register it via `observability.SetListener`. The runtime invokes these callbacks when it processes
real work (replays are skipped automatically):

- `CommandStart` / `CommandFinish` with `CommandEvent` metadata (`actor.kind`, workflow/run IDs, queue,
  message/correlation IDs, caller info, and attributes).
- `AskStart` / `AskFinish` with `AskEvent` metadata (caller/target refs, correlation IDs).
- `QueryStart` / `QueryFinish` with `QueryEvent` metadata (caller/target refs, correlation IDs).

Embed `observability.ListenerAdapter` to satisfy the interface and override only what you need:

```go
type loggingListener struct {
    observability.ListenerAdapter
    logger *slog.Logger
}

func (l loggingListener) CommandStart(_ context.Context, evt observability.CommandEvent) {
    l.logger.Info("command started",
        "actor.kind", evt.ActorKind,
        "workflow", evt.WorkflowID,
        "command", evt.Command,
        "message_id", evt.MessageID,
    )
}

func init() {
    observability.SetListener(loggingListener{logger: slog.Default()})
}
```

Listeners run inside the Temporal worker process and only when `workflow.IsReplaying` reports false,
so every hook fires exactly once per real execution even if the workflow later replays on another
worker. Use the metadata to correlate events across workers, registries, and telemetry backends.
