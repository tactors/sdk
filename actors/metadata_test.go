package actors_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/tactors/sdk/actors"
)

type metadataState struct {
	Count int
}

type incrementCommand struct {
	actors.CommandMsg[struct{}]
	Delta int
}

type sumQuery struct {
	actors.QueryMsg[int]
}

type logActivity struct {
	Message string
}

func TestDescriptionMetadata(t *testing.T) {
	commandName := fmt.Sprintf("%T", incrementCommand{})
	retry := actors.RetryPolicy{MaxAttempts: 9, InitialInterval: 2 * time.Second, BackoffCoefficient: 1.5}
	actor := actors.NewStateful("meta", func() metadataState { return metadataState{} }).
		WithVersionTag("v1.2.3").
		WithWorkflowQueue("meta-workflows").
		WithActivityQueue("meta-activities").
		WithTimeout(30*time.Second).
		WithRetry(retry).
		WithSignalTimeout(commandName, time.Second).
		DeclarePatch("change-1", true).
		WithSnapshot(actors.SnapshotConfig[metadataState]{Every: 10, ContinueArgs: func(state metadataState) (any, error) {
			return state, nil
		}}).
		With(
			actors.Start(func(ctx actors.Ctx, _ struct{}) (metadataState, error) {
				return metadataState{Count: 1}, nil
			}),
			actors.Command(func(ctx actors.Ctx, st *metadataState, cmd incrementCommand) (struct{}, error) {
				st.Count += cmd.Delta
				return struct{}{}, nil
			}, actors.WithValidator(func(cmd incrementCommand) error {
				if cmd.Delta == 0 {
					return fmt.Errorf("delta must be non-zero")
				}
				return nil
			})),
			actors.Query(func(ctx actors.Ctx, st metadataState, _ sumQuery) (int, error) {
				return st.Count, nil
			}, actors.WithCache(5*time.Second)),
			actors.ActivityNamed("log", func(context.Context, logActivity) (struct{}, error) {
				return struct{}{}, nil
			}, actors.WithActivityDefaults(
				actors.WithActivityStartToClose(10*time.Second),
				actors.WithActivityScheduleToClose(12*time.Second),
				actors.WithActivityHeartbeat(time.Second),
				actors.WithActivityTaskQueue("log-tq"),
				actors.WithActivityRetry(actors.RetryPolicy{MaxAttempts: 7}),
			)),
		).
		Build()
	meta := actor.Spec().Metadata()
	if meta.Kind != "meta" {
		t.Fatalf("expected kind meta, got %q", meta.Kind)
	}
	if meta.VersionTag != "v1.2.3" {
		t.Fatalf("unexpected version: %q", meta.VersionTag)
	}
	if meta.WorkflowQueue != "meta-workflows" || meta.ActivityQueue != "meta-activities" {
		t.Fatalf("queues not captured: %#v", meta)
	}
	if meta.DefaultTimeout != 30*time.Second {
		t.Fatalf("timeout mismatch: %v", meta.DefaultTimeout)
	}
	if meta.DefaultRetry != retry {
		t.Fatalf("retry mismatch: %#v", meta.DefaultRetry)
	}
	if meta.SnapshotEvery != 10 {
		t.Fatalf("snapshot cadence missing: %d", meta.SnapshotEvery)
	}
	if meta.Start.InputType != "struct {}" {
		t.Fatalf("unexpected start input: %q", meta.Start.InputType)
	}
	if len(meta.Commands) != 1 {
		t.Fatalf("expected one command, got %d", len(meta.Commands))
	}
	cmd := meta.Commands[0]
	if cmd.Name != commandName {
		t.Fatalf("command name mismatch: %q", cmd.Name)
	}
	if cmd.Timeout != 0 {
		t.Fatalf("command timeout should be zero override, got %v", cmd.Timeout)
	}
	if cmd.SignalTimeout != time.Second {
		t.Fatalf("signal timeout missing")
	}
	if !cmd.HasValidator {
		t.Fatalf("validator flag missing")
	}
	if len(meta.Queries) != 1 || meta.Queries[0].CacheTTL != 5*time.Second {
		t.Fatalf("query metadata mismatch: %#v", meta.Queries)
	}
	if len(meta.Activities) != 1 || meta.Activities[0].RequestType != "actors_test.logActivity" {
		t.Fatalf("activity metadata missing request type: %#v", meta.Activities)
	}
	if meta.Activities[0].StartToClose != 10*time.Second || meta.Activities[0].ScheduleToClose != 12*time.Second {
		t.Fatalf("activity default timeouts missing: %#v", meta.Activities[0])
	}
	if meta.Activities[0].Heartbeat != time.Second || meta.Activities[0].TaskQueue != "log-tq" {
		t.Fatalf("activity defaults missing: %#v", meta.Activities[0])
	}
	if meta.Activities[0].Retry.MaxAttempts != 7 {
		t.Fatalf("activity retry missing: %#v", meta.Activities[0].Retry)
	}
	if meta.CommandTypes["actors_test.incrementCommand"] == "" {
		t.Fatalf("command type mapping missing entries: %#v", meta.CommandTypes)
	}
	if meta.QueryTypes["actors_test.sumQuery"] == "" {
		t.Fatalf("query type mapping missing entries: %#v", meta.QueryTypes)
	}
	if meta.ActivityTypes["actors_test.logActivity"] != "log" {
		t.Fatalf("activity type mapping missing entries: %#v", meta.ActivityTypes)
	}
	if len(meta.Patches) != 1 || meta.Patches[0].ID != "change-1" || !meta.Patches[0].DefaultOn {
		t.Fatalf("patch metadata missing: %#v", meta.Patches)
	}
}

func TestDescriptionSerializationDeterministic(t *testing.T) {
	builder := actors.NewStateful("serialize", func() metadataState { return metadataState{} }).
		WithVersionTag("s1").
		WithTimeout(42*time.Second).
		DeclarePatch("alpha", false).
		With(
			actors.Command(func(ctx actors.Ctx, st *metadataState, cmd incrementCommand) (struct{}, error) {
				st.Count += cmd.Delta
				return struct{}{}, nil
			}),
			actors.Query(func(ctx actors.Ctx, st metadataState, q sumQuery) (int, error) {
				return st.Count, nil
			}),
		)
	spec := builder.Build().Spec()
	first, err := actors.MarshalDescription(spec)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	second, err := actors.MarshalDescription(spec.Clone())
	if err != nil {
		t.Fatalf("marshal clone failed: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("serialization not deterministic:\n%s\n%s", first, second)
	}
	manifest, err := actors.UnmarshalDescription(first)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if err := manifest.VerifyHash(); err != nil {
		t.Fatalf("hash mismatch: %v", err)
	}
	meta, err := manifest.Metadata()
	if err != nil {
		t.Fatalf("metadata decode failed: %v", err)
	}
	if !reflect.DeepEqual(meta, spec.Metadata()) {
		out1, _ := json.MarshalIndent(meta, "", "  ")
		out2, _ := json.MarshalIndent(spec.Metadata(), "", "  ")
		t.Fatalf("metadata mismatch after roundtrip:\n%s\n%s", out1, out2)
	}
}
