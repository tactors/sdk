package runtime

import (
	"testing"

	"github.com/tactors/sdk/actors"
)

func TestWorkflowContextAccessors(t *testing.T) {
	corr := actors.CorrelationData{SagaID: "saga", TraceID: "trace"}
	ctx := wfContext{
		ref:         actors.Ref{ID: "actor-123"},
		messageMeta: actors.MessageMetadata{CorrelationID: "cid-1", Correlation: corr},
		correlation: corr,
	}

	if got := ctx.ActorID(); got != "actor-123" {
		t.Fatalf("ActorID mismatch: %s", got)
	}
	meta := ctx.MessageMetadata()
	if meta.CorrelationID != "cid-1" || meta.Correlation.TraceID != "trace" {
		t.Fatalf("unexpected metadata %#v", meta)
	}

	retCorr := ctx.Correlation()
	if retCorr.TraceID != "trace" || retCorr.SagaID != "saga" {
		t.Fatalf("unexpected correlation %#v", retCorr)
	}
	// ensure SetCorrelation overwrites internal copy
	ctx.SetCorrelation(actors.CorrelationData{ParentID: "parent"})
	if ctx.correlation.ParentID != "parent" {
		t.Fatalf("SetCorrelation did not apply, got %+v", ctx.correlation)
	}
}
