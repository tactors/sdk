package actors

import "context"

const (
	// DiagnosticsPatchesQuery is the reserved Temporal query name that returns actor patch metadata.
	DiagnosticsPatchesQuery = "actors_diag_patches"
	// DiagnosticsSnapshotQuery returns snapshot/Continue-As-New statistics for the workflow.
	DiagnosticsSnapshotQuery = "actors_diag_snapshot"
)

// PatchStatus captures the static metadata for a declared patch.
type PatchStatus struct {
	ID        string
	DefaultOn bool
	Note      string
}

// PatchReport enumerates every patch declared on an actor kind.
type PatchReport struct {
	Kind    string
	Patches []PatchStatus
}

// SnapshotReport returns snapshot rotation statistics for an actor workflow.
type SnapshotReport struct {
	Kind     string
	Snapshot SnapshotInfo
}

// QueryPatchReport asks the target actor for its patch metadata via the diagnostics query.
func QueryPatchReport(ctx context.Context, ref Ref) (PatchReport, error) {
	var report PatchReport
	err := clientInvokerFactory(ref).InvokeQuery(ctx, ref, DiagnosticsPatchesQuery, nil, &report)
	return report, err
}

// QuerySnapshotReport asks the target actor for its snapshot/rotation state via the diagnostics query.
func QuerySnapshotReport(ctx context.Context, ref Ref) (SnapshotReport, error) {
	var report SnapshotReport
	err := clientInvokerFactory(ref).InvokeQuery(ctx, ref, DiagnosticsSnapshotQuery, nil, &report)
	return report, err
}
