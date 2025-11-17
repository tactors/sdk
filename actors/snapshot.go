package actors

import "time"

// SnapshotInfo exposes snapshot/continue statistics for diagnostics.
type SnapshotInfo struct {
	Enabled               bool
	SnapshotEvery         int
	SnapshotsTaken        int
	ContinueAsNewCount    int
	CommandsSinceSnapshot int
	LastSnapshotTime      time.Time
}
