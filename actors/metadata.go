package actors

import (
	"maps"
	"sort"
	"time"
)

// ActorMetadata captures routing, schema, and option details for an actor description.
type ActorMetadata struct {
	Kind           string
	VersionTag     string
	WorkflowQueue  string
	ActivityQueue  string
	DefaultTimeout time.Duration
	DefaultRetry   RetryPolicy
	SnapshotEvery  int
	Start          StartMetadata
	Commands       []CommandMetadata
	Queries        []QueryMetadata
	Activities     []ActivityMetadata
	Patches        []PatchMetadata
	CommandTypes   map[string]string
	QueryTypes     map[string]string
	ActivityTypes  map[string]string
}

// StartMetadata describes the init payload schema.
type StartMetadata struct {
	InputType string
}

// CommandMetadata captures per-command routing info.
type CommandMetadata struct {
	Name          string
	RequestType   string
	ResponseType  string
	Timeout       time.Duration
	SignalTimeout time.Duration
	Retry         RetryPolicy
	HasValidator  bool
}

// QueryMetadata captures per-query routing info.
type QueryMetadata struct {
	Name         string
	RequestType  string
	ResponseType string
	CacheTTL     time.Duration
}

// ActivityMetadata captures registered activity schemas.
type ActivityMetadata struct {
	Name            string
	RequestType     string
	ResponseType    string
	ScheduleToClose time.Duration
	ScheduleToStart time.Duration
	StartToClose    time.Duration
	Heartbeat       time.Duration
	TaskQueue       string
	Retry           RetryPolicy
}

// PatchMetadata captures declared patch toggles.
type PatchMetadata struct {
	ID        string
	DefaultOn bool
	Note      string
}

// Metadata generates a stable metadata summary for the description.
func (d *Description) Metadata() ActorMetadata {
	if d == nil {
		return ActorMetadata{}
	}
	meta := ActorMetadata{
		Kind:           d.Kind,
		VersionTag:     d.VersionTag,
		WorkflowQueue:  d.WorkflowQueue,
		ActivityQueue:  d.ActivityQueue,
		DefaultTimeout: d.Timeout,
		DefaultRetry:   d.Retry,
		SnapshotEvery:  d.SnapshotEvery,
		CommandTypes:   nil,
		QueryTypes:     nil,
	}
	meta.Start = StartMetadata{InputType: d.Start.Input}
	if len(d.CommandTypes) > 0 {
		meta.CommandTypes = maps.Clone(d.CommandTypes)
	}
	if len(d.QueryTypes) > 0 {
		meta.QueryTypes = maps.Clone(d.QueryTypes)
	}
	if len(d.ActivityNames) > 0 {
		meta.ActivityTypes = maps.Clone(d.ActivityNames)
	}
	meta.Commands = collectCommandMetadata(d)
	meta.Queries = collectQueryMetadata(d)
	meta.Activities = collectActivityMetadata(d)
	meta.Patches = collectPatchMetadata(d)
	return meta
}

func collectCommandMetadata(desc *Description) []CommandMetadata {
	if len(desc.Commands) == 0 {
		return nil
	}
	names := make([]string, 0, len(desc.Commands))
	for name := range desc.Commands {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]CommandMetadata, 0, len(names))
	for _, name := range names {
		spec := desc.Commands[name]
		out = append(out, CommandMetadata{
			Name:          name,
			RequestType:   spec.Handler.Input,
			ResponseType:  spec.ResponseType,
			Timeout:       spec.Timeout,
			SignalTimeout: desc.SignalTimeouts[name],
			Retry:         spec.Retry,
			HasValidator:  spec.Validator != nil,
		})
	}
	return out
}

func collectQueryMetadata(desc *Description) []QueryMetadata {
	if len(desc.Queries) == 0 {
		return nil
	}
	names := make([]string, 0, len(desc.Queries))
	for name := range desc.Queries {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]QueryMetadata, 0, len(names))
	for _, name := range names {
		spec := desc.Queries[name]
		out = append(out, QueryMetadata{
			Name:         name,
			RequestType:  spec.Handler.Input,
			ResponseType: spec.ResponseType,
			CacheTTL:     spec.CacheTTL,
		})
	}
	return out
}

func collectActivityMetadata(desc *Description) []ActivityMetadata {
	if len(desc.Activities) == 0 {
		return nil
	}
	names := make([]string, 0, len(desc.Activities))
	for name := range desc.Activities {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ActivityMetadata, 0, len(names))
	for _, name := range names {
		spec := desc.Activities[name]
		out = append(out, ActivityMetadata{
			Name:            name,
			RequestType:     desc.ActivityTypes[name],
			ResponseType:    desc.ActivityResults[name],
			ScheduleToClose: spec.Options.ScheduleToClose,
			ScheduleToStart: spec.Options.ScheduleToStart,
			StartToClose:    spec.Options.StartToClose,
			Heartbeat:       spec.Options.Heartbeat,
			TaskQueue:       spec.Options.TaskQueue,
			Retry:           spec.Options.Retry,
		})
	}
	return out
}

func collectPatchMetadata(desc *Description) []PatchMetadata {
	if len(desc.Patches) == 0 {
		return nil
	}
	ids := make([]string, 0, len(desc.Patches))
	for id := range desc.Patches {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]PatchMetadata, 0, len(ids))
	for _, id := range ids {
		spec := desc.Patches[id]
		out = append(out, PatchMetadata{
			ID:        spec.ID,
			DefaultOn: spec.DefaultOn,
			Note:      spec.Note,
		})
	}
	return out
}
