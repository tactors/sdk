package actors

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const descriptionManifestVersion = 1

// DescriptionManifest is the canonical serialized representation of an actor description.
type DescriptionManifest struct {
	Version int             `json:"version"`
	Hash    string          `json:"hash"`
	Actor   serializedActor `json:"actor"`
}

type serializedActor struct {
	Kind          string               `json:"kind"`
	VersionTag    string               `json:"versionTag,omitempty"`
	WorkflowQueue string               `json:"workflowQueue,omitempty"`
	ActivityQueue string               `json:"activityQueue,omitempty"`
	Timeout       string               `json:"timeout,omitempty"`
	Retry         *serializedRetry     `json:"retry,omitempty"`
	SnapshotEvery int                  `json:"snapshotEvery,omitempty"`
	Start         serializedStart      `json:"start,omitempty"`
	Commands      []serializedCommand  `json:"commands,omitempty"`
	Queries       []serializedQuery    `json:"queries,omitempty"`
	Activities    []serializedActivity `json:"activities,omitempty"`
	Patches       []serializedPatch    `json:"patches,omitempty"`
	CommandTypes  []serializedTypeMap  `json:"commandTypes,omitempty"`
	QueryTypes    []serializedTypeMap  `json:"queryTypes,omitempty"`
	ActivityTypes []serializedTypeMap  `json:"activityTypes,omitempty"`
}

type serializedRetry struct {
	MaxAttempts        int     `json:"maxAttempts,omitempty"`
	InitialInterval    string  `json:"initialInterval,omitempty"`
	BackoffCoefficient float64 `json:"backoffCoefficient,omitempty"`
}

type serializedStart struct {
	Input string `json:"input,omitempty"`
}

type serializedCommand struct {
	Name          string           `json:"name"`
	RequestType   string           `json:"requestType"`
	ResponseType  string           `json:"responseType"`
	Timeout       string           `json:"timeout,omitempty"`
	SignalTimeout string           `json:"signalTimeout,omitempty"`
	Retry         *serializedRetry `json:"retry,omitempty"`
	HasValidator  bool             `json:"hasValidator,omitempty"`
}

type serializedQuery struct {
	Name         string `json:"name"`
	RequestType  string `json:"requestType"`
	ResponseType string `json:"responseType"`
	CacheTTL     string `json:"cacheTtl,omitempty"`
}

type serializedActivity struct {
	Name            string           `json:"name"`
	RequestType     string           `json:"requestType"`
	ResponseType    string           `json:"responseType"`
	ScheduleToClose string           `json:"scheduleToClose,omitempty"`
	ScheduleToStart string           `json:"scheduleToStart,omitempty"`
	StartToClose    string           `json:"startToClose,omitempty"`
	Heartbeat       string           `json:"heartbeat,omitempty"`
	TaskQueue       string           `json:"taskQueue,omitempty"`
	Retry           *serializedRetry `json:"retry,omitempty"`
}

type serializedPatch struct {
	ID        string `json:"id"`
	DefaultOn bool   `json:"defaultOn,omitempty"`
	Note      string `json:"note,omitempty"`
}

type serializedTypeMap struct {
	Type  string `json:"type"`
	Route string `json:"route"`
}

// MarshalDescription produces the canonical manifest for the provided description.
func MarshalDescription(desc *Description) ([]byte, error) {
	if desc == nil {
		return nil, fmt.Errorf("actors: description is nil")
	}
	schema := toSerializedActor(desc.Metadata())
	payload, err := marshalCanonical(schema)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	manifest := DescriptionManifest{
		Version: descriptionManifestVersion,
		Hash:    hex.EncodeToString(sum[:]),
		Actor:   schema,
	}
	return marshalCanonical(manifest)
}

// UnmarshalDescription parses a manifest into its typed form.
func UnmarshalDescription(data []byte) (DescriptionManifest, error) {
	var manifest DescriptionManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return DescriptionManifest{}, err
	}
	if manifest.Version != descriptionManifestVersion {
		return DescriptionManifest{}, fmt.Errorf("actors: manifest version %d unsupported", manifest.Version)
	}
	return manifest, nil
}

// Metadata converts the manifest back into ActorMetadata.
func (m DescriptionManifest) Metadata() (ActorMetadata, error) {
	return fromSerializedActor(m.Actor)
}

// VerifyHash recomputes the schema hash and reports mismatches.
func (m DescriptionManifest) VerifyHash() error {
	payload, err := marshalCanonical(m.Actor)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	expected := hex.EncodeToString(sum[:])
	if expected != m.Hash {
		return fmt.Errorf("actors: manifest hash mismatch, expected %s got %s", expected, m.Hash)
	}
	return nil
}

func toSerializedActor(meta ActorMetadata) serializedActor {
	out := serializedActor{
		Kind:          meta.Kind,
		VersionTag:    meta.VersionTag,
		WorkflowQueue: meta.WorkflowQueue,
		ActivityQueue: meta.ActivityQueue,
		Timeout:       formatDuration(meta.DefaultTimeout),
		SnapshotEvery: meta.SnapshotEvery,
		Start:         serializedStart{Input: meta.Start.InputType},
		Commands:      make([]serializedCommand, 0, len(meta.Commands)),
		Queries:       make([]serializedQuery, 0, len(meta.Queries)),
		Activities:    make([]serializedActivity, 0, len(meta.Activities)),
		Patches:       make([]serializedPatch, 0, len(meta.Patches)),
		CommandTypes:  toSerializedTypeMap(meta.CommandTypes),
		QueryTypes:    toSerializedTypeMap(meta.QueryTypes),
		ActivityTypes: toSerializedTypeMap(meta.ActivityTypes),
	}
	out.Retry = toSerializedRetry(meta.DefaultRetry)
	for _, cmd := range meta.Commands {
		out.Commands = append(out.Commands, serializedCommand{
			Name:          cmd.Name,
			RequestType:   cmd.RequestType,
			ResponseType:  cmd.ResponseType,
			Timeout:       formatDuration(cmd.Timeout),
			SignalTimeout: formatDuration(cmd.SignalTimeout),
			Retry:         toSerializedRetry(cmd.Retry),
			HasValidator:  cmd.HasValidator,
		})
	}
	for _, qry := range meta.Queries {
		out.Queries = append(out.Queries, serializedQuery{
			Name:         qry.Name,
			RequestType:  qry.RequestType,
			ResponseType: qry.ResponseType,
			CacheTTL:     formatDuration(qry.CacheTTL),
		})
	}
	for _, act := range meta.Activities {
		out.Activities = append(out.Activities, serializedActivity{
			Name:            act.Name,
			RequestType:     act.RequestType,
			ResponseType:    act.ResponseType,
			ScheduleToClose: formatDuration(act.ScheduleToClose),
			ScheduleToStart: formatDuration(act.ScheduleToStart),
			StartToClose:    formatDuration(act.StartToClose),
			Heartbeat:       formatDuration(act.Heartbeat),
			TaskQueue:       act.TaskQueue,
			Retry:           toSerializedRetry(act.Retry),
		})
	}
	for _, patch := range meta.Patches {
		out.Patches = append(out.Patches, serializedPatch{
			ID:        patch.ID,
			DefaultOn: patch.DefaultOn,
			Note:      patch.Note,
		})
	}
	return out
}

func fromSerializedActor(s serializedActor) (ActorMetadata, error) {
	timeout, err := parseDuration(s.Timeout)
	if err != nil {
		return ActorMetadata{}, fmt.Errorf("actors: invalid actor timeout: %w", err)
	}
	meta := ActorMetadata{
		Kind:           s.Kind,
		VersionTag:     s.VersionTag,
		WorkflowQueue:  s.WorkflowQueue,
		ActivityQueue:  s.ActivityQueue,
		DefaultTimeout: timeout,
		SnapshotEvery:  s.SnapshotEvery,
		Start:          StartMetadata{InputType: s.Start.Input},
		CommandTypes:   fromSerializedTypeMap(s.CommandTypes),
		QueryTypes:     fromSerializedTypeMap(s.QueryTypes),
		ActivityTypes:  fromSerializedTypeMap(s.ActivityTypes),
	}
	retry, err := fromSerializedRetry(s.Retry)
	if err != nil {
		return ActorMetadata{}, err
	}
	meta.DefaultRetry = retry
	if len(s.Commands) > 0 {
		meta.Commands = make([]CommandMetadata, 0, len(s.Commands))
		for _, cmd := range s.Commands {
			cmdTimeout, err := parseDuration(cmd.Timeout)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: command %s timeout: %w", cmd.Name, err)
			}
			signalTimeout, err := parseDuration(cmd.SignalTimeout)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: command %s signal timeout: %w", cmd.Name, err)
			}
			cmdRetry, err := fromSerializedRetry(cmd.Retry)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: command %s retry: %w", cmd.Name, err)
			}
			meta.Commands = append(meta.Commands, CommandMetadata{
				Name:          cmd.Name,
				RequestType:   cmd.RequestType,
				ResponseType:  cmd.ResponseType,
				Timeout:       cmdTimeout,
				SignalTimeout: signalTimeout,
				Retry:         cmdRetry,
				HasValidator:  cmd.HasValidator,
			})
		}
	}
	if len(s.Queries) > 0 {
		meta.Queries = make([]QueryMetadata, 0, len(s.Queries))
		for _, qry := range s.Queries {
			cacheTTL, err := parseDuration(qry.CacheTTL)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: query %s cache ttl: %w", qry.Name, err)
			}
			meta.Queries = append(meta.Queries, QueryMetadata{
				Name:         qry.Name,
				RequestType:  qry.RequestType,
				ResponseType: qry.ResponseType,
				CacheTTL:     cacheTTL,
			})
		}
	}
	if len(s.Activities) > 0 {
		meta.Activities = make([]ActivityMetadata, 0, len(s.Activities))
		for _, act := range s.Activities {
			scheduleToClose, err := parseDuration(act.ScheduleToClose)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: invalid activity scheduleToClose: %w", err)
			}
			scheduleToStart, err := parseDuration(act.ScheduleToStart)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: invalid activity scheduleToStart: %w", err)
			}
			startToClose, err := parseDuration(act.StartToClose)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: invalid activity startToClose: %w", err)
			}
			heartbeat, err := parseDuration(act.Heartbeat)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: invalid activity heartbeat: %w", err)
			}
			retry, err := fromSerializedRetry(act.Retry)
			if err != nil {
				return ActorMetadata{}, fmt.Errorf("actors: invalid activity retry: %w", err)
			}
			meta.Activities = append(meta.Activities, ActivityMetadata{
				Name:            act.Name,
				RequestType:     act.RequestType,
				ResponseType:    act.ResponseType,
				ScheduleToClose: scheduleToClose,
				ScheduleToStart: scheduleToStart,
				StartToClose:    startToClose,
				Heartbeat:       heartbeat,
				TaskQueue:       act.TaskQueue,
				Retry:           retry,
			})
		}
	}
	if len(s.Patches) > 0 {
		meta.Patches = make([]PatchMetadata, 0, len(s.Patches))
		for _, patch := range s.Patches {
			meta.Patches = append(meta.Patches, PatchMetadata{
				ID:        patch.ID,
				DefaultOn: patch.DefaultOn,
				Note:      patch.Note,
			})
		}
	}
	return meta, nil
}

func toSerializedRetry(policy RetryPolicy) *serializedRetry {
	if policy == (RetryPolicy{}) {
		return nil
	}
	return &serializedRetry{
		MaxAttempts:        policy.MaxAttempts,
		InitialInterval:    formatDuration(policy.InitialInterval),
		BackoffCoefficient: policy.BackoffCoefficient,
	}
}

func fromSerializedRetry(src *serializedRetry) (RetryPolicy, error) {
	if src == nil {
		return RetryPolicy{}, nil
	}
	interval, err := parseDuration(src.InitialInterval)
	if err != nil {
		return RetryPolicy{}, fmt.Errorf("actors: invalid retry interval: %w", err)
	}
	return RetryPolicy{
		MaxAttempts:        src.MaxAttempts,
		InitialInterval:    interval,
		BackoffCoefficient: src.BackoffCoefficient,
	}, nil
}

func toSerializedTypeMap(source map[string]string) []serializedTypeMap {
	if len(source) == 0 {
		return nil
	}
	keys := make([]string, 0, len(source))
	for k := range source {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]serializedTypeMap, 0, len(keys))
	for _, key := range keys {
		out = append(out, serializedTypeMap{
			Type:  key,
			Route: source[key],
		})
	}
	return out
}

func fromSerializedTypeMap(entries []serializedTypeMap) map[string]string {
	if len(entries) == 0 {
		return nil
	}
	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		out[entry.Type] = entry.Route
	}
	return out
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

func parseDuration(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	dur, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	return dur, nil
}

func marshalCanonical(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}
