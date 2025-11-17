package actors_test

import (
	"errors"
	"testing"
	"time"

	"github.com/tactors/sdk/actors"
)

type stubCtx struct {
	sendErr         error
	sentRef         actors.Ref
	sentPayload     any
	oneshotResult   any
	spawnRef        actors.Ref
	spawnErr        error
	spawnedKind     string
	queryResult     any
	queryErr        error
	askResult       any
	askErr          error
	searchAttrs     map[string]any
	activityValue   any
	activityErr     error
	activityDecoder func(string, any) (any, error)
	backgroundCalls []struct {
		name    string
		payload any
	}
	memo            map[string]any
	versionOverride *int
	versionChangeID string
	correlation     actors.CorrelationData
}

func (stubCtx) ActorID() string           { return "test" }
func (stubCtx) Now() time.Time            { return time.Unix(0, 0) }
func (stubCtx) Sleep(time.Duration) error { return nil }
func (s *stubCtx) Version(changeID string, defaultVersion, newVersion int) int {
	s.versionChangeID = changeID
	if s.versionOverride != nil {
		return *s.versionOverride
	}
	return newVersion
}
func (stubCtx) Logger() actors.Logger { return nil }

func (s *stubCtx) SpawnOneShot(payload any, opts ...actors.SpawnOption) (any, error) {
	return s.oneshotResult, nil
}

func (s *stubCtx) SpawnChild(kind string, init any, opts ...actors.SpawnOption) (actors.Ref, error) {
	s.spawnedKind = kind
	return s.spawnRef, s.spawnErr
}

func (s *stubCtx) Self() actors.Ref {
	return actors.Ref{Kind: "test", ID: "test"}
}

func (s *stubCtx) SendCommand(ref actors.Ref, payload any) error {
	s.sentRef = ref
	s.sentPayload = payload
	return s.sendErr
}

func (s *stubCtx) QueryActor(ref actors.Ref, payload any) (any, error) {
	s.sentRef = ref
	s.sentPayload = payload
	return s.queryResult, s.queryErr
}

func (s *stubCtx) AskActor(ref actors.Ref, payload any) (any, error) {
	s.sentRef = ref
	s.sentPayload = payload
	return s.askResult, s.askErr
}

func (s *stubCtx) Effect(key string, fn actors.EffectFunc, opts ...actors.EffectOption) (any, error) {
	if fn == nil {
		return nil, errors.New("effect func is nil")
	}
	return fn(s)
}

func (s *stubCtx) BackgroundActivity(name string, payload any) {
	s.backgroundCalls = append(s.backgroundCalls, struct {
		name    string
		payload any
	}{name: name, payload: payload})
}

func (s *stubCtx) Activity(name string, payload any) actors.ActivityFuture {
	return stubActivityFuture{value: s.activityValue, err: s.activityErr}
}

func (s *stubCtx) ActivityWithOptions(name string, payload any, opts actors.ActivityCallOptions) actors.ActivityFuture {
	return stubActivityFuture{value: s.activityValue, err: s.activityErr}
}

func (s *stubCtx) DecodeActivityResult(name string, value any) (any, error) {
	if s.activityDecoder != nil {
		return s.activityDecoder(name, value)
	}
	return value, nil
}

func (s *stubCtx) UpsertSearchAttributes(attrs map[string]any) error {
	if s.searchAttrs == nil {
		s.searchAttrs = make(map[string]any)
	}
	for k, v := range attrs {
		s.searchAttrs[k] = v
	}
	return nil
}

func (s *stubCtx) SearchAttributes() map[string]any {
	if len(s.searchAttrs) == 0 {
		return nil
	}
	out := make(map[string]any, len(s.searchAttrs))
	for k, v := range s.searchAttrs {
		out[k] = v
	}
	return out
}

func (s *stubCtx) Memo() map[string]any {
	return s.memo
}

func (s *stubCtx) MessageMetadata() actors.MessageMetadata {
	return actors.MessageMetadata{Correlation: s.correlation}
}

func (s *stubCtx) Correlation() actors.CorrelationData {
	return s.correlation
}

func (s *stubCtx) SetCorrelation(data actors.CorrelationData) {
	s.correlation = data
}

func (stubCtx) SnapshotInfo() actors.SnapshotInfo {
	return actors.SnapshotInfo{}
}

type stubActivityFuture struct {
	value any
	err   error
}

func (f stubActivityFuture) Get() (any, error) {
	return f.value, f.err
}

type sampleCommand struct {
	actors.CommandMsg[sampleResponse]
	Value string
}

type sampleResponse struct {
	Result string
}

type sampleQuery struct {
	actors.QueryMsg[sampleQueryResponse]
}

type sampleQueryResponse struct {
	Value string
}

type sampleActivity struct {
	actors.ActivityMsg[sampleResponse]
	Value string
}

type sampleNoopActivity struct {
	actors.ActivityMsg[struct{}]
}

func TestSpawnOneShotDecodesMap(t *testing.T) {
	ctx := &stubCtx{oneshotResult: map[string]any{"Result": "OK"}}
	resp, err := actors.SpawnOneShot(ctx, sampleCommand{Value: "x"})
	if err != nil {
		t.Fatalf("SpawnOneShot: %v", err)
	}
	if resp.Result != "OK" {
		t.Fatalf("expected OK, got %v", resp.Result)
	}
}

func TestTellUnsupported(t *testing.T) {
	ctx := struct {
		actors.Ctx
	}{}
	err := actors.Tell[sampleCommand, sampleResponse](ctx, actors.Ref{Kind: "k", ID: "id"}, sampleCommand{Value: "v"})
	if !errors.Is(err, actors.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
}

func TestTellDelegates(t *testing.T) {
	ctx := &stubCtx{}
	cmd := sampleCommand{Value: "ping"}
	err := actors.Tell[sampleCommand, sampleResponse](ctx, actors.Ref{Kind: "child", ID: "child-1"}, cmd)
	if err != nil {
		t.Fatalf("Tell: %v", err)
	}
	if ctx.sentRef.ID != "child-1" {
		t.Fatalf("unexpected ref: %+v", ctx.sentRef)
	}
	if got, ok := ctx.sentPayload.(sampleCommand); !ok || got.Value != "ping" {
		t.Fatalf("payload mismatch: %#v", ctx.sentPayload)
	}
}

func TestQueryActorHelper(t *testing.T) {
	ctx := &stubCtx{queryResult: map[string]any{"Value": "state"}}
	resp, err := actors.QueryActor[sampleQuery, sampleQueryResponse](ctx, actors.Ref{Kind: "child", ID: "child-1"}, sampleQuery{})
	if err != nil {
		t.Fatalf("QueryActor: %v", err)
	}
	if resp.Value != "state" {
		t.Fatalf("expected state, got %s", resp.Value)
	}
	if ctx.sentRef.ID != "child-1" {
		t.Fatalf("unexpected ref in query: %+v", ctx.sentRef)
	}
}

func TestAskHelper(t *testing.T) {
	ctx := &stubCtx{askResult: map[string]any{"Result": "ASK"}}
	resp, err := actors.Ask[sampleCommand, sampleResponse](ctx, actors.Ref{Kind: "child", ID: "child-1"}, sampleCommand{Value: "ping"})
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}
	if resp.Result != "ASK" {
		t.Fatalf("expected ASK, got %s", resp.Result)
	}
}

func TestSearchAttributesHelpers(t *testing.T) {
	ctx := &stubCtx{}
	if err := actors.UpsertSearchAttributes(ctx, map[string]any{"foo": "bar"}); err != nil {
		t.Fatalf("UpsertSearchAttributes: %v", err)
	}
	attrs := actors.SearchAttributes(ctx)
	if attrs["foo"] != "bar" {
		t.Fatalf("expected bar, got %v", attrs["foo"])
	}
}

func TestContinueAsNewUnsupported(t *testing.T) {
	ctx := struct {
		actors.Ctx
	}{}
	if err := actors.ContinueAsNew(ctx, nil); !errors.Is(err, actors.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
}

func TestRunActivityUsesDecoder(t *testing.T) {
	ctx := &stubCtx{
		activityValue: map[string]any{"Result": "OK"},
		activityDecoder: func(name string, value any) (any, error) {
			m, _ := value.(map[string]any)
			return sampleResponse{Result: m["Result"].(string)}, nil
		},
	}
	resp, err := actors.RunActivity(ctx, "sendConfirmation", sampleActivity{Value: "req"})
	if err != nil {
		t.Fatalf("RunActivity: %v", err)
	}
	if resp.Result != "OK" {
		t.Fatalf("expected OK, got %s", resp.Result)
	}
}

func TestPatchHelper(t *testing.T) {
	ctx := &stubCtx{}
	if !actors.Patch(ctx, "change-1") {
		t.Fatalf("expected patch to be enabled")
	}
	defaultVersion := actors.DefaultVersion
	ctx.versionOverride = &defaultVersion
	if actors.Patch(ctx, "change-2") {
		t.Fatalf("expected patch disabled when default version returned")
	}
	if ctx.versionChangeID != "change-2" {
		t.Fatalf("version change id not recorded")
	}
}

func TestSpawnHelper(t *testing.T) {
	ctx := &stubCtx{spawnRef: actors.Ref{Kind: "child", ID: "child-42"}}
	ref, err := actors.Spawn(ctx, "child", struct{}{}, actors.WithChildName("child-42"))
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if ref.ID != "child-42" || ctx.spawnedKind != "child" {
		t.Fatalf("spawn mismatch: ref=%+v kind=%s", ref, ctx.spawnedKind)
	}
}

func TestSpawnUnsupported(t *testing.T) {
	ctx := struct {
		actors.Ctx
	}{}
	if _, err := actors.Spawn(ctx, "child", struct{}{}); !errors.Is(err, actors.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
}

func TestRunActivityNoResult(t *testing.T) {
	ctx := &stubCtx{activityValue: struct{}{}}
	if err := actors.RunActivityNoResult(ctx, "noop", sampleNoopActivity{}); err != nil {
		t.Fatalf("RunActivityNoResult: %v", err)
	}
}

func TestBackgroundActivityDelegates(t *testing.T) {
	ctx := &stubCtx{}
	actors.BackgroundActivity(ctx, "log", map[string]string{"a": "b"})
	if len(ctx.backgroundCalls) != 1 || ctx.backgroundCalls[0].name != "log" {
		t.Fatalf("background activity not recorded: %#v", ctx.backgroundCalls)
	}
}

func TestMemoHelper(t *testing.T) {
	ctx := &stubCtx{memo: map[string]any{"foo": "bar"}}
	memo := actors.Memo(ctx)
	if memo["foo"] != "bar" {
		t.Fatalf("memo mismatch: %#v", memo)
	}
}
