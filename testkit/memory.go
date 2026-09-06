package testkit

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tactors/sdk/actors"
)

// MemoryCtx is an in-process actors.Ctx for unit-testing handler functions
// without Temporal. It implements a real, channel-based WaitForEvent so a test
// can run a handler in a goroutine, deliver an event with DeliverEvent, and
// observe the handler resume. Activities, spawning and cross-actor messaging
// are not supported and return actors.ErrUnsupported.
//
// Semantics mirror the Temporal runtime: events are buffered per name until a
// handler waits for them, and a name is namespaced through
// actors.EventSignalName so it cannot collide with commands.
type MemoryCtx struct {
	id          string
	now         func() time.Time
	mu          sync.Mutex
	mailboxes   map[string]chan any
	attrs       map[string]any
	correlation actors.CorrelationData
	meta        actors.MessageMetadata
	logger      actors.Logger
}

// memoryMailboxSize bounds how many undelivered events a single name buffers.
const memoryMailboxSize = 1024

// NewMemoryCtx constructs an in-memory context for the given actor id.
func NewMemoryCtx(id string) *MemoryCtx {
	return &MemoryCtx{
		id:        id,
		now:       time.Now,
		mailboxes: make(map[string]chan any),
		attrs:     make(map[string]any),
	}
}

// DeliverEvent pushes an event into the context's mailbox. It never blocks; it
// returns an error when the per-name buffer is full or the name is invalid.
func (m *MemoryCtx) DeliverEvent(name string, payload any) error {
	signal, err := actors.EventSignalName(name)
	if err != nil {
		return err
	}
	select {
	case m.mailbox(signal) <- payload:
		return nil
	default:
		return fmt.Errorf("testkit: event mailbox %q is full", name)
	}
}

// WaitForEvent implements actors.Ctx with a real blocking wait.
func (m *MemoryCtx) WaitForEvent(name string, timeout time.Duration) (any, error) {
	signal, err := actors.EventSignalName(name)
	if err != nil {
		return nil, err
	}
	ch := m.mailbox(signal)
	if timeout <= 0 {
		return <-ch, nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case payload := <-ch:
		return payload, nil
	case <-timer.C:
		return nil, fmt.Errorf("%w: event %q after %s", actors.ErrEventTimeout, name, timeout)
	}
}

func (m *MemoryCtx) mailbox(signal string) chan any {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.mailboxes[signal]
	if !ok {
		ch = make(chan any, memoryMailboxSize)
		m.mailboxes[signal] = ch
	}
	return ch
}

// PendingEvents reports how many undelivered events are buffered for name.
func (m *MemoryCtx) PendingEvents(name string) int {
	signal, err := actors.EventSignalName(name)
	if err != nil {
		return 0
	}
	return len(m.mailbox(signal))
}

// SetNow overrides the clock used by Now.
func (m *MemoryCtx) SetNow(fn func() time.Time) {
	if fn != nil {
		m.now = fn
	}
}

// SetLogger installs a logger; nil restores the default no-op logger.
func (m *MemoryCtx) SetLogger(logger actors.Logger) { m.logger = logger }

// SetMessageMetadata overrides the metadata returned by MessageMetadata.
func (m *MemoryCtx) SetMessageMetadata(meta actors.MessageMetadata) { m.meta = meta }

func (m *MemoryCtx) ActorID() string { return m.id }
func (m *MemoryCtx) Now() time.Time  { return m.now() }
func (m *MemoryCtx) Sleep(d time.Duration) error {
	if d > 0 {
		time.Sleep(d)
	}
	return nil
}
func (m *MemoryCtx) Version(_ string, _, newVersion int) int { return newVersion }
func (m *MemoryCtx) Activity(string, any) actors.ActivityFuture {
	return memoryFuture{err: actors.ErrUnsupported}
}
func (m *MemoryCtx) ActivityWithOptions(string, any, actors.ActivityCallOptions) actors.ActivityFuture {
	return memoryFuture{err: actors.ErrUnsupported}
}
func (m *MemoryCtx) BackgroundActivity(string, any) {}
func (m *MemoryCtx) Logger() actors.Logger {
	if m.logger == nil {
		return memoryLogger{}
	}
	return m.logger
}
func (m *MemoryCtx) Self() actors.Ref { return actors.Ref{ID: m.id} }
func (m *MemoryCtx) UpsertSearchAttributes(attrs map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range attrs {
		m.attrs[k] = v
	}
	return nil
}
func (m *MemoryCtx) SearchAttributes() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]any, len(m.attrs))
	for k, v := range m.attrs {
		out[k] = v
	}
	return out
}
func (m *MemoryCtx) MessageMetadata() actors.MessageMetadata { return m.meta }
func (m *MemoryCtx) Effect(_ string, fn actors.EffectFunc, _ ...actors.EffectOption) (any, error) {
	if fn == nil {
		return nil, errors.New("testkit: effect function is nil")
	}
	return fn(m)
}
func (m *MemoryCtx) Correlation() actors.CorrelationData        { return m.correlation.Clone() }
func (m *MemoryCtx) SetCorrelation(data actors.CorrelationData) { m.correlation = data.Clone() }
func (m *MemoryCtx) SnapshotInfo() actors.SnapshotInfo          { return actors.SnapshotInfo{} }

type memoryFuture struct {
	err error
}

func (f memoryFuture) Get() (any, error) { return nil, f.err }

type memoryLogger struct{}

func (memoryLogger) Debug(string, ...any) {}
func (memoryLogger) Info(string, ...any)  {}
func (memoryLogger) Warn(string, ...any)  {}
func (memoryLogger) Error(string, ...any) {}
