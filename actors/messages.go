package actors

import (
	"maps"
	"time"
)

// CommandMsg is a helper embed that ties a request to its response type.
type CommandMsg[Resp any] struct{}

// CommandResponsePrototype satisfies the TypedCommandMessage interface.
func (CommandMsg[Resp]) CommandResponsePrototype() Resp {
	var zero Resp
	return zero
}

// TypedCommandMessage allows runtimes to infer a command's response type.
type TypedCommandMessage[Resp any] interface {
	CommandResponsePrototype() Resp
}

// QueryMsg is a helper embed that ties a query request to its response type.
type QueryMsg[Resp any] struct{}

// QueryResponsePrototype satisfies the TypedQueryMessage interface.
func (QueryMsg[Resp]) QueryResponsePrototype() Resp {
	var zero Resp
	return zero
}

// TypedQueryMessage allows runtimes to infer a query's response type.
type TypedQueryMessage[Resp any] interface {
	QueryResponsePrototype() Resp
}

// ActivityMsg ties an activity request to its response type.
type ActivityMsg[Resp any] struct{}

// ActivityResponsePrototype satisfies the TypedActivityMessage interface.
func (ActivityMsg[Resp]) ActivityResponsePrototype() Resp {
	var zero Resp
	return zero
}

// TypedActivityMessage allows helpers to infer activity response types.
type TypedActivityMessage[Resp any] interface {
	ActivityResponsePrototype() Resp
}

// CorrelationData carries saga/trace identifiers and arbitrary annotations.
type CorrelationData struct {
	SagaID     string
	TraceID    string
	ParentID   string
	Attributes map[string]string
}

// Clone returns a deep copy of the correlation data.
func (c CorrelationData) Clone() CorrelationData {
	out := c
	if len(c.Attributes) > 0 {
		out.Attributes = maps.Clone(c.Attributes)
	}
	return out
}

// IsZero reports whether the correlation payload is empty.
func (c CorrelationData) IsZero() bool {
	return c.SagaID == "" && c.TraceID == "" && c.ParentID == "" && len(c.Attributes) == 0
}

// MessageMetadata carries correlation data about the currently processed message.
type MessageMetadata struct {
	// ID uniquely identifies the delivery attempt within the receiving workflow.
	ID string
	// CorrelationID links the delivery back to the caller; defaults to ID when unset.
	CorrelationID string
	// Correlation carries saga IDs or tracing spans that should propagate downstream.
	Correlation CorrelationData
	// Deadline is optional and represents the point after which the call is considered expired.
	Deadline time.Time
	// RetryBudget expresses how many retries remain from the caller's perspective.
	RetryBudget int
	// Caller identifies who initiated the delivery if known.
	Caller Ref
}

// HasDeadline reports whether a deadline is configured.
func (m MessageMetadata) HasDeadline() bool {
	return !m.Deadline.IsZero()
}
