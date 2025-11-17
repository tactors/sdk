package actors

import (
	"errors"
	"time"
)

var (
	// ErrStopLoop signals the workflow loop should terminate gracefully.
	ErrStopLoop = errors.New("actors: stop loop")
)

// BusinessError wraps err so runtimes can reply with application errors
// without treating them as fatal workflow failures.
func BusinessError(err error) error {
	if err == nil {
		return nil
	}
	return &businessError{err: err}
}

// AsBusinessError reports whether err was created via BusinessError.
func AsBusinessError(err error) (error, bool) {
	var b *businessError
	if errors.As(err, &b) && b != nil {
		return b.err, true
	}
	return nil, false
}

type businessError struct {
	err error
}

func (b *businessError) Error() string { return b.err.Error() }
func (b *businessError) Unwrap() error { return b.err }

// NonRetryable marks an error so runtimes treat it as non-retryable.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// IsNonRetryable reports whether err was wrapped via NonRetryable.
func IsNonRetryable(err error) bool {
	var n *nonRetryableError
	return errors.As(err, &n)
}

type nonRetryableError struct {
	err error
}

func (n nonRetryableError) Error() string { return n.err.Error() }
func (n nonRetryableError) Unwrap() error { return n.err }

// RetryAfter requests the runtime to retry the message after delay.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, delay: delay}
}

// IsRetryAfter extracts the requested retry delay.
func IsRetryAfter(err error) (time.Duration, bool) {
	var r *retryAfterError
	if errors.As(err, &r) {
		return r.delay, true
	}
	return 0, false
}

type retryAfterError struct {
	err   error
	delay time.Duration
}

func (r retryAfterError) Error() string        { return r.err.Error() }
func (r retryAfterError) Unwrap() error        { return r.err }
func (r retryAfterError) Delay() time.Duration { return r.delay }

// WithCause attaches an explanatory cause to err for richer diagnostics.
func WithCause(err, cause error) error {
	if err == nil || cause == nil {
		return err
	}
	return &causeError{err: err, cause: cause}
}

// Cause extracts the underlying cause attached via WithCause.
func Cause(err error) error {
	var c *causeError
	if errors.As(err, &c) {
		return c.cause
	}
	return nil
}

type causeError struct {
	err   error
	cause error
}

func (c *causeError) Error() string { return c.err.Error() }
func (c *causeError) Unwrap() error { return c.err }
