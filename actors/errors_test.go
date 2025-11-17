package actors

import (
	"errors"
	"testing"
	"time"
)

func TestBusinessErrorHelpers(t *testing.T) {
	base := errors.New("missing")
	err := BusinessError(base)
	inner, ok := AsBusinessError(err)
	if !ok {
		t.Fatalf("expected business error")
	}
	if inner != base {
		t.Fatalf("unexpected inner error: %v", inner)
	}
	if _, ok := AsBusinessError(nil); ok {
		t.Fatalf("nil should not unwrap")
	}
}

func TestNonRetryable(t *testing.T) {
	err := NonRetryable(errors.New("fail"))
	if !IsNonRetryable(err) {
		t.Fatalf("expected non-retryable")
	}
	if IsNonRetryable(errors.New("other")) {
		t.Fatalf("unexpected non-retryable")
	}
}

func TestRetryAfter(t *testing.T) {
	delay := 5 * time.Second
	err := RetryAfter(errors.New("again"), delay)
	got, ok := IsRetryAfter(err)
	if !ok || got != delay {
		t.Fatalf("expected retry-after %v, got %v ok=%v", delay, got, ok)
	}
}

func TestWithCause(t *testing.T) {
	cause := errors.New("io")
	err := WithCause(errors.New("wrap"), cause)
	if Cause(err) != cause {
		t.Fatalf("expected cause")
	}
}
