package rand

import (
	"errors"
	"strings"
	"testing"
)

func TestIDIncludesPrefixAndHex(t *testing.T) {
	const prefix = "actor"
	id := ID(prefix)
	if !strings.HasPrefix(id, prefix+"-") {
		t.Fatalf("unexpected prefix: %s", id)
	}
	hex := id[len(prefix)+1:]
	if len(hex) != 16 {
		t.Fatalf("expected 16 hex chars, got %d (%s)", len(hex), id)
	}
}

func TestIDFallsBackOnEntropyFailure(t *testing.T) {
	original := randomRead
	defer func() { randomRead = original }()
	randomRead = func([]byte) (int, error) {
		return 0, errors.New("boom")
	}

	if got := ID("fallback"); got != "fallback" {
		t.Fatalf("expected plain prefix fallback, got %s", got)
	}
}
