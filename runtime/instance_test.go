package runtime

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tactors/sdk/actors"
)

func TestPrepareCommandRequest(t *testing.T) {
	desc := &actors.Description{
		Kind: "demo",
		Commands: map[string]actors.CommandSpec{
			"ok": {
				DecodePayload: nil,
			},
			"fail": {
				DecodePayload: func(any) (any, error) {
					return nil, fmt.Errorf("decode failure")
				},
			},
		},
	}
	inst := newTemporalInstance(desc)

	t.Run("missing command", func(t *testing.T) {
		_, _, ok, err := inst.prepareCommandRequest("missing", nil)
		require.False(t, ok)
		require.NoError(t, err)
	})

	t.Run("decode error", func(t *testing.T) {
		_, _, ok, err := inst.prepareCommandRequest("fail", "payload")
		require.True(t, ok)
		require.EqualError(t, err, "decode failure")
	})

	t.Run("success", func(t *testing.T) {
		spec, payload, ok, err := inst.prepareCommandRequest("ok", "hello")
		require.True(t, ok)
		require.NoError(t, err)
		require.Equal(t, desc.Commands["ok"], spec)
		require.Equal(t, "hello", payload)
	})
}
