package runtime

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
)

func TestConfigurePayloadCodecsEncryptsPayloads(t *testing.T) {
	t.Cleanup(func() { ConfigurePayloadCodecs() })

	key := bytes.Repeat([]byte{0xAB}, 32)
	codec, err := NewEncryptionCodec(key)
	require.NoError(t, err)

	ConfigurePayloadCodecs(codec)
	dc := dataConverter()

	type sample struct {
		Secret string
	}
	value := sample{Secret: "value"}

	payload, err := dc.ToPayload(value)
	require.NoError(t, err)
	require.Equal(t, metadataEncodingEncrypted, string(payload.Metadata[converter.MetadataEncoding]))

	var decoded sample
	require.NoError(t, dc.FromPayload(payload, &decoded))
	require.Equal(t, value, decoded)

	ConfigurePayloadCodecs()
	dc = dataConverter()
	plain, err := dc.ToPayload(value)
	require.NoError(t, err)
	require.Equal(t, metadataEncodingCBOR, string(plain.Metadata[converter.MetadataEncoding]))
}
