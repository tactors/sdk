package runtime

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

func TestEncryptionCodecRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	codec, err := NewEncryptionCodec(key)
	require.NoError(t, err)

	payload := &commonpb.Payload{
		Metadata: map[string][]byte{
			converter.MetadataEncoding: []byte("binary/test"),
			"hello":                    []byte("world"),
		},
		Data: []byte("secret"),
	}

	encoded, err := codec.Encode([]*commonpb.Payload{payload})
	require.NoError(t, err)
	require.Len(t, encoded, 1)
	require.NotEqual(t, payload.GetData(), encoded[0].GetData(), "ciphertext should differ from plaintext")

	decoded, err := codec.Decode(encoded)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, payload.GetData(), decoded[0].GetData())
	require.Equal(t, payload.GetMetadata(), decoded[0].GetMetadata())
}

func TestEncryptionCodecSkipUnencrypted(t *testing.T) {
	key := bytes.Repeat([]byte{0x24}, 16)
	codec, err := NewEncryptionCodec(key)
	require.NoError(t, err)
	payload := &commonpb.Payload{
		Metadata: map[string][]byte{
			converter.MetadataEncoding: []byte("binary/plain"),
		},
		Data: []byte("hello"),
	}
	out, err := codec.Decode([]*commonpb.Payload{payload})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, payload.GetData(), out[0].GetData())
	require.Equal(t, payload.GetMetadata(), out[0].GetMetadata())
}

func TestEncryptionCodecRejectsBadNonce(t *testing.T) {
	key := bytes.Repeat([]byte{0x11}, 32)
	codec, err := NewEncryptionCodec(key)
	require.NoError(t, err)
	enc := &commonpb.Payload{
		Metadata: map[string][]byte{
			converter.MetadataEncoding: []byte(metadataEncodingEncrypted),
			metadataEncryptionNonce:    []byte("bad"),
		},
		Data: []byte("payload"),
	}
	_, err = codec.Decode([]*commonpb.Payload{enc})
	require.Error(t, err)
}

func TestNewEncryptionCodecKeyLength(t *testing.T) {
	_, err := NewEncryptionCodec([]byte("short"))
	require.Error(t, err)
}
