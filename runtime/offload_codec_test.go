package runtime

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type memOffloadStore struct {
	mu      sync.Mutex
	items   map[string][]byte
	puts    int
	putKeys []string
}

func (s *memOffloadStore) Put(key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = make(map[string][]byte)
	}
	if key == "" {
		return errors.New("empty key")
	}
	s.items[key] = append([]byte(nil), payload...)
	s.puts++
	s.putKeys = append(s.putKeys, key)
	return nil
}

func (s *memOffloadStore) Get(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.items[key]
	if !ok {
		return nil, errors.New("missing payload")
	}
	return append([]byte(nil), val...), nil
}

func (s *memOffloadStore) getRaw(key string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	val := s.items[key]
	return append([]byte(nil), val...)
}

func (s *memOffloadStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.putKeys))
	copy(out, s.putKeys)
	return out
}

func TestOffloadCodecOffloadsLargePayloads(t *testing.T) {
	t.Cleanup(func() { ConfigurePayloadCodecs() })

	store := &memOffloadStore{}
	codec, err := NewOffloadCodec(store, OffloadCodecOptions{ThresholdBytes: 512})
	require.NoError(t, err)
	ConfigurePayloadCodecs(codec)

	dc := dataConverter()
	value := bytes.Repeat([]byte("a"), 2048)

	payload, err := dc.ToPayload(value)
	require.NoError(t, err)
	require.Equal(t, metadataEncodingOffload, string(payload.Metadata[converter.MetadataEncoding]))
	require.NotEmpty(t, payload.Metadata[metadataOffloadKey])
	require.Equal(t, 1, store.puts)

	var decoded []byte
	require.NoError(t, dc.FromPayload(payload, &decoded))
	require.Equal(t, value, decoded)
}

func TestOffloadCodecKeepsSmallPayloadsInline(t *testing.T) {
	t.Cleanup(func() { ConfigurePayloadCodecs() })

	store := &memOffloadStore{}
	codec, err := NewOffloadCodec(store, OffloadCodecOptions{ThresholdBytes: 4096})
	require.NoError(t, err)
	ConfigurePayloadCodecs(codec)

	dc := dataConverter()
	value := []byte("small-payload")

	payload, err := dc.ToPayload(value)
	require.NoError(t, err)
	require.NotEqual(t, metadataEncodingOffload, string(payload.Metadata[converter.MetadataEncoding]))
	require.Equal(t, 0, store.puts)

	var decoded []byte
	require.NoError(t, dc.FromPayload(payload, &decoded))
	require.Equal(t, value, decoded)
}

func TestOffloadCodecDefaultsThreshold(t *testing.T) {
	t.Cleanup(func() { ConfigurePayloadCodecs() })

	store := &memOffloadStore{}
	codec, err := NewOffloadCodec(store, OffloadCodecOptions{})
	require.NoError(t, err)
	ConfigurePayloadCodecs(codec)

	dc := dataConverter()
	small := []byte("small")
	large := bytes.Repeat([]byte("b"), defaultOffloadThresholdBytes+1024)

	smallPayload, err := dc.ToPayload(small)
	require.NoError(t, err)
	require.NotEqual(t, metadataEncodingOffload, string(smallPayload.Metadata[converter.MetadataEncoding]))

	largePayload, err := dc.ToPayload(large)
	require.NoError(t, err)
	require.Equal(t, metadataEncodingOffload, string(largePayload.Metadata[converter.MetadataEncoding]))
	require.Equal(t, 1, store.puts)
}

func TestOffloadCodecRejectsNegativeThreshold(t *testing.T) {
	store := &memOffloadStore{}
	_, err := NewOffloadCodec(store, OffloadCodecOptions{ThresholdBytes: -1})
	require.Error(t, err)
}

func TestOffloadCodecMissingKey(t *testing.T) {
	codec, err := NewOffloadCodec(&memOffloadStore{}, OffloadCodecOptions{ThresholdBytes: 1})
	require.NoError(t, err)

	payload := &commonpb.Payload{
		Metadata: map[string][]byte{
			converter.MetadataEncoding: []byte(metadataEncodingOffload),
		},
	}
	_, err = codec.Decode([]*commonpb.Payload{payload})
	require.Error(t, err)
}

func TestOffloadCodecWrapsEncryptedPayloads(t *testing.T) {
	t.Cleanup(func() { ConfigurePayloadCodecs() })

	key := bytes.Repeat([]byte{0xAB}, 32)
	enc, err := NewEncryptionCodec(key)
	require.NoError(t, err)

	store := &memOffloadStore{}
	offload, err := NewOffloadCodec(store, OffloadCodecOptions{ThresholdBytes: 1})
	require.NoError(t, err)
	ConfigurePayloadCodecs(offload, enc)

	dc := dataConverter()
	value := bytes.Repeat([]byte("z"), 1024)

	payload, err := dc.ToPayload(value)
	require.NoError(t, err)
	require.Equal(t, metadataEncodingOffload, string(payload.Metadata[converter.MetadataEncoding]))

	keyID := string(payload.Metadata[metadataOffloadKey])
	require.NotEmpty(t, keyID)
	raw := store.getRaw(keyID)
	require.NotEmpty(t, raw)

	inner := &commonpb.Payload{}
	require.NoError(t, proto.Unmarshal(raw, inner))
	require.Equal(t, metadataEncodingEncrypted, string(inner.Metadata[converter.MetadataEncoding]))
}

func TestOffloadCodecDeterministicKeys(t *testing.T) {
	t.Cleanup(func() { ConfigurePayloadCodecs() })

	store := &memOffloadStore{}
	codec, err := NewOffloadCodec(store, OffloadCodecOptions{ThresholdBytes: 1})
	require.NoError(t, err)
	ConfigurePayloadCodecs(codec)

	dc := dataConverter()
	value := bytes.Repeat([]byte("k"), 1024)

	first, err := dc.ToPayload(value)
	require.NoError(t, err)
	second, err := dc.ToPayload(value)
	require.NoError(t, err)

	firstKey := string(first.Metadata[metadataOffloadKey])
	secondKey := string(second.Metadata[metadataOffloadKey])
	require.NotEmpty(t, firstKey)
	require.Equal(t, firstKey, secondKey)
	keys := store.keys()
	require.NotEmpty(t, keys)
	for _, key := range keys {
		require.Equal(t, firstKey, key)
	}
}
