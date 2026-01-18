package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const (
	metadataEncodingOffload      = "binary/offload"
	metadataOffloadKey           = "offload/key"
	defaultOffloadThresholdBytes = 256 * 1024
)

// OffloadStore stores and fetches raw Temporal payload bytes for offloading.
type OffloadStore interface {
	Put(key string, payload []byte) error
	Get(key string) ([]byte, error)
}

// OffloadKeyFunc derives a deterministic key for a payload.
type OffloadKeyFunc func(payload []byte) (string, error)

// OffloadCodecOptions configures payload offloading behavior.
type OffloadCodecOptions struct {
	ThresholdBytes int
	KeyFunc        OffloadKeyFunc
}

type offloadCodec struct {
	store     OffloadStore
	threshold int
	keyFunc   OffloadKeyFunc
}

// NewOffloadCodec creates a Temporal payload codec that stores large payloads externally.
// ThresholdBytes defaults to 256 KiB when zero. KeyFunc defaults to SHA-256 of the payload bytes.
func NewOffloadCodec(store OffloadStore, opts OffloadCodecOptions) (converter.PayloadCodec, error) {
	if store == nil {
		return nil, errors.New("runtime: offload store is nil")
	}
	threshold := opts.ThresholdBytes
	if threshold == 0 {
		threshold = defaultOffloadThresholdBytes
	}
	if threshold < 0 {
		return nil, fmt.Errorf("runtime: offload threshold must be >= 0, got %d", threshold)
	}
	keyFunc := opts.KeyFunc
	if keyFunc == nil {
		keyFunc = defaultOffloadKey
	}
	return &offloadCodec{
		store:     store,
		threshold: threshold,
		keyFunc:   keyFunc,
	}, nil
}

func (c *offloadCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		if payload == nil {
			result[i] = payload
			continue
		}
		if string(payload.Metadata[converter.MetadataEncoding]) == metadataEncodingOffload {
			result[i] = payload
			continue
		}
		raw, err := proto.Marshal(payload)
		if err != nil {
			return payloads, err
		}
		if c.threshold > 0 && len(raw) < c.threshold {
			result[i] = payload
			continue
		}
		key, err := c.keyFunc(raw)
		if err != nil {
			return payloads, err
		}
		if key == "" {
			return payloads, errors.New("runtime: offload key is empty")
		}
		if err := c.store.Put(key, raw); err != nil {
			return payloads, err
		}
		result[i] = &commonpb.Payload{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(metadataEncodingOffload),
				metadataOffloadKey:         []byte(key),
			},
		}
	}
	return result, nil
}

func defaultOffloadKey(payload []byte) (string, error) {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (c *offloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		if payload == nil {
			result[i] = payload
			continue
		}
		if string(payload.Metadata[converter.MetadataEncoding]) != metadataEncodingOffload {
			result[i] = payload
			continue
		}
		key := string(payload.Metadata[metadataOffloadKey])
		if key == "" {
			return payloads, errors.New("runtime: offloaded payload missing key")
		}
		raw, err := c.store.Get(key)
		if err != nil {
			return payloads, err
		}
		out := &commonpb.Payload{}
		if err := proto.Unmarshal(raw, out); err != nil {
			return payloads, err
		}
		result[i] = out
	}
	return result, nil
}
