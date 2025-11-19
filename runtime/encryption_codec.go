package runtime

import (
	"crypto/aes"
	"crypto/cipher"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const (
	metadataEncodingEncrypted = "binary/aes-gcm"
	metadataEncryptionNonce   = "encryption/aes-gcm-nonce"
)

type encryptionCodec struct {
	aead cipher.AEAD
	rand io.Reader
}

// NewEncryptionCodec creates a Temporal payload codec that wraps payloads in AES-GCM ciphertext.
func NewEncryptionCodec(key []byte) (converter.PayloadCodec, error) {
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("runtime: encryption key must be 16, 24, or 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &encryptionCodec{
		aead: aead,
		rand: crand.Reader,
	}, nil
}

func (c *encryptionCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		raw, err := proto.Marshal(payload)
		if err != nil {
			return payloads, err
		}
		nonce := make([]byte, c.aead.NonceSize())
		if _, err := io.ReadFull(c.rand, nonce); err != nil {
			return payloads, err
		}
		result[i] = &commonpb.Payload{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(metadataEncodingEncrypted),
				metadataEncryptionNonce:    nonce,
			},
			Data: c.aead.Seal(nil, nonce, raw, nil),
		}
	}
	return result, nil
}

func (c *encryptionCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		if string(payload.Metadata[converter.MetadataEncoding]) != metadataEncodingEncrypted {
			result[i] = payload
			continue
		}
		nonce := payload.Metadata[metadataEncryptionNonce]
		if len(nonce) != c.aead.NonceSize() {
			return payloads, errors.New("runtime: encrypted payload missing nonce")
		}
		raw, err := c.aead.Open(nil, nonce, payload.GetData(), nil)
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
