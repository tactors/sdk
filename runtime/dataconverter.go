package runtime

import (
	"fmt"

	"github.com/tactors/sdk/internal/codec"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const metadataEncodingCBOR = "binary/cbor"

var defaultDataConverter = converter.NewCompositeDataConverter(
	converter.NewNilPayloadConverter(),
	converter.NewByteSlicePayloadConverter(),
	converter.NewProtoJSONPayloadConverter(),
	converter.NewProtoPayloadConverter(),
	cborPayloadConverter{},
)

// dataConverter exposes the CBOR-backed Temporal data converter used by the runtime.
func dataConverter() converter.DataConverter {
	return defaultDataConverter
}

// DataConverter exposes the runtime's CBOR-backed data converter so external clients
// (e.g., HTTP gateways) can encode/decode payloads consistently with actors.
func DataConverter() converter.DataConverter {
	return dataConverter()
}

type cborPayloadConverter struct{}

func (cborPayloadConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	data, err := codec.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", converter.ErrUnableToEncode, err)
	}
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			converter.MetadataEncoding: []byte(metadataEncodingCBOR),
		},
		Data: data,
	}, nil
}

func (cborPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	if err := codec.Unmarshal(payload.GetData(), valuePtr); err != nil {
		return fmt.Errorf("%w: %v", converter.ErrUnableToDecode, err)
	}
	return nil
}

func (cborPayloadConverter) ToString(payload *commonpb.Payload) string {
	return fmt.Sprintf("cbor[%d bytes]", len(payload.GetData()))
}

func (cborPayloadConverter) Encoding() string {
	return metadataEncodingCBOR
}
