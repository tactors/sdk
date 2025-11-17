package codec

import (
	"sync"

	"github.com/fxamacker/cbor/v2"
)

var (
	cborOnce       sync.Once
	defaultEncMode cbor.EncMode
	defaultDecMode cbor.DecMode
	cborInitErr    error
)

func initModes() {
	cborOnce.Do(func() {
		encOptions := cbor.CoreDetEncOptions()
		defaultEncMode, cborInitErr = encOptions.EncMode()
		if cborInitErr != nil {
			return
		}
		decOptions := cbor.DecOptions{}
		decOptions.IndefLength = cbor.IndefLengthAllowed
		defaultDecMode, cborInitErr = decOptions.DecMode()
	})
}

// Marshal encodes the provided value using deterministic CBOR.
func Marshal(v any) ([]byte, error) {
	initModes()
	if cborInitErr != nil {
		return nil, cborInitErr
	}
	return defaultEncMode.Marshal(v)
}

// Unmarshal decodes the CBOR payload into the provided destination.
func Unmarshal(b []byte, v any) error {
	initModes()
	if cborInitErr != nil {
		return cborInitErr
	}
	return defaultDecMode.Unmarshal(b, v)
}
