package rand

import (
	crand "crypto/rand"
	"fmt"
)

var randomRead = crand.Read

// ID returns a random identifier with the provided prefix.
func ID(prefix string) string {
	buf := make([]byte, 8)
	if _, err := randomRead(buf); err != nil {
		return prefix
	}
	return fmt.Sprintf("%s-%x", prefix, buf)
}
