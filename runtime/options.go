package runtime

import (
	"sync/atomic"
	"time"
)

type AskRoutingMode int32

const (
	AskRouteSignal AskRoutingMode = iota
	AskRouteUpdate
)

var (
	askTimeoutNanos   atomic.Int64
	queryTimeoutNanos atomic.Int64
	askRouteMode      atomic.Int32
)

func init() {
	askTimeoutNanos.Store(int64(defaultAskWaitTimeout))
	queryTimeoutNanos.Store(int64(defaultQueryTimeout))
	askRouteMode.Store(int32(AskRouteSignal))
}

// SetDefaultAskTimeout overrides the timeout used by actor-to-actor asks. Set to zero to disable.
func SetDefaultAskTimeout(d time.Duration) {
	askTimeoutNanos.Store(int64(d))
}

// SetDefaultQueryTimeout overrides the timeout used by actor-to-actor queries. Set to zero to disable.
func SetDefaultQueryTimeout(d time.Duration) {
	queryTimeoutNanos.Store(int64(d))
}

func askTimeout() time.Duration {
	return time.Duration(askTimeoutNanos.Load())
}

func queryTimeout() time.Duration {
	return time.Duration(queryTimeoutNanos.Load())
}

// SetAskRoutingMode switches between signal-based asks and future update routing.
func SetAskRoutingMode(mode AskRoutingMode) {
	askRouteMode.Store(int32(mode))
}

func currentAskRoutingMode() AskRoutingMode {
	return AskRoutingMode(askRouteMode.Load())
}
