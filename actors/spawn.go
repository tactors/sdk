package actors

import (
	"errors"
	"strings"
	"time"
)

var ErrUnsupported = errors.New("actors: unsupported operation")

// SpawnConfig captures optional settings for child workflows.
type SpawnConfig struct {
	Kind      string
	Name      string
	Timeout   time.Duration
	TaskQueue string
	Namespace string
}

// SpawnOption customizes spawn behavior.
type SpawnOption func(*SpawnConfig)

func WithChildName(name string) SpawnOption {
	return func(cfg *SpawnConfig) {
		cfg.Name = name
	}
}

func WithChildTimeout(d time.Duration) SpawnOption {
	return func(cfg *SpawnConfig) {
		cfg.Timeout = d
	}
}

// WithChildKind specifies the actor kind to target when spawning children.
func WithChildKind(kind string) SpawnOption {
	return func(cfg *SpawnConfig) {
		cfg.Kind = kind
	}
}

// WithChildTaskQueue routes the child workflow to a specific task queue.
func WithChildTaskQueue(name string) SpawnOption {
	return func(cfg *SpawnConfig) {
		cfg.TaskQueue = strings.TrimSpace(name)
	}
}

// WithSpawnNamespace targets a Temporal namespace for remote spawns.
func WithSpawnNamespace(name string) SpawnOption {
	return func(cfg *SpawnConfig) {
		cfg.Namespace = strings.TrimSpace(name)
	}
}
