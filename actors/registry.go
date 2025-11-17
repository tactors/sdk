package actors

import (
	"sort"
	"strings"
	"sync"
)

// DescriptionRegistry exposes thread-safe registration and lookup helpers for actor descriptions.
type DescriptionRegistry interface {
	RegisterActor(actor Actor)
	RegisterDescription(desc *Description)
	Unregister(kind string)
	Lookup(kind string) *Description
	List() []RegisteredActor
}

// RegisteredActor captures a point-in-time snapshot of a registered actor.
type RegisteredActor struct {
	Kind        string
	Description *Description
	Metadata    ActorMetadata
}

// NewRegistry returns an in-memory implementation of DescriptionRegistry.
func NewRegistry() DescriptionRegistry {
	return newDescriptionRegistry()
}

type descriptionRegistry struct {
	mu      sync.RWMutex
	entries map[string]*Description
}

func newDescriptionRegistry() *descriptionRegistry {
	return &descriptionRegistry{
		entries: make(map[string]*Description),
	}
}

func (r *descriptionRegistry) RegisterActor(actor Actor) {
	if actor == nil {
		return
	}
	r.RegisterDescription(actor.Spec())
}

func (r *descriptionRegistry) RegisterDescription(desc *Description) {
	if desc == nil {
		return
	}
	clone := desc.Clone()
	r.mu.Lock()
	r.entries[clone.Kind] = clone
	r.mu.Unlock()
}

func (r *descriptionRegistry) Unregister(kind string) {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return
	}
	r.mu.Lock()
	delete(r.entries, kind)
	r.mu.Unlock()
}

func (r *descriptionRegistry) Lookup(kind string) *Description {
	if kind == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if desc, ok := r.entries[kind]; ok {
		return desc.Clone()
	}
	return nil
}

func (r *descriptionRegistry) List() []RegisteredActor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return nil
	}
	kinds := make([]string, 0, len(r.entries))
	for kind := range r.entries {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	out := make([]RegisteredActor, 0, len(kinds))
	for _, kind := range kinds {
		desc := r.entries[kind]
		out = append(out, RegisteredActor{
			Kind:        kind,
			Description: desc.Clone(),
			Metadata:    desc.Metadata(),
		})
	}
	return out
}

var (
	defaultRegistryMu sync.RWMutex
	defaultRegistry   DescriptionRegistry = newDescriptionRegistry()
)

// DefaultRegistry returns the process-wide registry used by runtime helpers.
func DefaultRegistry() DescriptionRegistry {
	defaultRegistryMu.RLock()
	defer defaultRegistryMu.RUnlock()
	return defaultRegistry
}

// SetDefaultRegistry replaces the process-wide registry reference.
func SetDefaultRegistry(reg DescriptionRegistry) {
	defaultRegistryMu.Lock()
	defer defaultRegistryMu.Unlock()
	if reg == nil {
		defaultRegistry = newDescriptionRegistry()
		return
	}
	defaultRegistry = reg
}

// RegisterActorDescription registers the provided actor with the default registry.
func RegisterActorDescription(actor Actor) {
	DefaultRegistry().RegisterActor(actor)
}

// RegisterDescription stores a description in the default registry.
func RegisterDescription(desc *Description) {
	DefaultRegistry().RegisterDescription(desc)
}

// UnregisterKind removes a kind from the default registry.
func UnregisterKind(kind string) {
	DefaultRegistry().Unregister(kind)
}

// LookupDescription fetches a description by kind from the default registry.
func LookupDescription(kind string) *Description {
	return DefaultRegistry().Lookup(kind)
}

// RegisteredActors returns sorted snapshots of every registered kind.
func RegisteredActors() []RegisteredActor {
	return DefaultRegistry().List()
}
