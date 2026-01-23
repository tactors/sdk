package runtime

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tactors/sdk/actors"
)

// ClientPool resolves Temporal clients per namespace.
type ClientPool interface {
	DefaultNamespace() string
	Client(namespace string) (TemporalClient, error)
}

// StaticClientPool maps namespaces to pre-dialed clients.
type StaticClientPool struct {
	Default string
	Clients map[string]TemporalClient
}

func (s StaticClientPool) DefaultNamespace() string {
	return strings.TrimSpace(s.Default)
}

func (s StaticClientPool) Client(namespace string) (TemporalClient, error) {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = s.DefaultNamespace()
	}
	if ns == "" {
		return nil, errors.New("runtime: namespace is empty")
	}
	if s.Clients == nil {
		return nil, fmt.Errorf("runtime: no client configured for namespace %q", ns)
	}
	client := s.Clients[ns]
	if client == nil {
		return nil, fmt.Errorf("runtime: no client configured for namespace %q", ns)
	}
	return client, nil
}

// NamespaceResolver selects a namespace for a target reference.
type NamespaceResolver interface {
	Resolve(ref actors.Ref) string
}

// KindNamespaceMap maps actor kinds to Temporal namespaces.
type KindNamespaceMap map[string]string

func (m KindNamespaceMap) Resolve(ref actors.Ref) string {
	if m == nil {
		return ""
	}
	return strings.TrimSpace(m[ref.Kind])
}

// CrossNamespacePolicy controls cross-namespace routing.
type CrossNamespacePolicy struct {
	Enabled   bool
	Allowlist map[string]map[string]struct{}
}

func (p CrossNamespacePolicy) Allows(callerNS, targetNS string) bool {
	if callerNS == targetNS {
		return true
	}
	if !p.Enabled {
		return false
	}
	if len(p.Allowlist) == 0 {
		return true
	}
	if targets, ok := p.Allowlist[callerNS]; ok {
		if _, allowed := targets[targetNS]; allowed {
			return true
		}
	}
	if targets, ok := p.Allowlist["*"]; ok {
		if _, allowed := targets[targetNS]; allowed {
			return true
		}
	}
	return false
}

// NormalizeAllowlist returns a defensive copy of an allowlist map.
func NormalizeAllowlist(input map[string][]string) map[string]map[string]struct{} {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]map[string]struct{}, len(input))
	for caller, targets := range input {
		caller = strings.TrimSpace(caller)
		if caller == "" {
			continue
		}
		if len(targets) == 0 {
			continue
		}
		set := make(map[string]struct{}, len(targets))
		for _, target := range targets {
			target = strings.TrimSpace(target)
			if target == "" {
				continue
			}
			set[target] = struct{}{}
		}
		if len(set) > 0 {
			out[caller] = set
		}
	}
	return out
}

var (
	ErrCrossNamespaceDisabled = errors.New("runtime: cross-namespace calls are disabled")
	ErrCrossNamespaceDenied   = errors.New("runtime: cross-namespace call not permitted")
)

type namespaceRouting struct {
	pool     ClientPool
	resolver NamespaceResolver
	policy   CrossNamespacePolicy
}

func (r *namespaceRouting) defaultNamespace() string {
	if r == nil || r.pool == nil {
		return ""
	}
	return strings.TrimSpace(r.pool.DefaultNamespace())
}

func (r *namespaceRouting) effectiveNamespace(ns string) string {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		return r.defaultNamespace()
	}
	return ns
}

func (r *namespaceRouting) resolveNamespace(ref actors.Ref) string {
	if r == nil {
		return strings.TrimSpace(ref.Namespace)
	}
	if ns := strings.TrimSpace(ref.Namespace); ns != "" {
		return ns
	}
	if r.resolver != nil {
		if ns := strings.TrimSpace(r.resolver.Resolve(ref)); ns != "" {
			return ns
		}
	}
	return r.defaultNamespace()
}

func (r *namespaceRouting) canCrossNamespace(callerNS, targetNS string) error {
	if r == nil {
		return ErrCrossNamespaceDisabled
	}
	if callerNS == targetNS {
		return nil
	}
	if r.pool == nil || !r.policy.Enabled {
		return ErrCrossNamespaceDisabled
	}
	if !r.policy.Allows(callerNS, targetNS) {
		return ErrCrossNamespaceDenied
	}
	return nil
}

func (r *namespaceRouting) client(namespace string) (TemporalClient, error) {
	if r == nil || r.pool == nil {
		return nil, ErrCrossNamespaceDisabled
	}
	return r.pool.Client(namespace)
}

func (r *namespaceRouting) bridgeEnabled() bool {
	return r != nil && r.pool != nil && r.policy.Enabled
}
