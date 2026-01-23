# Cross-Namespace Actors (Temporal Namespaces)

This document explains the opt-in, production-ready way to address actors across
multiple Temporal namespaces while preserving the single-namespace happy path.

## Why this exists

Temporal namespaces are an isolation boundary. Tactors normally assumes a single
namespace per process, which keeps semantics simple and predictable. Some teams
separate domains into namespaces (for example: accounts vs cards) and still need
cross-domain calls. This feature makes that safe and explicit.

## Key ideas

- Namespace becomes part of actor identity.
- Namespace selection is resolved consistently and can be centralized.
- Cross-namespace calls are executed via an activity (bridge) in the caller
  namespace.
- Cross-namespace spawn is explicit and not a child workflow.
- Policy controls are required to enable cross-namespace routing.

## Namespace as part of actor identity

`actors.Ref` carries an optional namespace. When empty, the runtime treats it as
"use the default namespace".

```go
type Ref struct {
    Namespace string
    // existing fields...
}

// Explicit override
actors.ARef("Card", "card-123", actors.WithNamespace("cards"))
```

This is distinct from `Tenant`, which is an application-level partition inside a
single namespace.

## Namespace resolution rules

Namespace selection follows a strict order:

1) `ref.Namespace` if set
2) resolver mapping (`NamespaceResolver`)
3) pool default namespace

Before comparing namespaces for "same vs different", empty values are normalized
to the pool default. This prevents accidental cross-namespace routing when one
side uses the empty default.

## Client pool + resolver configuration

You must opt in via a client pool and policy. The pool is the single source of
truth for how to reach each namespace. A resolver keeps call sites ergonomic.

```go
pool := runtime.StaticClientPool{
    Default: "accounts",
    Clients: map[string]runtime.TemporalClient{
        "accounts": accountsClient,
        "cards":    cardsClient,
    },
}
resolver := runtime.KindNamespaceMap{
    "Account": "accounts",
    "Card":    "cards",
}
policy := runtime.CrossNamespacePolicy{Enabled: true}

set := runtime.NewWorkerSet(accountsClient).
    Configure(
        runtime.WithClientPool(pool),
        runtime.WithNamespaceResolver(resolver),
        runtime.WithCrossNamespacePolicy(policy),
    )
```

## Cross-namespace routing behavior

- Same namespace: use the existing fast path (signals/queries/updates).
- Different namespace: schedule a bridge activity in the caller namespace, which
  invokes the target namespace client.

Bridge activities execute in the caller namespace and call into the target
namespace. This makes the boundary visible in histories, retries, and metrics.

## Spawn semantics

Child workflows cannot cross namespaces. Use `SpawnRemote` for explicit remote
starts.

```go
ref, err := actors.SpawnRemote(ctx, "Card", initPayload,
    actors.WithSpawnNamespace("cards"))
```

`SpawnRemote` is idempotent by workflow ID; if the workflow already exists, the
call succeeds without creating a second run.

## Guardrails and policy

Cross-namespace routing is disabled by default. Enable it explicitly and apply
policy controls:

- `CrossNamespacePolicy.Enabled` must be true for any cross-namespace routing.
- Optional allowlists limit which namespaces can call which targets.

This prevents accidental fan-out across all configured namespaces.

## Cancellation and idempotency

If the caller cancels, the bridge activity is cancelled. The remote workflow may
already have received a signal or update. Commands and asks should be idempotent
and keyed by `CorrelationID` where possible.

## Observability

Cross-namespace calls include these fields:

- `actor.namespace`
- `actor.target_namespace`
- `actor.cross_namespace`

Use them to distinguish local calls from bridge hops.

## API Quick Reference

```go
// Configure worker routing
set := runtime.NewWorkerSet(defaultClient).
    Configure(
        runtime.WithClientPool(pool),
        runtime.WithNamespaceResolver(resolver),
        runtime.WithCrossNamespacePolicy(policy),
    )

// Cross-namespace calls (from inside a workflow)
ref := actors.ARef("Card", "card-123") // resolver fills namespace
_, _ = actors.Ask[IssueCardCommand, IssueCardResponse](ctx, ref, IssueCardCommand{...})

// Cross-namespace spawn (explicit)
_, _ = actors.SpawnRemote(ctx, "Card", initPayload, actors.WithSpawnNamespace("cards"))
```

## Minimal wiring example

See `docs/CROSS_NAMESPACE_EXAMPLE.md` for a compact end-to-end snippet.
