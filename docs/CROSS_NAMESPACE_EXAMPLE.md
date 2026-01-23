# Cross-Namespace Example (Opt-In)

This example shows the minimal wiring for cross-namespace routing plus a remote spawn.

## Worker bootstrap

```go
accountsClient, _ := client.Dial(client.Options{
    Namespace:    "accounts",
    DataConverter: runtime.DataConverter(),
})
cardsClient, _ := client.Dial(client.Options{
    Namespace:    "cards",
    DataConverter: runtime.DataConverter(),
})

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

set.Register(actors.BuildAccountActor(), runtime.WorkerConfig{})
set.Register(actors.BuildCardActor(), runtime.WorkerConfig{})
_ = set.StartAll(ctx)
```

## Cross-namespace Ask/Tell

```go
card := actors.ARef("Card", cardID) // no explicit namespace; resolver maps "Card" -> "cards"
resp, err := actors.Ask[IssueCardCommand, IssueCardResponse](ctx, card, IssueCardCommand{...})
```

## Remote spawn (explicit)

```go
ref, err := actors.SpawnRemote(ctx, "Card", initPayload, actors.WithSpawnNamespace("cards"))
```

Notes:
- Cross-namespace calls run through a bridge activity in the caller namespace.
- Remote spawn is not a child workflow; it is an external start with idempotent WorkflowID.
