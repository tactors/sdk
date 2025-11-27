# Activity flow pattern: actor as the graph

Tactors treats the workflow (actor) as the graph. Activities are leaf nodes; they never know or care about “the next step.” Use this pattern to wire multi-step flows:

1) **State as shared memory (“pull”)**

```go
type OnboardingState struct {
    User    User
    Payment PaymentResult
}
```

Command handlers pull from this state; each step writes back into it.

2) **Activities are stateless leaf nodes**

```go
type FetchUserActivity struct {
    actors.ActivityMsg[User]
    UserID string
}

type ChargeCardActivity struct {
    actors.ActivityMsg[PaymentResult]
    UserID string
    Amount int64
}
```

Activities do not orchestrate or reference other activities.

3) **Actor orchestrates (“push” at edges, pull from state)**

```go
builder := actors.NewStateful("onboarding", func() OnboardingState { return OnboardingState{} }).
    Command("StartOnboarding", func(ctx actors.Context, s *OnboardingState, cmd StartOnboarding) error {
        user, err := actors.DoActivity[User](ctx, FetchUserActivity{UserID: cmd.UserID})
        if err != nil {
            return err
        }
        s.User = user

        payment, err := actors.DoActivity[PaymentResult](ctx, ChargeCardActivity{
            UserID: s.User.ID,
            Amount: 42_00,
        })
        if err != nil {
            return err
        }
        s.Payment = payment

        cmd.Reply(ctx, OnboardingResult{UserID: s.User.ID, PaymentID: s.Payment.ID})
        return nil
    })
```

- “Push” happens when the actor calls an activity with inputs.
- “Pull” happens when the handler reads prior results from state.
- Activities never call activities; the actor owns ordering and wiring.

## Why this matters

- Determinism: orchestration stays in the workflow; activities stay pure side-effects.
- Reusability: activities can be reused in different DAGs without coupling.
- Evolvability: you can change ordering or insert steps by editing the command handler only.
