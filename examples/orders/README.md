# Orders Actor (vNext)

This directory reimplements the `examples/orders` workflow using the typed builder and the
testsuite-backed `testkit`. It is not a feature-complete port of the original example, but it mirrors
the command/query surface so future migration is straightforward.

## Commands & Queries

- `StartOrderCommand` sets up the order with an ID and customer.
- `AddItemCommand` appends items.
- `SubmitOrderCommand` marks the order as submitted and records the timestamp.
- `OrderStatusQuery` returns the state snapshot.

All commands embed `actors.CommandMsg[Response]`, and the query embeds `actors.QueryMsg[Response]`,
so other actors (or tests) can rely on typed `Ask`/`QueryActor` helpers.

## Runtime usage

Run the deterministic Temporal scenario:

```bash
cd examples/orders
go test .
```

This spins up the actor inside the Temporal testsuite, issues the three commands, and asserts on the
final status.
