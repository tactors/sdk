# Ticketing Actor (vNext)

This example models a support ticket lifecycle (open, add issues, assign agent, close). It relies on
`actors.CommandMsg`/`QueryMsg` for typed commands and the testsuite-backed `testkit` scenario for
invocation.

## Running the sample

```bash
cd examples/ticketing
go test ./...
```

The test harness spins up the actor inside the Temporal testsuite, issues commands, and asserts on
the resulting state. Use the same pattern for your own actors before wiring them into the Temporal
runtime.
