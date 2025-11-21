# Contributing

Thanks for diving in! This page captures the basic workflow plus the conventions we are
following while the SDK stabilizes.

## Development workflow

1. Install Go 1.23+.
2. (Optional) Install the Temporal CLI if you plan to inspect real histories, though the tests here do
   not require it.
3. Format Go files you modify: `gofmt -w <files>`.
4. Run the full suite locally before sending a PR:
   ```bash
   GOCACHE=$(pwd)/.gocache go test ./...
   ```
5. Open pull requests against `github.com/tactors/sdk`.

## Coding guidelines

- Keep APIs typed and fluent—reach for builders instead of reflection (beyond the `%T` formatting we
  already use for type names).
- Surface runtime features via composition (`WithSnapshot`, `actors.Patch`, typed error helpers)
  rather than hidden magic.
- Temporal runtime code lives under `runtime`, keeping the shared builder, testkit, and
  typed clients clean even though Temporal is the only supported runtime.
- Tests should be deterministic. Lean on the Temporal testsuite-backed testkit for workflow coverage.

## Ideas on the roadmap

- Payload codecs / encryption hooks
- Local activity helpers (timeouts, heartbeats)
- Structured concurrency helpers for fan-out/fan-in
- Worker versioning + build-ID rollout helpers
- Replay tooling and recorded-history CI flows

Have an idea that is missing here? Open an issue or start a discussion in your PR description.
