# go-redismq

Redis Streams-backed message queue library. Go 1.25, module `github.com/Orfeo42/go-redismq/v3`, package `redismq`. Dependency-free beyond `github.com/redis/go-redis/v9` and `github.com/stretchr/testify` (test-only).

## Critical Workflow Rules

- Do not modify `.go` files without running `just build` and `just test` afterward.
- Do not commit, push, or open PRs unless explicitly asked.
- Never use destructive git commands (stash, checkout, reset, clean, rebase, merge, branch switching) unless explicitly asked.
- Read every file before editing it.

## Repository Intelligence

- Before searching source by hand, grep `.ai-docs/symbols.tags` for "where is X defined" (ctags output: symbol, file, line, signature) and check `.ai-docs/structure.txt` for the directory layout.
- Both files are generated locally and NOT tracked in git (`.ai-docs/` is in `.gitignore`). If missing or stale, regenerate with `just ai-update` — do this after adding/moving packages or changing public symbols.

## Architecture

- Root package `redismq` is a façade: 7 files, 463 lines total (`doc.go` 39, `types.go` 43, `errors.go` 45, `client.go` 173, `options.go` 87, `logger.go` 56, `config.go` 20). Declarations only — type aliases, option constructors, the `Client` struct that wires dependencies in `New()`, and thin delegating methods. No business logic lives at root; every method on `*Client` (`Send`, `SendTransaction`, `SendDelay`, `Invoke`, `RegisterInvoke`, `RegisterListener`, `RegisterChecker`) is a one-line delegation into an `internal/` engine.
- All logic lives under `internal/`, in an acyclic layered graph:
  - **Layer 0** (no imports of any other internal package): `mqtype` (core types: `Message`, `Action`, `TransactionStatus`, `MQTopicEnum`, listener/checker interfaces), `logging` (the `Logger`/`AttrLogger` adapter framework), `logattr` (imports `mqtype` + `streamname`; shared `slog.Attr` builders), `streamname` (stream/queue key naming), `idgen` (ID generation), `jsonutil` (JSON marshal helpers).
  - **Layer 1**: `traceio` (imports `mqtype`; trace-ID stamping/extraction), `registry` (imports `logging`, `mqtype`, `streamname`; listener/checker registration and lookup). Neither is imported by any Layer 2 engine directly — root wires concrete Layer 1 instances into Layer 2 engines through each engine's own narrow interface (see next point).
  - **Layer 2** (the engines — `producer`, `delayqueue`, `invoke`, `consumer`, `txchecker`): each imports only Layer 0 packages (`logattr`, `logging`, `mqtype`, plus `streamname`/`idgen`/`jsonutil` where needed). **They never import each other, and never import `traceio` or `registry` directly.**
  - **Layer 3**: root `redismq`, which imports everything and performs all wiring in `client.go`'s `New()`.
- Verified with a direct import grep per package — the graph above is exact, not approximate.

## Two design decisions a newcomer will otherwise "fix" and break

1. **Type aliases in root, not new types.** `types.go` does `type Message = mqtype.Message` (and likewise for `Action`, `TransactionStatus`, `MQTopicEnum`, `IMessageListener`, `IMessageChecker`, `InvokeRequest = invoke.Request`, `InvokeResponse = invoke.Response`). Go forbids an exported root-package function from taking/returning a type from an `internal/` package it doesn't own the alias for — external hosts could not spell `internal/mqtype.Message`. The `=` alias makes `redismq.Message` and `mqtype.Message` the identical type, so every `internal/` function signature that mentions `*mqtype.Message` is directly usable at the root API. Turning any of these into a new defined type (`type Message mqtype.Message` or a hand-written struct) breaks every host importing `redismq.Message` — and it still compiles inside this module (internal call sites don't care), so `go build ./...` here will NOT catch the break; only downstream consumers would.
2. **Consumer-side interfaces break the Layer 2 import cycle.** Each engine declares the narrow interface(s) IT needs, not a shared one:
   - `internal/consumer.DelayScheduler` (`ScheduleDelay`) — satisfied structurally by `*delayqueue.Queue`.
   - `internal/delayqueue.Publisher` and `internal/invoke.Publisher` — each package declares its OWN `Publisher` interface (`Publish(ctx, *mqtype.Message) (bool, error)`), both satisfied structurally by `*producer.Producer`. Two identical-shaped interfaces, declared twice, deliberately — not shared, because sharing would require a common package either engine imports.
   - `internal/txchecker.TransactionCompleter` (`Commit`, `Rollback`) — satisfied structurally by `*producer.Producer`.
   - `internal/consumer.TransactionChecker` (`Run(ctx, topic)`) — satisfied structurally by `*txchecker.Checker`.
   - None of these engines import each other's packages to declare these interfaces; `client.go`'s `New()` is the only place concrete types from different engines meet, passed positionally into each engine's constructor.
   - Every engine also declares its own narrow `Redis` interface (2-7 methods — e.g. `producer.Redis` has 2, `consumer.Redis` has 7) instead of depending on `redis.Cmdable`. That narrowness is what makes each engine mockable with a small generated mock (`//go:generate moq -out redis_mock.go . Redis`) instead of a mock implementing the entire go-redis client surface.

## The caller-skip invariant — the most dangerous thing to get wrong in this repo

- `internal/logging/logging.go:22` defines `const DirectCallerSkip = 2`. `(*Adapter).LogAttrs` (`internal/logging/adapter.go`) and `(*Bridge).LogAttrs` (`internal/logging/bridge.go`) both call `runtime.Callers(DirectCallerSkip, pcs[:])` to capture the `source` field (file:line) that ends up in the log record.
- This constant assumes every package calls the injected logger's `LogAttrs` method DIRECTLY at the logging call site — e.g. `c.log.LogAttrs(ctx, slog.LevelWarn, "...", attrs...)` as seen throughout `internal/consumer`, `internal/producer`, `internal/delayqueue`, `internal/invoke`, `internal/txchecker`.
- If any package added a local wrapper function (e.g. a package-private `logAttrs(ctx, level, msg, attrs...)` helper that itself calls `c.log.LogAttrs(...)`), that adds one stack frame between the real call site and the skip-count, and EVERY `source` field logged from that package would silently point at the wrapper's line instead of the actual call site. This would not fail any test unless a pin test for that package's `source` field exists and is kept passing — check for `source`-field assertions in `*_test.go` files (e.g. `internal/logging/adapter_test.go`, `internal/logging/bridge_test.go`) before adding any such wrapper, and do not introduce one without updating/adding the pin test.

## `guard_test.go` invariants (root, enforced repo-wide including `internal/`)

Four `testing.T` checks over every non-generated, non-`_test.go` `.go` file under the module root (walk skips `.git`):

- `TestNoPrintfLogging` — no `logger.Debugf(`, `logger.Infof(`, `logger.Warnf(`, `logger.Errorf(` outside `logger.go`. The exemption is matched by `filepath.Base(file) == "logger.go"` (see `isLoggerFallbackFile`, guard_test.go:65-67) — matched by BASENAME, so any file anywhere in the tree named exactly `logger.go` is exempt, not just the root one.
- `TestNoContextBackground` — no `context.Background()` anywhere.
- `TestNoInitFunc` — no `func init()` anywhere.
- `TestNoPanic` — no `panic(` anywhere.
- `.golangci.yml`'s `sloglint` exclusion (`path: "(^|/)logger\\.go$"`, line ~123) matches the same basename pattern as the guard test's exemption. Renaming `logger.go` to anything else would silently make BOTH the printf-logging guard test and the sloglint exclusion stop matching it — the file would then have to conform to structured-logging rules it's deliberately exempt from (it's the fallback path for hosts that only implement the printf-style `Logger` interface, not `AttrLogger`).

## Logging conventions

- Static message strings only — every piece of variable data becomes an `slog.Attr`, never string-interpolated into the message (this is what `TestNoPrintfLogging` and `.golangci.yml`'s `sloglint: static-msg: true` both enforce).
- Attribute keys are `snake_case` (`sloglint: key-naming-case: snake`), e.g. `stream`, `consumer_group`, `message_id`, `reconsume_max`.
- The root-cause error is always keyed `cause` via `logattr.CauseAttr(err)`, never `error` — a host's own boundary log commonly uses `error` as its own field name, and a duplicate JSON key would shadow it. See README.md's logging section for the full attribute-key table.
- Never log at Debug level (no `LevelDebug` call sites exist in `internal/`; `getLogLevelFromEnv()` in `logger.go` maps `LOG_LEVEL=DEBUG` only for hosts that want it, the library itself doesn't emit at that level).
- Logging is always the injected logger's `LogAttrs` called directly at the site (`c.log.LogAttrs(ctx, level, "static message", attrs...)`) — never `fmt.Printf` or the standard `log` package inside library logic (the printf-style `Logger` interface exists only as a host-adapter fallback in `logger.go`/`internal/logging/adapter.go`, consumed through `AttrLogger`).

## Testing

- `testify` (`require`/`assert`) is the assertion convention (`go.mod`: `github.com/stretchr/testify v1.11.1`).
- `moq` generates mocks via `//go:generate moq -out <name>_mock.go . <Interface>:<Mock>` directives, one per engine's narrow interfaces (`internal/consumer`, `internal/delayqueue`, `internal/invoke`, `internal/producer`, `internal/txchecker` each have their own `*_mock.go` files).
- `t.Run` subtests, one per case, no table-driven slices of input/expected structs.
- Tests that need to vary an env var use `t.Setenv` (auto-restoring) — see `options_test.go` for `LOG_LEVEL` cases. This repo has no package-level mutable globals (`doc.go` states state is owned by `*Client`, not globals), so there is no `t.Cleanup`-based global-restore pattern to follow here.
- Two integration test files at root, `send_integration_test.go` and `invoke_integration_test.go`: both skip automatically under `testing.Short()` and also when Redis is unreachable at `127.0.0.1:6379` (`testRedisAddr` constant, `send_integration_test.go:16`).

## Commands (see `justfile` for the authoritative list)

- `just setup-hooks` — one-time: point `core.hooksPath` at `.hooks/`.
- `just validate` — lint (`golangci-lint run`); this is also what `.hooks/pre-commit` runs.
- `just format [dir]` — `golangci-lint run --fix` (auto-fixes what it can).
- `just build [dir]`, `just test [dir]`, `just test-short [dir]` — default `dir=./...`.
- `just lint-sh` — shellcheck over `.hooks/pre-commit`, `.hooks/pre-push`, and the `.scripts/*.sh` scripts including `ai-gen-context.sh`.
- `just lint-nilaway [dir]` — `.scripts/nilaway-check.sh`.
- `just ai-update` — regenerate `.ai-docs/structure.txt` and `.ai-docs/symbols.tags`.

## Known issues (do not be surprised by these, do not silently "fix" them without being asked)

- There is no custom static-analysis tooling in this repo any more. `gocritic` was previously enabled in `.golangci.yml` with a single `ruleguard`-based check (`ruleguard/rules.go`) flagging double-logged errors; it could never run, because `rules.go` imports `github.com/quasilyte/go-ruleguard/dsl` and golangci-lint typechecks the rules file against the ROOT module's dependencies — a nested `ruleguard/go.mod` requiring the DSL is invisible to it, so `golangci-lint run` errored outright instead of reporting issues. Adding the DSL to the root `go.mod` would fix it but puts a lint-only dependency into every consumer's module graph, which this library does not accept. `gocritic`, `ruleguard/`, and the `.tools/droppederr` AST linter (dropped-root-error and `cause`/`error` key-collision checks) were all removed instead. The conventions they enforced — log the root cause once at the boundary, always key it `cause` — are now documented-only, enforced in review.
- Pre-existing discarded errors, left deliberately (not test-coverage gaps, don't "fix" them as drive-by cleanup):
  - `internal/txchecker/txchecker.go:119` and `:127` — `_, _ = c.completer.Rollback(...)` / `Commit(...)` results discarded.
  - `internal/txchecker/txchecker.go:158` — `value, _ := client.Get(ctx, messageId).Result()`.
  - `internal/consumer/consumer.go:244` — `groups, _ := client.XInfoGroups(ctx, queueName).Result()`.
  - `internal/producer/producer.go:225` — `streamMessageId, _ = client.XAdd(ctx, streamAddArgs).Result()` inside a `TxPipelined` closure.
  - `internal/logging/logging.go:75` — `f, _ := fs.Next()`.
- `Message.ReconsumeMax` (`internal/mqtype/message.go:25`) is never written into the serialized stream metadata: `ToStreamAddArgsValues` (message.go:92-122) builds a `messageMetaData` struct (message.go:31-39) that has a `ReconsumeMax` field but never assigns `message.ReconsumeMax` into it before marshaling. So a message's `ReconsumeMax` does not survive a publish→broker→consume round trip; it only has effect within the same process before publish (see `internal/consumer/dispatch.go:180`, `resumeTimesMax := max(40, message.ReconsumeMax)`).
