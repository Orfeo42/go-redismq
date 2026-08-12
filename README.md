# GO-REDISMQ

**go-redismq** is a Go library for implementing distributed message queues using Redis Streams. It supports message production, consumption, delayed delivery, transactions, and method invocation patterns.

## Features

- Message queueing with Redis Streams
- Delayed message delivery
- Transactional message sending and checking
- Method invocation via messages
- Customizable message listeners and checkers
- Constructor-injected `Client` — no package-level mutable state, safe to run multiple independent instances in one process

## Getting Started

### Installation

Add the module to your project:

```
go get github.com/Orfeo42/go-redismq/v3
```

### Basic Usage

```go
package main

import (
    "context"
    "log"
    "log/slog"
    "time"

    "github.com/Orfeo42/go-redismq/v3"
)

type MyListener struct{}

func (l MyListener) GetTopic() string { return "topic" }
func (l MyListener) GetTag() string   { return "tag" }
func (l MyListener) Consume(ctx context.Context, msg *redismq.Message) redismq.Action {
    // handle message
    return redismq.CommitMessage
}

func main() {
    ctx := context.Background()

    client, err := redismq.New(redismq.RedisMqConfig{
        Group:    "YourGroup",
        Addr:     "127.0.0.1:6379",
        Password: "",
        Database: 0,
    }, redismq.WithSlogLogger(slog.Default()))
    if err != nil {
        log.Fatal(err)
    }
    defer func() {
        closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()
        _ = client.Close(closeCtx)
    }()

    if err := client.RegisterListener(ctx, &MyListener{}); err != nil {
        log.Fatal(err)
    }

    if err := client.Start(ctx); err != nil {
        log.Fatal(err)
    }

    // ... application runs ...
}
```

Why the order matters:

1. **`New` builds the `Client` and validates configuration eagerly.** `Group` and `Addr` are required; a blank one returns `ErrConfigGroupBlank` / `ErrConfigAddrBlank` before any Redis connection is attempted (see [Error Handling](#error-handling)).
2. **Everything optional is an `Option`.** Logger, trace-id hooks, and clock all have working zero-value defaults (a no-op logger, identity trace hooks, the system clock) — pass only the ones you need. See [Options](#options).
3. **Listeners and checkers must be registered before `Start`** so their topics are known when the consume loops spin up. `RegisterListener` and `RegisterChecker` now return an `error` instead of logging-and-dropping invalid or duplicate registrations — check it.
4. **`Start(ctx)` owns the root context.** It derives its own cancellable context from `ctx`, wires the internal invoke listener automatically (see below), and returns once the consumer's bootstrap (resolving a consumer name, initializing the death-queue group) succeeds or fails — it does not block for the process lifetime. Every background loop, Redis call, and log line the library emits from that point on derives from that context (see [Context and Graceful Shutdown](#context-and-graceful-shutdown)).
5. **`defer client.Close(ctx)` every `Client` you `Start`.** `Close` cancels the background loops, waits for in-flight message handlers to finish (bounded by the `ctx` you pass it), and only then closes the shared Redis connection.

**`RegisterInternalListeners` is gone.** In the pre-`Client` API this had to be called explicitly after `RegisterListener` and before `StartRedisMqConsumer`, and forgetting it was the single most common upgrade mistake — every `Invoke` call would silently fail to find its handler. `Start` now wires the invoke listener itself as its first step, so the mistake is no longer possible to make.

**Send a message:**

```go
if _, err := client.Send(ctx, &redismq.Message{
    Topic: "topic",
    Tag:   "tag",
    Body:  "Hello, World!",
}); err != nil {
    // handle error
}
```

## Options

`New` takes required config as a plain struct and everything else as functional options:

```go
func New(cfg RedisMqConfig, opts ...Option) (*Client, error)

func WithLogger(l Logger) Option
func WithSlogLogger(l *slog.Logger) Option
func WithStdLogger(l *log.Logger) Option
func WithTraceIDFromContext(fn func(ctx context.Context) string) Option
func WithTraceIDToContext(fn func(ctx context.Context, traceID string) context.Context) Option
func WithClock(c Clock) Option
```

A `nil` argument to any `With*` option is ignored — the zero-value default stays in effect instead of panicking or erroring. `Clock` is `interface{ Now() time.Time }`, useful for deterministic tests; the default is the real system clock.

## Context and Graceful Shutdown

Every entry point takes a `context.Context` — there are no ctx-less variants. The context passed to `Start` reaches every background loop, every Redis call, and every log line the library emits from that `Client`.

```go
ctx, cancel := context.WithCancel(context.Background())

if err := client.Start(ctx); err != nil {
    log.Fatal(err)
}

// ... application runs ...

cancel() // or call client.Close(closeCtx) directly, see below
```

Cancelling the context (or calling `Close`, which cancels it internally) shuts the library down in an orderly way: the consume loops, the delay-queue poller, the trim scheduler, and the invoke keepalive loop all exit on cancellation, and the 60-second blocking stream read (`XReadGroup` with `Block: 60 * time.Second`) unblocks immediately instead of waiting out its timeout. Cancellation is treated as a normal shutdown, not a fault — it does not produce error-level log noise.

`Close(ctx)` is the supported shutdown path and does three things in order:

1. Cancels the context `Start` derived internally.
2. Waits for every background loop **and every in-flight message handler** to finish, bounded by the `ctx` passed to `Close` — if that context is done first, `Close` returns its error (e.g. `context.DeadlineExceeded`) instead of blocking forever.
3. Only once that wait completes cleanly does it close the shared Redis connection. Closing it earlier, while a loop is mid-`XReadGroup`/`XAck`, would surface spurious connection errors during an otherwise-clean shutdown.

Point 2 is a deliberate fix over the pre-`Client` behavior: message handlers used to run as fully detached goroutines that a shutdown could not wait for or account for. They are now tracked the same way every other background loop is, so a bounded `Close` genuinely waits for work in flight instead of abandoning it mid-execution.

```go
closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := client.Close(closeCtx); err != nil {
    // in-flight handlers did not finish within 10s
}
```

`Close` is safe to call even if `Start` was never invoked (it just closes the Redis connection).

## Error Handling

`New` returns an `error` instead of panicking on invalid configuration. The sentinels are plain package vars, comparable with `errors.Is`:

```go
var (
    ErrConfigAddrBlank  = errors.New("redismq: config addr is blank")
    ErrConfigGroupBlank = errors.New("redismq: config group is blank")
)
```

```go
client, err := redismq.New(cfg)
if err != nil {
    switch {
    case errors.Is(err, redismq.ErrConfigAddrBlank):
        // cfg.Addr was empty
    case errors.Is(err, redismq.ErrConfigGroupBlank):
        // cfg.Group was empty
    }
}
```

A `RedisMqConfig` cannot be nil — `New` takes it by value — so there is no equivalent of the old `ErrConfigNil`, and there is no way to end up with a `*Client` whose config was never validated, so there is no equivalent of the old `ErrConfigNotSet` either: a `Client` cannot exist without already-validated config.

`Start` can fail with `ErrConsumerNameUnresolved` if the process has no non-loopback IPv4 interface to derive a consumer identity from.

Registration now fails loudly instead of logging-and-dropping:

| Method | Sentinels |
| --- | --- |
| `RegisterListener` | `ErrNilListener`, `ErrTooManyTopics`, `ErrInvalidTopic`, `ErrDuplicateListener` |
| `RegisterChecker` | `ErrNilChecker`, `ErrDuplicateChecker` |
| `RegisterInvoke` | `ErrMethodNameBlank`, `ErrHandlerNil`, `ErrMethodAlreadyRegistered` |

Sending has its own sentinels: `ErrMessageIDNotBlank` (a message with a pre-set `MessageId` was passed to `Send`), `ErrBlankTag` (a message's `Tag` was blank), `ErrDelayNotSupportedInTransaction` (a delayed message was passed to `SendTransaction`), `ErrUnknownTransactionStatus` (a transaction executer returned a `TransactionStatus` other than `CommitTransaction`/`RollbackTransaction`), and `ErrDeliverTimeInThePast` (a delayed message's computed delivery time is not in the future).

The library does not panic on invalid configuration or invalid input. `Assert`, `AssertError`, `Try`, and `SystemAssertPrefix` were removed entirely, and nothing replaced them by design — every place that used to assert-and-panic now returns an error instead.

## Trace ID Propagation

The library does not hardcode a context key it does not own for trace ids. Instead it exposes two constructor options the host passes once, at `New`:

```go
client, err := redismq.New(cfg,
    redismq.WithTraceIDFromContext(myctx.GetTraceID),
    redismq.WithTraceIDToContext(myctx.WithTraceID),
)
```

Mechanism: on publish, the trace id is read from the sending context via the `WithTraceIDFromContext` hook and stamped into the message's `CustomData["traceId"]`, which already round-trips through the message's `metadata` JSON on the stream. On consume, the library reads it back; if the message predates the feature (no stored trace id), it generates a new one. The resulting id is placed into the context via the `WithTraceIDToContext` hook before that context is passed to `IMessageListener.Consume` — so the host's own listener logs carry the trace id too, not just the library's. A redelivered or delayed message keeps its original trace id rather than being restamped.

Without these options, log lines are still structured but trace-less: the default hook returns `""` and no `trace_id` attribute is attached.

## Testing

Run the unit tests:

```
go test ./...
```

Two tests (`TestProducerAndConsumer`, `TestMethodInvoke`) are integration tests against a real Redis at `127.0.0.1:6379`. They probe the address with a short-timeout TCP dial and call `t.Skip` if nothing answers, so `go test ./...` passes in an environment with no Redis running — it just skips those two. To skip them unconditionally regardless of whether Redis happens to be reachable, run in short mode:

```
go test -short ./...
```

Run with the race detector, as this repository's CI does:

```
go test -race -count=1 ./...
```

## Migration Guide

### This release: package-global API → constructor-injected `Client`

Every package-level function and mutable global is gone. A host now calls `redismq.New(cfg, opts...)` once to get a `*Client`, and every operation is a method on it. Multiple `Client`s in the same process are fully independent — they do not share a registry, logger, tracer, or Redis connection.

| Before | After |
| --- | --- |
| `RegisterRedisMqConfig(ctx, *RedisMqConfig) error` + package-level state | `redismq.New(cfg RedisMqConfig, opts ...Option) (*Client, error)` |
| `SetLogger` / `SetStdLogger` / `SetSlogLogger` / `GetLogger` (runtime-mutable) | `WithLogger` / `WithStdLogger` / `WithSlogLogger` passed to `New` (fixed for the `Client`'s lifetime) |
| `SetTraceIDFromContext` / `SetTraceIDToContext` (runtime-mutable) | `WithTraceIDFromContext` / `WithTraceIDToContext` passed to `New` |
| `Send(ctx, message)` | `client.Send(ctx, message)` |
| `SendDelay(ctx, message, delay)` | `client.SendDelay(ctx, message, delay)` |
| `SendTransaction(ctx, message, executer)` | `client.SendTransaction(ctx, message, executer)` |
| `Invoke(ctx, req, timeoutSeconds) *InvokeResponse` | `client.Invoke(ctx, req, timeoutSeconds) *InvokeResponse` |
| `RegisterListener(ctx, i)` — no return value, dropped invalid/duplicate input silently | `client.RegisterListener(ctx, i) error` — returns `ErrNilListener`/`ErrTooManyTopics`/`ErrInvalidTopic`/`ErrDuplicateListener` |
| `RegisterChecker(ctx, i)` — no return value | `client.RegisterChecker(ctx, i) error` — returns `ErrNilChecker`/`ErrDuplicateChecker` |
| `RegisterInvoke(ctx, name, op)` — no return value | `client.RegisterInvoke(ctx, name, op) error` — returns `ErrMethodNameBlank`/`ErrHandlerNil`/`ErrMethodAlreadyRegistered` |
| `RegisterInternalListeners(ctx)` — mandatory, explicit, easy to forget | gone; `Start` wires it automatically |
| `StartRedisMqConsumer(ctx)` | `client.Start(ctx) error` — also starts the delay-queue background thread and the invoke keepalive loop, previously separate/manual steps |
| `StartDelayBackgroundThread(ctx)` | gone; folded into `client.Start(ctx)` |
| No graceful-shutdown entry point; message handlers ran fully detached | `client.Close(ctx) error` — cancels loops, waits for in-flight handlers bounded by `ctx`, then closes the Redis connection |
| `ErrConfigNotSet` | gone; structurally unreachable — a `Client` cannot exist without already-validated config |
| `ErrConfigNil` | gone; `RedisMqConfig` is passed by value, so `New` can never receive a nil config |

### Earlier breaking changes (module `/v2` → `/v3`)

| Before | After |
| --- | --- |
| Module path `github.com/Orfeo42/go-redismq/v2` | `github.com/Orfeo42/go-redismq/v3` |
| Package `go_redismq` | `redismq` |
| `InvoiceRequest` / `InvoiceResponse` | `InvokeRequest` / `InvokeResponse` |
| `Assert`, `AssertError`, `Try`, `SystemAssertPrefix` | removed entirely; invalid input returns an error instead of panicking |
| Printf-style logging (`Debugf("...%s...", x)`) | structured `slog.Attr` fields via the optional `AttrLogger` interface (see below); printf-style `Logger` still works as a fallback |
| Implicit `func init()` registration | removed; registration is explicit, now automatic only where `Start` performs it itself (the invoke listener) |
| Functions without `ctx` (e.g. old `Send(message)`) | every entry point takes `context.Context` as its first argument |
| `RegisterRedisMqConfig` — no return value, panicked on invalid config | config validated eagerly by `New`, returns `error` |

There are no `SendContext`/`SendDelayContext`/`SendTransactionContext` names at any point in this API's history — `Send`, `SendDelay`, and `SendTransaction` themselves took the `ctx` parameter from the `/v3` ctx-first change onward.

# Logger Configuration Guide

The library uses a flexible logger interface that allows you to inject your own logger implementation, passed as an `Option` to `New`.

## Quick Start

### 1. Default Colored Logger (Recommended for Development)

There is no implicit default logger — until one is set, the library's log calls are silent no-ops. Opt into the built-in colored dev logger with one option:

```go
package main

import (
    "context"

    "github.com/Orfeo42/go-redismq/v3"
)

func main() {
    ctx := context.Background()

    cfg := redismq.RedisMqConfig{Group: "YourGroup", Addr: "127.0.0.1:6379"}

    client, err := redismq.New(cfg, redismq.WithLogger(redismq.NewDefaultLogger()))
    if err != nil {
        panic(err)
    }

    if err := client.Start(ctx); err != nil {
        panic(err)
    }
}
```

**Control log level with environment variable:**

```bash
export LOG_LEVEL=DEBUG    # Shows all logs (DEBUG, INFO, WARN, ERROR)
export LOG_LEVEL=INFO     # Default - shows INFO, WARN, ERROR
export LOG_LEVEL=WARN     # Shows only WARN and ERROR
export LOG_LEVEL=ERROR    # Shows only ERROR
```

**Output format:**

```
[INFO] 2024-01-29 15:04:05 [thread-1] redismq: delay background thread started
[ERROR] 2024-01-29 15:04:05 [thread-1] [consumer.go:124] redismq: consumer iteration panicked
```

---

## Production Setups

### 2. JSON Structured Logging (Recommended for Production)

```go
package main

import (
    "context"
    "log/slog"
    "os"

    "github.com/Orfeo42/go-redismq/v3"
)

func main() {
    ctx := context.Background()

    cfg := redismq.RedisMqConfig{Group: "YourGroup", Addr: "127.0.0.1:6379"}

    // Create JSON logger
    logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level:     slog.LevelInfo,
        AddSource: false, // Set to true to include source code location
    }))

    client, err := redismq.New(cfg, redismq.WithSlogLogger(logger))
    if err != nil {
        panic(err)
    }

    if err := client.Start(ctx); err != nil {
        panic(err)
    }
}
```

**Output:**

```json
{
  "time": "2024-01-29T15:04:05",
  "level": "INFO",
  "msg": "redismq: delay background thread started"
}
```

---

### 3. Log to File

```go
package main

import (
    "context"
    "log/slog"
    "os"

    "github.com/Orfeo42/go-redismq/v3"
)

func main() {
    ctx := context.Background()

    cfg := redismq.RedisMqConfig{Group: "YourGroup", Addr: "127.0.0.1:6379"}

    // Open log file
    logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
    if err != nil {
        panic(err)
    }
    defer logFile.Close()

    // Create logger writing to file
    logger := slog.New(slog.NewJSONHandler(logFile, &slog.HandlerOptions{
        Level: slog.LevelInfo,
    }))

    client, err := redismq.New(cfg, redismq.WithSlogLogger(logger))
    if err != nil {
        panic(err)
    }

    if err := client.Start(ctx); err != nil {
        panic(err)
    }
}
```

---

### 4. Standard Go Logger

```go
package main

import (
    "context"
    "log"
    "os"

    "github.com/Orfeo42/go-redismq/v3"
)

func main() {
    ctx := context.Background()

    cfg := redismq.RedisMqConfig{Group: "YourGroup", Addr: "127.0.0.1:6379"}

    logger := log.New(os.Stdout, "[RedisMQ] ", log.LstdFlags)

    client, err := redismq.New(cfg, redismq.WithStdLogger(logger))
    if err != nil {
        panic(err)
    }

    if err := client.Start(ctx); err != nil {
        panic(err)
    }
}
```

---

## Third-Party Logger Integration

The `Logger` interface itself is unchanged from earlier versions — every adapter below still compiles and works with no changes to the adapter type. The only thing that changed is how it is wired in: through `WithLogger(...)` passed to `New`, instead of a runtime `SetLogger(...)` call.

### 5. Logrus

```go
package main

import (
    "context"

    "github.com/sirupsen/logrus"

    "github.com/Orfeo42/go-redismq/v3"
)

// Create adapter
type LogrusAdapter struct {
    logger *logrus.Logger
}

func (l *LogrusAdapter) Debugf(format string, args ...any) {
    l.logger.Debugf(format, args...)
}

func (l *LogrusAdapter) Infof(format string, args ...any) {
    l.logger.Infof(format, args...)
}

func (l *LogrusAdapter) Warnf(format string, args ...any) {
    l.logger.Warnf(format, args...)
}

func (l *LogrusAdapter) Errorf(format string, args ...any) {
    l.logger.Errorf(format, args...)
}

func main() {
    ctx := context.Background()

    cfg := redismq.RedisMqConfig{Group: "YourGroup", Addr: "127.0.0.1:6379"}

    // Setup logrus
    logrusLogger := logrus.New()
    logrusLogger.SetLevel(logrus.InfoLevel)
    logrusLogger.SetFormatter(&logrus.JSONFormatter{})

    // Use adapter
    client, err := redismq.New(cfg, redismq.WithLogger(&LogrusAdapter{logger: logrusLogger}))
    if err != nil {
        panic(err)
    }

    if err := client.Start(ctx); err != nil {
        panic(err)
    }
}
```

---

### 6. Zap

```go
package main

import (
    "context"

    "go.uber.org/zap"

    "github.com/Orfeo42/go-redismq/v3"
)

// Create adapter
type ZapAdapter struct {
    sugar *zap.SugaredLogger
}

func (l *ZapAdapter) Debugf(format string, args ...any) {
    l.sugar.Debugf(format, args...)
}

func (l *ZapAdapter) Infof(format string, args ...any) {
    l.sugar.Infof(format, args...)
}

func (l *ZapAdapter) Warnf(format string, args ...any) {
    l.sugar.Warnf(format, args...)
}

func (l *ZapAdapter) Errorf(format string, args ...any) {
    l.sugar.Errorf(format, args...)
}

func main() {
    ctx := context.Background()

    cfg := redismq.RedisMqConfig{Group: "YourGroup", Addr: "127.0.0.1:6379"}

    // Setup zap
    zapLogger, _ := zap.NewProduction()
    defer zapLogger.Sync()

    // Use adapter
    client, err := redismq.New(cfg, redismq.WithLogger(&ZapAdapter{sugar: zapLogger.Sugar()}))
    if err != nil {
        panic(err)
    }

    if err := client.Start(ctx); err != nil {
        panic(err)
    }
}
```

---

### 7. Zerolog

```go
package main

import (
    "context"
    "os"

    "github.com/rs/zerolog"

    "github.com/Orfeo42/go-redismq/v3"
)

// Create adapter
type ZerologAdapter struct {
    logger zerolog.Logger
}

func (l *ZerologAdapter) Debugf(format string, args ...any) {
    l.logger.Debug().Msgf(format, args...)
}

func (l *ZerologAdapter) Infof(format string, args ...any) {
    l.logger.Info().Msgf(format, args...)
}

func (l *ZerologAdapter) Warnf(format string, args ...any) {
    l.logger.Warn().Msgf(format, args...)
}

func (l *ZerologAdapter) Errorf(format string, args ...any) {
    l.logger.Error().Msgf(format, args...)
}

func main() {
    ctx := context.Background()

    cfg := redismq.RedisMqConfig{Group: "YourGroup", Addr: "127.0.0.1:6379"}

    // Setup zerolog
    zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()

    // Use adapter
    client, err := redismq.New(cfg, redismq.WithLogger(&ZerologAdapter{logger: zerologLogger}))
    if err != nil {
        panic(err)
    }

    if err := client.Start(ctx); err != nil {
        panic(err)
    }
}
```

---

## Logger Interface

To implement your own logger, satisfy this interface:

```go
type Logger interface {
    Debugf(format string, args ...any)
    Infof(format string, args ...any)
    Warnf(format string, args ...any)
    Errorf(format string, args ...any)
}
```

**Example:**

```go
type MyLogger struct{}

func (l *MyLogger) Debugf(format string, args ...any) {
    // Your implementation
}

func (l *MyLogger) Infof(format string, args ...any) {
    // Your implementation
}

func (l *MyLogger) Warnf(format string, args ...any) {
    // Your implementation
}

func (l *MyLogger) Errorf(format string, args ...any) {
    // Your implementation
}

// Use it
client, err := redismq.New(cfg, redismq.WithLogger(&MyLogger{}))
```

---

## Structured Attributes (AttrLogger)

The `Logger` interface above is unchanged. Every custom adapter shown in this guide (Logrus, Zap, Zerolog, or your own) keeps compiling and keeps working with no code change.

Internally, the library emits log events through a second, optional interface that it type-asserts for:

```go
type AttrLogger interface {
    LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}
```

- A logger that implements `AttrLogger` receives a static message plus real `slog.Attr` fields and the request's `context.Context`.
- A logger that does **not** implement it falls back to the existing printf methods (`Debugf`, `Infof`, `Warnf`, `Errorf`). The fields are rendered into the message as `msg key=value key=value` rather than lost — the only difference is that they are no longer separately queryable in your log aggregator.
- `WithSlogLogger` upgrades a `Client` to full structured output automatically: the built-in slog adapter implements `AttrLogger` natively, so nothing beyond passing the option is required on the host side.

To opt a custom adapter into structured output, implement `LogAttrs` alongside the printf methods:

```go
func (l *ZapAdapter) LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
    fields := make([]zap.Field, 0, len(attrs))
    for _, a := range attrs {
        fields = append(fields, zap.Any(a.Key, a.Value.Any()))
    }

    switch level {
    case slog.LevelWarn:
        l.sugar.Desugar().Warn(msg, fields...)
    case slog.LevelError:
        l.sugar.Desugar().Error(msg, fields...)
    default:
        l.sugar.Desugar().Info(msg, fields...)
    }
}
```

### Correct `source`

Every library log line reports `source` as the real call site inside the package that logged it (e.g. `consumer.go`, function `dropMessage`) rather than the logging plumbing itself. The slog adapter builds its `slog.Record` manually with `runtime.Callers`, skipping exactly the right number of frames depending on whether the call reached it directly (every engine package, and `Client`'s own methods) — this is what makes `AddSource: true` (see [JSON Structured Logging](#2-json-structured-logging-recommended-for-production)) worth enabling: the line points at actionable code, not at the library's own logging wrapper.

---

## Structured Log Attributes

Attribute keys emitted alongside library log events are stable and `snake_case`, safe to filter and aggregate on in a log aggregator. This is not an exhaustive list of every attribute the library emits — it covers the ones that appear across multiple event types:

| Key | Meaning |
| --- | --- |
| `topic` | Message topic |
| `tag` | Message tag |
| `message_id` | Redis stream message ID |
| `message_key` | Caller-supplied dedupe/business key, when set |
| `consumer_group` | Redis consumer group name |
| `consumer_name` | Consumer instance identifier (resolved from the local IPv4 address) |
| `stream` | Redis stream name backing the topic |
| `reconsume_times` | Number of times this message has been redelivered |
| `reconsume_max` | Per-message redelivery cap set by the sender |
| `reconsume_limit` | Effective redelivery ceiling applied by the consumer (`max(40, reconsume_max)`) |
| `cost_ms` | Milliseconds between send and consume |
| `trace_id` | Trace id read from the message, when present |
| `cause` | The root-cause error value |
| `stack` | Captured call stack, only on recovered panics |

Two rules apply library-wide:

- **The root cause is always keyed `cause`, never `error`.** Hosts conventionally reserve `error` for their own boundary field; a duplicate JSON key would shadow it.
- **The library never logs at Debug level.** Deployed environments filter Debug out, so a Debug line is unreachable — every library event is Info, Warn, or Error, or it is not logged at all.

Level convention:

- Recovered panics are logged at Error with a `stack` attribute.
- Redis/transport failures the library recovers from (connection close, publish failure, ack failure) are Warn, with no stack.
- Expected conditions — a consumer group that already exists, a message with no registered listener, an expired message being dropped, a message exhausting its retry budget — are Info or Warn.

---

## Tips

1. **Development**: Use default colored logger with `LOG_LEVEL=DEBUG`
2. **Production**: Use JSON structured logging for better log aggregation
3. **File Logging**: Remember to handle log rotation (use tools like logrotate)
4. **Performance**: If high performance is critical, use Zap or Zerolog
5. **Integration**: If you already use a logger in your app, create an adapter

---

## Environment Variables

- `LOG_LEVEL`: Controls `NewDefaultLogger`'s level (DEBUG, INFO, WARN, ERROR)
  - Default: INFO
  - Example: `export LOG_LEVEL=DEBUG`

---

## API Summary

```go
func New(cfg RedisMqConfig, opts ...Option) (*Client, error)

func (c *Client) Send(ctx context.Context, m *Message) (bool, error)
func (c *Client) SendDelay(ctx context.Context, m *Message, delay int64) (bool, error)
func (c *Client) SendTransaction(ctx context.Context, m *Message, executer func(*Message) (TransactionStatus, error)) (bool, error)
func (c *Client) Invoke(ctx context.Context, req *InvokeRequest, timeoutSeconds int) *InvokeResponse
func (c *Client) RegisterListener(ctx context.Context, l IMessageListener) error
func (c *Client) RegisterChecker(ctx context.Context, ch IMessageChecker) error
func (c *Client) RegisterInvoke(ctx context.Context, method string, op func(ctx context.Context, request any) (response any, err error)) error
func (c *Client) Start(ctx context.Context) error
func (c *Client) Close(ctx context.Context) error

type Option func(*settings)

func WithLogger(l Logger) Option
func WithSlogLogger(l *slog.Logger) Option
func WithStdLogger(l *log.Logger) Option
func WithTraceIDFromContext(fn func(ctx context.Context) string) Option
func WithTraceIDToContext(fn func(ctx context.Context, traceID string) context.Context) Option
func WithClock(c Clock) Option

func NewDefaultLogger() Logger
```
