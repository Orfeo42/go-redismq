# GO-REDISMQ

**go-redismq** is a Go library for implementing distributed message queues using Redis Streams. It supports message production, consumption, delayed delivery, transactions, and method invocation patterns.

## Features

- Message queueing with Redis Streams
- Delayed message delivery
- Transactional message sending and checking
- Method invocation via messages
- Customizable message listeners and checkers

## Getting Started

### Installation

Add the module to your project:

```
go get github.com/Orfeo42/go-redismq/v2
```

### Basic Usage

The startup sequence below is order-sensitive — each step depends on the one before it:

```go
package main

import (
    "context"
    "log/slog"

    goredismq "github.com/Orfeo42/go-redismq/v2"
)

type MyListener struct{}

func (l MyListener) GetTopic() string { return "topic" }
func (l MyListener) GetTag() string   { return "tag" }
func (l MyListener) Consume(ctx context.Context, msg *goredismq.Message) goredismq.Action {
    // handle message
    return goredismq.CommitMessage
}

func main() {
    ctx := context.Background()

    goredismq.SetSlogLogger(slog.Default())

    cfg := &goredismq.RedisMqConfig{
        Group:    "YourGroup",
        Addr:     "127.0.0.1:6379",
        Password: "",
        Database: 0,
    }
    if err := goredismq.RegisterRedisMqConfig(cfg); err != nil {
        panic(err)
    }

    goredismq.RegisterListener(ctx, &MyListener{})
    goredismq.RegisterInternalListeners(ctx)
    goredismq.StartRedisMqConsumer(ctx)
}
```

Why the order matters:

1. **The logger goes first.** There is no implicit default logger any more: until one is set, the library's log calls are silent no-ops, so a config or registration problem raised before this line would produce no log output at all. For the built-in colored dev logger instead of a `slog.Logger`, use `goredismq.SetLogger(goredismq.NewDefaultLogger())`.
2. **`RegisterRedisMqConfig` returns an error** instead of panicking on an invalid config — check it before continuing (see [Error Handling](#error-handling)).
3. **Listeners must be registered before the consumer starts** so their topics are known when the consume loops spin up.
4. **`RegisterInternalListeners(ctx)` is mandatory if the host uses `Invoke`.** This used to happen implicitly in a `func init()`; that implicit registration is gone. Forgetting this call means the invoke listener is never registered and every `Invoke` call fails to find its handler — this is the single most likely upgrade mistake.
5. **`StartRedisMqConsumer(ctx)` owns the root context.** The `ctx` passed here is the one every background loop, Redis call, and log line in the library derives from (see [Context and Graceful Shutdown](#context-and-graceful-shutdown)).

**Send a message:**

```go
if _, err := goredismq.Send(ctx, &goredismq.Message{
    Topic: "topic",
    Tag:   "tag",
    Body:  "Hello, World!",
}); err != nil {
    // handle error
}
```

## Context and Graceful Shutdown

Every entry point takes a `context.Context` — there are no ctx-less variants. The root context passed to `StartRedisMqConsumer` reaches every background loop, every Redis call, and every log line the library emits.

Cancelling that context shuts the library down in an orderly way: the consume loops, the delay-queue poller, the trim scheduler, and the invoke keepalive loop all exit on cancellation, and the 60-second blocking stream read (`XReadGroup` with `Block: 60 * time.Second`) unblocks immediately instead of waiting out its timeout. Cancellation is treated as a normal shutdown, not a fault — it does not produce error-level log noise.

```go
ctx, cancel := context.WithCancel(context.Background())

goredismq.StartRedisMqConsumer(ctx)

// ... application runs ...

cancel() // consume loops, delay poller, trim scheduler and invoke
         // keepalive all exit; no error-level logs are produced
```

## Error Handling

`RegisterRedisMqConfig` returns an `error` instead of panicking on invalid configuration. The sentinels are plain package vars, comparable with `errors.Is`:

```go
var (
    ErrConfigNil        = errors.New("redismq: config is nil")
    ErrConfigAddrBlank  = errors.New("redismq: config addr is blank")
    ErrConfigGroupBlank = errors.New("redismq: config group is blank")
    ErrConfigNotSet     = errors.New("redismq: redis config not registered")
)
```

`ErrConfigNil`, `ErrConfigAddrBlank`, and `ErrConfigGroupBlank` are returned by `RegisterRedisMqConfig`. `ErrConfigNotSet` is returned by `GetRedisConfig` (and therefore by every function that calls it internally — `Send`, `SendDelay`, `SendTransaction`, and the consumer/listener machinery) when no config has been registered yet:

```go
if err := goredismq.RegisterRedisMqConfig(cfg); err != nil {
    switch {
    case errors.Is(err, goredismq.ErrConfigNil):
        // cfg was nil
    case errors.Is(err, goredismq.ErrConfigAddrBlank):
        // cfg.Addr was empty
    case errors.Is(err, goredismq.ErrConfigGroupBlank):
        // cfg.Group was empty
    }
}
```

Sending also has its own sentinels, `ErrMessageIDNotBlank` (a message with a pre-set `MessageId` was passed to `Send`) and `ErrDeliverTimeInThePast` (a delayed message's computed delivery time is not in the future).

The library no longer panics on invalid configuration. `Assert`, `AssertError`, `Try`, and `SystemAssertPrefix` were removed entirely — every place that used to assert-and-panic now returns an error instead.

## Trace ID Propagation

The library does not hardcode a context key it does not own for trace ids. Instead it exposes two package-level hooks the host registers once at startup:

```go
var TraceIDFromContext = func(ctx context.Context) string { return "" }
var TraceIDToContext = func(ctx context.Context, traceID string) context.Context { return ctx }
```

```go
goredismq.TraceIDFromContext = myctx.GetTraceID
goredismq.TraceIDToContext = myctx.WithTraceID
```

Mechanism: on publish, the trace id is read from the sending context via `TraceIDFromContext` and stamped into the message's `CustomData["traceId"]`, which already round-trips through the message's `metadata` JSON on the stream. On consume, the library reads it back; if the message predates the feature (no stored trace id), it generates a new one. The resulting id is placed into the context via `TraceIDToContext` before that context is passed to `IMessageListener.Consume` — so the host's own listener logs carry the trace id too, not just the library's. A redelivered or delayed message keeps its original trace id rather than being restamped.

Until both hooks are registered, log lines are still structured but trace-less: `TraceIDFromContext` returns `""` and no `trace_id` attribute is attached.

## Testing

Run unit tests:

```
go test ./test/...
```

## Migration Guide (Breaking Changes)

A refactor replaced printf logging with structured `slog` attributes, replaced assertion/panic validation with returned errors, removed the implicit `init()` registration, and threaded `context.Context` through every entry point. This is a breaking change for every host.

`go.mod` currently declares `github.com/Orfeo42/go-redismq/v2`. A further bump to `/v3` is a pending release decision and is not live — do not import `/v3` paths.

| Function / behavior | Before | After |
| --- | --- | --- |
| `RegisterRedisMqConfig` | no return value; panicked on invalid config | returns `error` (`ErrConfigNil`, `ErrConfigAddrBlank`, `ErrConfigGroupBlank`) |
| `GetRedisConfig` | returns `*redis.Options` | returns `(*redis.Options, error)` (`ErrConfigNotSet`) |
| `Send` | `Send(message *Message)` | `Send(ctx context.Context, message *Message)` |
| `SendDelay` | `SendDelay(message *Message, delay int64)` | `SendDelay(ctx context.Context, message *Message, delay int64)` |
| `SendTransaction` | `SendTransaction(message, executer)` | `SendTransaction(ctx, message, executer)` |
| `RegisterListener` | `RegisterListener(i)` | `RegisterListener(ctx, i)` |
| `RegisterChecker` | `RegisterChecker(i)` | `RegisterChecker(ctx, i)` |
| `RegisterInvoke` | `RegisterInvoke(name, op)` | `RegisterInvoke(ctx, name, op)` |
| `StartRedisMqConsumer` | `StartRedisMqConsumer()` | `StartRedisMqConsumer(ctx)` |
| `StartDelayBackgroundThread` | `StartDelayBackgroundThread()` | `StartDelayBackgroundThread(ctx)` |
| Invoke listener registration | implicit, via `func init()` | explicit `RegisterInternalListeners(ctx)` — the host must call it |
| Default logger | implicit, active until overridden | none; `logger` stays unset until `SetLogger`/`SetSlogLogger`/`SetStdLogger` is called (opt into the old default with `SetLogger(NewDefaultLogger())`) |
| `Assert`, `AssertError`, `Try`, `SystemAssertPrefix` | present (`assert.go`) | removed; invalid input returns an error or is logged and dropped instead of panicking |

There are no `SendContext`/`SendDelayContext`/`SendTransactionContext` names at any point in this API — `Send`, `SendDelay`, and `SendTransaction` themselves took the `ctx` parameter.

# Logger Configuration Guide

The library uses a flexible logger interface that allows you to inject your own logger implementation.

## Quick Start

### 1. Default Colored Logger (Recommended for Development)

There is no implicit default logger — until one is set, the library's log calls are silent no-ops. Opt into the built-in colored dev logger with one call:

```go
package main

import (
    "context"

    "github.com/Orfeo42/go-redismq/v2"
)

func main() {
    ctx := context.Background()

    go_redismq.SetLogger(go_redismq.NewDefaultLogger())

    go_redismq.StartRedisMqConsumer(ctx)
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
[INFO] 2024-01-29 15:04:05 [thread-1] MQStream Start Delay Queue!
[ERROR] 2024-01-29 15:04:05 [thread-1] [consumer.go:124] MQStream Error...
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

    "github.com/Orfeo42/go-redismq/v2"
)

func main() {
    ctx := context.Background()

    // Create JSON logger
    logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level:     slog.LevelInfo,
        AddSource: false, // Set to true to include source code location
    }))

    go_redismq.SetSlogLogger(logger)
    go_redismq.StartRedisMqConsumer(ctx)
}
```

**Output:**

```json
{
  "time": "2024-01-29T15:04:05",
  "level": "INFO",
  "msg": "MQStream Start Delay Queue!"
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

    "github.com/Orfeo42/go-redismq/v2"
)

func main() {
    ctx := context.Background()

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

    go_redismq.SetSlogLogger(logger)
    go_redismq.StartRedisMqConsumer(ctx)
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

    "github.com/Orfeo42/go-redismq/v2"
)

func main() {
    ctx := context.Background()

    logger := log.New(os.Stdout, "[RedisMQ] ", log.LstdFlags)
    go_redismq.SetStdLogger(logger)

    go_redismq.StartRedisMqConsumer(ctx)
}
```

---

## Third-Party Logger Integration

### 5. Logrus

```go
package main

import (
    "context"

    "github.com/sirupsen/logrus"

    "github.com/Orfeo42/go-redismq/v2"
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

    // Setup logrus
    logrusLogger := logrus.New()
    logrusLogger.SetLevel(logrus.InfoLevel)
    logrusLogger.SetFormatter(&logrus.JSONFormatter{})

    // Use adapter
    go_redismq.SetLogger(&LogrusAdapter{logger: logrusLogger})
    go_redismq.StartRedisMqConsumer(ctx)
}
```

---

### 6. Zap

```go
package main

import (
    "context"

    "go.uber.org/zap"

    "github.com/Orfeo42/go-redismq/v2"
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

    // Setup zap
    zapLogger, _ := zap.NewProduction()
    defer zapLogger.Sync()

    // Use adapter
    go_redismq.SetLogger(&ZapAdapter{sugar: zapLogger.Sugar()})
    go_redismq.StartRedisMqConsumer(ctx)
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

    "github.com/Orfeo42/go-redismq/v2"
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

    // Setup zerolog
    zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()

    // Use adapter
    go_redismq.SetLogger(&ZerologAdapter{logger: zerologLogger})
    go_redismq.StartRedisMqConsumer(ctx)
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
go_redismq.SetLogger(&MyLogger{})
```

---

## Structured Attributes (AttrLogger)

The `Logger` interface above is unchanged. Every custom adapter shown in this guide (Logrus, Zap, Zerolog, or your own) keeps compiling and keeps working with no code change.

Internally, the library now emits log events through a second, optional interface that it type-asserts for on every call:

```go
type AttrLogger interface {
    LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}
```

- A logger that implements `AttrLogger` receives a static message plus real `slog.Attr` fields and the request's `context.Context`.
- A logger that does **not** implement it falls back to the existing printf methods (`Debugf`, `Infof`, `Warnf`, `Errorf`). The fields are rendered into the message as `msg key=value key=value` rather than lost — the only difference is that they are no longer separately queryable in your log aggregator.
- `SetSlogLogger` upgrades a host to full structured output automatically: the built-in slog adapter implements `AttrLogger` natively, so nothing beyond the version bump is required on the host side.

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

Previously every library log line reported `source` inside the logger wrapper itself (`logger.go`), because slog captured the program counter at the printf call site, one frame removed from the real caller. The slog adapter now builds its `slog.Record` manually with `runtime.Callers`, so `source` reports the real call site — e.g. `consumer.go`, function `dropMessage` — instead of the wrapper. This is what makes `AddSource: true` (see [JSON Structured Logging](#2-json-structured-logging-recommended-for-production)) worth enabling: the line points at actionable code, not at the library's own logging plumbing.

---

## Structured Log Attributes

Attribute keys emitted alongside library log events are stable and `snake_case`, safe to filter and aggregate on in a log aggregator:

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

- `LOG_LEVEL`: Controls the default logger level (DEBUG, INFO, WARN, ERROR)
  - Default: INFO
  - Example: `export LOG_LEVEL=DEBUG`

---

## API Functions

```go
// Set custom logger implementing the Logger interface
func SetLogger(l Logger)

// Set Go's standard log.Logger
func SetStdLogger(l *log.Logger)

// Set Go's slog.Logger
func SetSlogLogger(l *slog.Logger)

// Get current logger
func GetLogger() Logger
```
