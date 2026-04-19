# AMQP Broker Examples

Examples demonstrating all features of the `amqp-broker` library, from basic publish/consume operations to advanced production patterns.

## Prerequisites

- Go 1.25 or later
- RabbitMQ 3.8+ running on `localhost:5672` (default guest/guest credentials)

### Starting RabbitMQ

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## Examples

### Basics

| Directory | Description |
| --- | --- |
| [publish](publish/) | One-off publish via `b.Publish()` and managed `NewPublisher()` |
| [consume](consume/) | One-off consume via `b.Consume()` and managed `NewConsumer()` |
| [publish-consume](publish-consume/) | Bidirectional publish + consume in a single program |

### Topology

| Directory | Description |
| --- | --- |
| [topology](topology/) | Exchange types, queue types, binding types, routing keys, Sync() and operations |
| [message](message/) | MessageBuilder API, JSON messages, properties, persistent delivery, metadata |
| [handler](handler/) | Handler actions, error handling, context usage |

### Middlewares

| Directory | Description |
| --- | --- |
| [middleware-logging](middleware-logging/) | `LoggingMiddleware`: structured lifecycle logging |
| [middleware-metrics](middleware-metrics/) | `MetricsMiddleware`: per-message duration recording and outcome tracking |
| [middleware-debug](middleware-debug/) | `DebugMiddleware`: full message field logging |
| [middleware-recovery](middleware-recovery/) | `RecoveryMiddleware`: panic recovery |
| [middleware-fallback](middleware-fallback/) | `FallbackMiddleware`: invoke a secondary handler on primary error |
| [middleware-retry](middleware-retry/) | `RetryMiddleware`: exponential-backoff retries |
| [middleware-circuit-breaker](middleware-circuit-breaker/) | `CircuitBreakerMiddleware`: fault isolation |
| [middleware-concurrency](middleware-concurrency/) | `ConcurrencyMiddleware`: max-parallel handlers |
| [middleware-rate-limit](middleware-rate-limit/) | `RateLimitMiddleware`: token-bucket rate limiting |
| [middleware-deduplication](middleware-deduplication/) | `DeduplicationMiddleware`: skip already-seen messages via a pluggable cache |
| [middleware-validation](middleware-validation/) | `ValidationMiddleware`: reject messages that fail a custom predicate |
| [middleware-transform](middleware-transform/) | `TransformMiddleware`: body rewriting (gzip, base64, lowercase) |
| [middleware-deadline](middleware-deadline/) | `DeadlineMiddleware`: discard messages past their deadline |
| [middleware-timeout](middleware-timeout/) | `TimeoutMiddleware`: per-message time limit |
| [middleware-batch](middleware-batch/) | `BatchMiddleware`: aggregate messages into batches before processing |
| [middleware-stack](middleware-stack) | `WrapHandler` with a full 10-layer middleware stack |

### Patterns

| Directory | Description |
| --- | --- |
| [pattern-rpc](pattern-rpc/) | Request-reply via `ReplyTo` / `CorrelationID` |
| [pattern-work-queue](pattern-work-queue/) | Task distribution across multiple workers |
| [pattern-pub-sub](pattern-pub-sub/) | Fanout exchange: every subscriber receives every event |
| [pattern-direct](pattern-direct/) | Direct exchange: route by exact routing key |
| [pattern-topic](pattern-topic/) | Topic exchange: wildcard routing (`*`, `#`) |
| [pattern-headers](pattern-headers/) | Headers exchange: route by message headers |

### Advanced

| Directory | Description |
| --- | --- |
| [advanced-config](advanced-config) | Connection pool, lifecycle hooks, reconnection, publisher confirms, QoS, endpoint caching, custom context |
| [advanced-graceful-shutdown](advanced-graceful-shutdown/) | Context cancellation + WaitGroup for clean shutdown |
| [advanced-error-recovery](advanced-error-recovery/) | Auto-reconnect + retry loop for resilient consumers |
| [advanced-idempotency](advanced-idempotency/) | `TTLCache` with SHA-256 content-hash fallback wired into `DeduplicationMiddleware` |
| [advanced-low-level](advanced-low-level/) | Raw `Channel`, `Connection`, `Transaction`, and `Release` APIs |
| [advanced-production](advanced-production/) | Full production setup: pool + middleware stack + graceful shutdown |

## Running an Example

Each example is a standalone Go program:

```bash
cd {name-of-example}
go run main.go
```

## Running All Examples

A runner script at the root of this directory runs every example in the canonical order used in this README:

```bash
go run main.go
```

Or via the `Makefile` from the repository root:

```bash
make examples
```

Each example is given a 30-second timeout. Long-running demos (e.g. `advanced-error-recovery`) will be reported as `TIMEOUT`, which is expected, as they are designed to run until interrupted. Only non-zero exits (compile errors, panics, `log.Fatal`) count as failures.

## Connection Details

All examples connect to `amqp://guest:guest@localhost:5672/`.
Edit the URL in `main.go` if your RabbitMQ instance uses different settings.

## Additional Resources

- [Package documentation](https://pkg.go.dev/github.com/MarwanAlsoltany/amqp-broker)
- [RabbitMQ documentation](https://www.rabbitmq.com/documentation.html)
- [AMQP 0.9.1 specification](https://www.rabbitmq.com/amqp-0-9-1-reference.html)
