# Architecture

EnterpriseKafka follows clean architecture boundaries:

- `EnterpriseKafka.Abstractions` owns stable contracts consumed by application services.
- `EnterpriseKafka.Core` owns Confluent.Kafka integration, DI, producer/consumer engines, middleware execution, retry orchestration, topic resolution, and default strategies.
- Feature packages (`Producer`, `Consumer`, `Retry`, `Logging`, `Telemetry`, `HealthChecks`, `Monitoring`, `Testing`) provide deployment-friendly package boundaries and extension points.

## Core abstractions

- `IKafkaProducer` publishes single messages and batches with serialization, keying, header enrichment, correlation ids, retries, tracing, partition targeting, and delivery metadata.
- `IKafkaHandler<T>` is the base strongly typed handler contract; `IKafkaMessageHandler<T>` inherits from it and is the recommended developer-facing name for new services. Handlers should contain business logic only.
- `IKafkaMiddleware` provides ASP.NET Core-style middleware for logging, tracing, validation, idempotency, auditing, and custom cross-cutting concerns.
- `IKafkaRetryEngine` isolates retry implementation; the default uses Polly and can be replaced.
- `IMessageFailureStrategy` supports DLQ, ignore, database persistence, alert-only, stop-consumer, and custom handling.
- `ITopicResolver` supports explicit mapping, attributes, environment naming, retry topic naming, DLQ naming, and convention fallback.

## Enterprise concerns

### Producer engine

The producer is singleton and thread-safe through the Confluent producer. It enables idempotence by default, uses `acks=all`, enriches headers with `x-correlation-id`, `content-type`, and trace context, and returns topic/partition/offset persistence metadata. Batch publishing intentionally reuses the same publish path so retries, serialization, telemetry, partition handling, and header enrichment are consistent.

### Consumer engine

Consumers are registered by DI with `AddConsumer<TMessage,THandler>()`. The hosted service creates topic/group subscriptions, deserializes payloads, builds `KafkaContext`, executes middleware, invokes the strongly typed handler, and commits offsets only after successful processing. Auto-commit is disabled by default to support at-least-once delivery and predictable recovery.

### Middleware pipeline

Middleware runs in registration order and can be composed with:

- `UseLogging()` for structured support logs.
- `UseTracing()` for `ActivitySource` and metrics via `Meter`.
- `UseRetry()` for Polly-backed retry execution.
- `UseValidation()` for contract guardrails.
- `UseMiddleware<T>()` for custom enterprise concerns such as audit trails, idempotency, schema validation, authorization, or replay guards.

### Failure handling

DLQ is optional. Teams choose a strategy based on support model and criticality:

- `UseDeadLetterQueue()` for poison-message isolation and replay.
- `Ignore()` for low-value telemetry streams.
- `PersistToDatabase()` for support dashboards and manual replay workflows.
- `AlertOnly()` for operations-owned intervention.
- `StopConsumer()` for strict data integrity streams.
- `UseCustomHandler<T>()` for domain-specific workflows.

### Retry architecture

Immediate and exponential backoff retries are configured with `RetryOptions`. Retry topics are modeled through `ITopicResolver.ResolveRetryTopic()` so delayed retry and scheduled retry workers can be added without changing business handlers.

### Observability and supportability

Every message should expose correlation id, topic, partition, offset, consumer group, retry attempt, processing duration, and failure strategy in logs and metrics. OpenTelemetry exporters can subscribe to `EnterpriseKafka` `ActivitySource` and `Meter`; Serilog receives structured properties through `ILogger` scopes.

### Security

`SecurityOptions` models SASL, SSL, OAuth endpoint metadata, and Azure Key Vault integration points. Secrets should be resolved by configuration providers or an `ISecretProvider` extension before options reach the producer/consumer configuration.

### Scaling and throughput

- Scale consumer replicas up to the partition count for a topic.
- Use stable keys to preserve aggregate ordering within a partition.
- Tune partitions, `linger.ms`, batch size, channel capacity, and max poll interval per workload.
- Keep handlers idempotent and short; offload slow dependencies through bounded queues or retryable outbox workflows.
- Use lag and processing duration metrics to drive horizontal scaling.

### Delivery semantics

The default strategy is at-least-once: process successfully, then commit. Exactly-once end-to-end requires Kafka transactions plus transactional writes to downstream state, and should be reserved for workloads that justify the operational cost. Most enterprise services should combine at-least-once delivery, idempotent consumers, deduplication keys, and audit trails.

### Kubernetes guidance

Set `terminationGracePeriodSeconds` to cover in-flight processing, wire readiness to broker/topic/lag health checks, use PodDisruptionBudgets for critical consumers, configure CPU/memory requests based on batch and handler behavior, and roll deployments gradually to avoid consumer-group churn.
