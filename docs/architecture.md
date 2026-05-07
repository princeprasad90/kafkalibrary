# Architecture

EnterpriseKafka is split into small projects so application code can depend on contracts while infrastructure code owns Kafka-specific implementation details.

## Layers

- `EnterpriseKafka.Abstractions` contains interfaces and shared message context types.
- `EnterpriseKafka.Core` contains default infrastructure: JSON serialization, topic resolution, producer implementation, service registration, middleware, and the hosted consumer bootstrap.
- `EnterpriseKafka.Web` is a sample and internal test utility for manual broker validation.

## Producer flow

1. Application code calls `IKafkaProducer.PublishAsync` or `PublishBatchAsync`.
2. The producer chooses a topic from `KafkaPublishOptions.Topic` or `ITopicResolver`.
3. `JsonMessageSerializer` serializes the message to UTF-8 JSON bytes.
4. The producer adds the `x-correlation-id` header and any custom headers.
5. `Confluent.Kafka` sends the message and logs topic, partition, offset, and correlation id.

## Topic resolution order

1. Explicit `KafkaPublishOptions.Topic` supplied by the caller.
2. `EnterpriseKafkaOptions.TopicMappings` entry for the message type full name or type name.
3. `[KafkaTopic("topic-name")]` on the message class.
4. Lowercase message type name convention.

## Consumer model

The public consumer contract is `IKafkaHandler<T>`. A handler receives the deserialized message plus `KafkaContext`, which includes topic, key, correlation id, partition, offset, consumer group, and an item bag for cross-cutting data.

The current core `KafkaConsumerHostedService` is a bootstrap placeholder that logs startup and expects application-level consumer registration extensions. This keeps the contracts stable while allowing different services to choose their own subscription, retry, and scaling models.

## Middleware

`IKafkaMiddleware` provides an ASP.NET Core-style pipeline:

- Log message metadata and latency.
- Add metrics or tracing spans.
- Validate message context.
- Add data to `KafkaContext.Items` for later middleware or handlers.

`LoggingMiddleware` is the default sample middleware.

## Failure strategy

`IMessageFailureStrategy` separates handler failure policy from message handling. The default `IgnoreStrategy` is intentionally minimal. Production services can replace it with dead-letter topic publishing, retry with backoff, alerting, or parking-topic behavior.

## Web test console architecture

The web sample uses MVC plus SignalR:

- `HomeController.Publish` sends JSON payloads through `IKafkaProducer`.
- `KafkaTestConsoleService` starts and stops a single test consumer for a selected topic and group.
- `KafkaMessagesHub` broadcasts consumer state and received messages to connected browsers.
- The browser keeps the displayed table current and the service stores the latest 100 messages in memory.

This console is intended for development and non-production testing. It should be protected or disabled before exposing an application publicly because it can publish to arbitrary topics configured for the connected broker.

## Enterprise concerns

- Correlation id propagation through Kafka headers.
- Structured logging around producer delivery and middleware handling.
- Options-driven configuration for local, on-prem, IIS-hosted, and cloud-hosted services.
- Pluggable topic resolution, middleware, and failure behavior.
