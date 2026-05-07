# Onboarding

1. Reference the smallest EnterpriseKafka package set needed by the service.
2. Bind `Kafka` configuration from appsettings or secret-backed configuration providers.
3. Register `AddEnterpriseKafka` and enable standard middleware: logging, tracing, retry, and validation.
4. Implement `IKafkaMessageHandler<T>` for each message contract; it inherits from `IKafkaHandler<T>` and is the recommended developer-facing name. Do not place infrastructure code in handlers.
5. Configure topics using `[KafkaTopic]`, `TopicMappings`, or explicit consumer registrations.
6. Select a failure strategy per service. DLQ is recommended for recoverable poison messages, but is not mandatory.
7. Expose health checks and OpenTelemetry metrics/traces in every production microservice.
8. In Kubernetes, set graceful shutdown windows long enough to stop polling, finish in-flight messages, and commit processed offsets.

## Production checklist

- Use stable message keys for ordering-sensitive aggregates.
- Prefer at-least-once processing with idempotent handlers and deduplication keys.
- Enable producer idempotence and `acks=all` for critical event streams.
- Track correlation id, topic, partition, offset, retry count, and processing duration in logs.
- Monitor consumer lag by topic-partition-consumer group and alert on threshold breaches.
- Keep secrets in managed stores such as Azure Key Vault and never in appsettings committed to source control.
