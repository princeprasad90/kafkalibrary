# Architecture

- `IKafkaProducer` supports single and batch publishing.
- `IKafkaHandler<T>` is the consumer handler contract.
- `IKafkaMiddleware` provides ASP.NET Core style pipeline behavior.
- `IMessageFailureStrategy` is pluggable and avoids hardcoded DLQ-only behavior.
- `ITopicResolver` supports attribute, config mapping, and convention fallback.

## Enterprise concerns
- Correlation id propagation through headers.
- Structured logging and middleware hooks.
- Options-driven configuration for on-prem and IIS-hosted services.
