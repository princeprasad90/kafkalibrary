# EnterpriseKafka

EnterpriseKafka is a reusable .NET 8 Kafka framework built on `Confluent.Kafka` with middleware, standardized producer/consumer abstractions, observability hooks, and a lightweight internal web utility.

## Quick start

```csharp
builder.Services.AddEnterpriseKafka(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.UseMiddleware<LoggingMiddleware>();
});
```

```csharp
await producer.PublishAsync(new OrderCreated { OrderId = "A-100" });
```

## Projects
- `EnterpriseKafka.Abstractions` - contracts and attributes
- `EnterpriseKafka.Core` - producer, topic resolver, serializer, DI
- `EnterpriseKafka.Web` - lightweight internal test utility

See `docs/` for architecture and deployment guidance.
