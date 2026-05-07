# EnterpriseKafka

EnterpriseKafka is a reusable .NET 8 Kafka framework built on `Confluent.Kafka` with standardized producer/consumer abstractions, ASP.NET Core-style middleware, Polly retry policies, OpenTelemetry-friendly diagnostics, and pluggable failure handling.

## Quick start

```csharp
builder.Services.AddEnterpriseKafka(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.TopicNaming.Environment = builder.Environment.EnvironmentName.ToLowerInvariant();
    options.UseLogging()
           .UseTracing()
           .UseRetry()
           .UseValidation()
           .AddConsumer<OrderCreated, OrderCreatedHandler>("orders.created", "orders-service");

    options.OnFailure.UseDeadLetterQueue(); // optional; Ignore, AlertOnly, StopConsumer, DatabasePersistence and custom strategies are supported
});
```

```csharp
await producer.PublishAsync(
    new OrderCreated("A-100"),
    new KafkaPublishOptions(Key: "A-100", Headers: new Dictionary<string, string> { ["source"] = "checkout" }));
```

```csharp
public sealed class OrderCreatedHandler : IKafkaMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, KafkaContext context, CancellationToken cancellationToken)
    {
        // business logic only
        return Task.CompletedTask;
    }
}
```

## Projects
- `EnterpriseKafka.Abstractions` - contracts and attributes
- `EnterpriseKafka.Core` - producer, consumer host, middleware pipeline, retry engine, topic resolver, serializer, DI
- `EnterpriseKafka.Producer` / `EnterpriseKafka.Consumer` - package split points for producer- and consumer-only deployments
- `EnterpriseKafka.Retry`, `EnterpriseKafka.Logging`, `EnterpriseKafka.Telemetry`, `EnterpriseKafka.HealthChecks`, `EnterpriseKafka.Testing` - modular extension package boundaries
- `EnterpriseKafka.Web` - lightweight internal test utility

See `docs/` for architecture and deployment guidance.
