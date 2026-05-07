# Onboarding

Use this checklist when adding EnterpriseKafka to a new .NET service.

## 1. Add references

Reference the abstractions project from code that only needs contracts. Reference the core project from the application host that configures Kafka.

## 2. Configure services

Register EnterpriseKafka during application startup:

```csharp
builder.Services.AddEnterpriseKafka(options =>
{
    options.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
    options.Consumer.GroupId = "my-service";
    options.UseMiddleware<LoggingMiddleware>();
});
```

## 3. Define messages

Prefer immutable records for messages. Add `[KafkaTopic]` when the topic is stable and owned by code:

```csharp
[KafkaTopic("orders.created")]
public sealed record OrderCreated(string OrderId, decimal Amount);
```

Use `TopicMappings` when the environment owns topic names:

```csharp
options.TopicMappings[typeof(OrderCreated).FullName!] = builder.Configuration["Kafka:Topics:OrderCreated"]!;
```

## 4. Publish messages

Inject `IKafkaProducer` and publish with explicit options when you need a key, custom headers, or a runtime topic:

```csharp
await producer.PublishAsync(
    orderCreated,
    new KafkaPublishOptions(
        Topic: "orders.created",
        Key: orderCreated.OrderId,
        Headers: new Dictionary<string, string> { ["source"] = "orders-api" }));
```

For multiple messages, call `PublishBatchAsync` with a collection.

## 5. Implement handlers

Create an `IKafkaHandler<T>` for each consumed message type:

```csharp
public sealed class OrderCreatedHandler : IKafkaHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, KafkaContext context, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}
```

Use `KafkaContext` for metadata such as topic, key, partition, offset, group, and correlation id.

## 6. Choose failure behavior

Replace `IMessageFailureStrategy` if the default ignore behavior is not appropriate. Common strategies include retry, dead-letter topic, parking topic, alerting, and manual replay.

## 7. Add middleware

Add middleware for logging, metrics, tracing, validation, or context enrichment. Register it with `options.UseMiddleware<T>()` and DI.

## 8. Test with the web console

Run the sample web application:

```bash
dotnet run --project src/EnterpriseKafka.Web/EnterpriseKafka.Web.csproj --Kafka:BootstrapServers=localhost:9092
```

Use the page to publish a JSON message, start a consumer on the same topic, and verify realtime delivery. Use a unique consumer group and the `Earliest` option when you need to replay existing test messages.

## 9. Validate before release

Run the repository validation commands:

```bash
dotnet build src/EnterpriseKafka.Web/EnterpriseKafka.Web.csproj --nologo
dotnet test tests/UnitTests/UnitTests.csproj --nologo
```

## Scenario guide

- **Local smoke test:** run Kafka locally, run the web console, publish and consume on one topic.
- **New producer:** define the message, configure topic resolution, inject `IKafkaProducer`, publish with a key and correlation id.
- **New consumer:** define `IKafkaHandler<T>`, choose a consumer group, configure topic subscription in the hosting application, and decide failure behavior.
- **Environment-specific topics:** keep code stable and use `TopicMappings` from configuration.
- **Debugging missing messages:** verify bootstrap servers, topic name, consumer group offsets, `Earliest` vs `Latest`, and broker connectivity.
- **Operational readiness:** add logging middleware, metrics/tracing middleware, and a production failure strategy before enabling critical workflows.
