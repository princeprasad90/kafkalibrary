# EnterpriseKafka

EnterpriseKafka is a reusable .NET 8 Kafka framework built on `Confluent.Kafka`. It provides standardized producer and consumer contracts, JSON serialization, topic resolution, middleware hooks, correlation-id propagation, failure strategy extension points, and a lightweight ASP.NET Core test console.

## Projects

- `EnterpriseKafka.Abstractions` - public contracts, context objects, publish options, and topic attributes.
- `EnterpriseKafka.Core` - producer, serializer, topic resolver, dependency injection, middleware, and hosted consumer bootstrap.
- `EnterpriseKafka.Web` - developer/test web application for publishing to Kafka and viewing consumed messages in realtime.
- `tests/UnitTests` - unit tests for core behavior.

## Quick start

Register EnterpriseKafka in an ASP.NET Core or worker-service application:

```csharp
builder.Services.AddEnterpriseKafka(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.Consumer.GroupId = "orders-service";
    options.UseMiddleware<LoggingMiddleware>();
});
```

Publish a message with an explicit topic:

```csharp
await producer.PublishAsync(
    new OrderCreated("A-100", 99.99m),
    new KafkaPublishOptions(Topic: "orders.created", Key: "A-100"));
```

Or decorate a message type and let EnterpriseKafka resolve the topic:

```csharp
[KafkaTopic("orders.created")]
public sealed record OrderCreated(string OrderId, decimal Amount);

await producer.PublishAsync(new OrderCreated("A-100", 99.99m));
```

## Configuration

The main option object is `EnterpriseKafkaOptions`:

| Option | Purpose | Default |
| --- | --- | --- |
| `BootstrapServers` | Kafka bootstrap servers. | `localhost:9092` |
| `Producer.EnableIdempotence` | Enables idempotent producer delivery. | `true` |
| `Consumer.GroupId` | Default consumer group for application consumers. | `enterprise-kafka` |
| `Consumer.Concurrency` | Reserved for consumer scaling. | `1` |
| `Retry.MaxRetries` | Reserved retry policy setting. | `3` |
| `Retry.BaseDelayMs` | Reserved retry delay setting. | `250` |
| `Retry.UseExponentialBackoff` | Reserved retry backoff setting. | `true` |
| `TopicMappings` | Maps .NET message type names to Kafka topic names. | Empty |
| `Middlewares` | Adds Kafka middleware to the handling pipeline. | Empty |

Example using `appsettings.json` with the web app:

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092"
  }
}
```

## Publishing scenarios

### Explicit topic

Use `KafkaPublishOptions.Topic` when a caller needs to choose the topic at runtime, such as a support tool, admin utility, or routing layer.

### Attribute-based topic

Use `[KafkaTopic("topic-name")]` on message classes when each message type has one canonical topic.

### Configuration-based topic mapping

Use `EnterpriseKafkaOptions.TopicMappings` when deployment environments own topic names or when message classes should not reference Kafka topic strings directly:

```csharp
options.TopicMappings[typeof(OrderCreated).FullName!] = "prod.orders.created";
```

### Convention fallback

If no explicit topic, configured mapping, or attribute exists, the default resolver uses the lowercase message type name.

### Batch publishing

`PublishBatchAsync` publishes each message in a collection with the same publish options. Use this for small logical batches where each message should still be delivered independently.

### Headers and correlation ids

`KafkaPublishOptions.CorrelationId` is written to the `x-correlation-id` header. Additional string headers can be supplied through `KafkaPublishOptions.Headers`.

## Consuming scenarios

Implement `IKafkaHandler<T>` in application services to define typed message handling:

```csharp
public sealed class OrderCreatedHandler : IKafkaHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, KafkaContext context, CancellationToken cancellationToken = default)
    {
        // Process message and use context.Topic, context.Key, context.CorrelationId, Partition, and Offset.
        return Task.CompletedTask;
    }
}
```

`KafkaContext` carries topic, key, correlation id, offset, partition, consumer group, the deserialized message, and per-message items for middleware or handlers.

The current core hosted service logs startup and leaves typed consumer registration to application-level extensions. Use the web test console below when you need an interactive smoke test against a Kafka broker.

## Middleware and failure handling

Middleware implements `IKafkaMiddleware` and follows an ASP.NET Core-style `next` delegate. It can measure latency, enrich logs, add tracing, validate messages, or populate `KafkaContext.Items`.

`IMessageFailureStrategy` is the extension point for handler failures. The default `IgnoreStrategy` completes without action; production services can replace it with retry, dead-letter, parking-topic, alerting, or compensating behavior.

## Web test console

Run the sample web application:

```bash
dotnet run --project src/EnterpriseKafka.Web/EnterpriseKafka.Web.csproj
```

Open the displayed local URL. The page can:

1. Publish a JSON payload to any Kafka topic with an optional partition key.
2. Start a test consumer for a topic and consumer group.
3. Choose whether the consumer starts at the latest offset or the earliest offset.
4. Display received Kafka messages in realtime through SignalR.
5. Keep the latest 100 received messages in memory for quick inspection.

Set the broker with configuration:

```bash
dotnet run --project src/EnterpriseKafka.Web/EnterpriseKafka.Web.csproj --Kafka:BootstrapServers=localhost:9092
```

Recommended test flow:

1. Start Kafka locally or point the app at a shared non-production broker.
2. Open the web console.
3. Enter `orders.created` as the receive topic and start the consumer.
4. Publish a JSON payload to the same topic.
5. Confirm the message appears in the realtime messages table.

Use a unique consumer group when you want to read old messages from the beginning. Reusing a group may resume from committed offsets.

## Operational scenarios

- **Local development:** use Docker or a local Kafka broker, `localhost:9092`, and the web console for publish/consume smoke tests.
- **Service-to-service messaging:** use typed records, `[KafkaTopic]` or `TopicMappings`, and one consumer group per service.
- **Environment-specific topics:** use `TopicMappings` so test, staging, and production topic names can differ without code changes.
- **Observability:** add middleware for structured logs, metrics, tracing, and correlation-id propagation.
- **Failure handling:** replace `IMessageFailureStrategy` with retry, dead-letter, or alerting logic that matches the business scenario.
- **IIS or hosted services:** configure `BootstrapServers` and consumer group through application configuration rather than hardcoded values.
- **Testing:** use the unit tests for core behavior and the web console for broker integration checks.

## Validation commands

```bash
dotnet build src/EnterpriseKafka.Web/EnterpriseKafka.Web.csproj --nologo
dotnet test tests/UnitTests/UnitTests.csproj --nologo
```

See `docs/architecture.md` and `docs/onboarding.md` for additional guidance.
