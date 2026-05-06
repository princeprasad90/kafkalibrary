namespace EnterpriseKafka.Abstractions;

public interface IKafkaProducer
{
    Task PublishAsync<T>(T message, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default);
    Task PublishBatchAsync<T>(IReadOnlyCollection<T> messages, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default);
}

public interface IKafkaHandler<T>
{
    Task HandleAsync(T message, KafkaContext context, CancellationToken cancellationToken = default);
}

public interface IMessageSerializer
{
    byte[] Serialize<T>(T message);
    T Deserialize<T>(ReadOnlySpan<byte> payload);
}

public interface IKafkaMiddleware
{
    Task InvokeAsync(KafkaContext context, Func<Task> next, CancellationToken cancellationToken = default);
}

public interface IMessageFailureStrategy
{
    Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default);
}

public interface ITopicResolver
{
    string ResolveTopic<T>();
}

public sealed record KafkaPublishOptions(
    string? Topic = null,
    string? Key = null,
    IReadOnlyDictionary<string, string>? Headers = null,
    string? CorrelationId = null);

public sealed class KafkaContext
{
    public required string Topic { get; init; }
    public string? Key { get; init; }
    public string CorrelationId { get; set; } = Guid.NewGuid().ToString("N");
    public long? Offset { get; set; }
    public int? Partition { get; set; }
    public string? ConsumerGroup { get; set; }
    public object Message { get; init; } = default!;
    public Dictionary<string, object> Items { get; } = new();
}

[AttributeUsage(AttributeTargets.Class)]
public sealed class KafkaTopicAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}
