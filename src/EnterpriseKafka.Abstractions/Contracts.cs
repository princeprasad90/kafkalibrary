namespace EnterpriseKafka.Abstractions;

public interface IKafkaProducer
{
    Task<KafkaPublishResult> PublishAsync<T>(T message, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default);
    Task<IReadOnlyCollection<KafkaPublishResult>> PublishBatchAsync<T>(IReadOnlyCollection<T> messages, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default);
}

public interface IKafkaHandler<T>
{
    Task HandleAsync(T message, KafkaContext context, CancellationToken cancellationToken = default);
}

public interface IKafkaMessageHandler<T> : IKafkaHandler<T>;

public interface IMessageSerializer
{
    string ContentType { get; }
    byte[] Serialize<T>(T message);
    T Deserialize<T>(ReadOnlySpan<byte> payload);
}

public interface IKafkaMiddleware
{
    Task InvokeAsync(KafkaContext context, Func<CancellationToken, Task> next, CancellationToken cancellationToken = default);
}

public interface IMessageFailureStrategy
{
    Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default);
}

public interface IKafkaRetryEngine
{
    Task ExecuteAsync(Func<int, CancellationToken, Task> operation, KafkaContext context, CancellationToken cancellationToken = default);
}

public interface ITopicResolver
{
    string ResolveTopic<T>();
    string ResolveTopic(Type messageType);
    string ResolveRetryTopic(string sourceTopic, int attempt);
    string ResolveDeadLetterTopic(string sourceTopic);
}

public interface ITopicProvisioner
{
    Task EnsureTopicsAsync(IEnumerable<KafkaTopicDefinition> topics, CancellationToken cancellationToken = default);
}

public interface IKafkaLagMonitor
{
    Task<KafkaConsumerLag> GetLagAsync(string topic, int partition, string consumerGroup, CancellationToken cancellationToken = default);
}

public sealed record KafkaPublishOptions(
    string? Topic = null,
    string? Key = null,
    IReadOnlyDictionary<string, string>? Headers = null,
    string? CorrelationId = null,
    int? Partition = null,
    DateTimeOffset? Timestamp = null);

public sealed record KafkaPublishResult(
    string Topic,
    int Partition,
    long Offset,
    string CorrelationId,
    KafkaPersistenceStatus PersistenceStatus);

public enum KafkaPersistenceStatus
{
    NotPersisted,
    PossiblyPersisted,
    Persisted
}

public sealed record KafkaTopicDefinition(
    string Name,
    int Partitions = 6,
    short ReplicationFactor = 3,
    TimeSpan? Retention = null,
    bool IsRetryTopic = false,
    bool IsDeadLetterTopic = false);

public sealed record KafkaConsumerLag(string Topic, int Partition, long CurrentOffset, long EndOffset, long Lag);

public sealed class KafkaContext
{
    public required string Topic { get; init; }
    public string? Key { get; init; }
    public string CorrelationId { get; set; } = Guid.NewGuid().ToString("N");
    public long? Offset { get; set; }
    public int? Partition { get; set; }
    public string? ConsumerGroup { get; set; }
    public int Attempt { get; set; }
    public DateTimeOffset ReceivedAt { get; init; } = DateTimeOffset.UtcNow;
    public object Message { get; init; } = default!;
    public IDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, object> Items { get; } = new();
}

[AttributeUsage(AttributeTargets.Class)]
public sealed class KafkaTopicAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

[AttributeUsage(AttributeTargets.Class)]
public sealed class KafkaConsumerAttribute(string topic, string? groupId = null) : Attribute
{
    public string Topic { get; } = topic;
    public string? GroupId { get; } = groupId;
}
