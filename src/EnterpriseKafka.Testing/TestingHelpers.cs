using System.Collections.Concurrent;
using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;

namespace EnterpriseKafka.Testing;

/// <summary>
/// In-memory <see cref="IKafkaProducer"/> for use in unit and integration tests.
/// Records all published messages without connecting to a real Kafka cluster.
/// </summary>
public sealed class FakeKafkaProducer : IKafkaProducer
{
    private readonly ConcurrentQueue<(string Topic, object Message, KafkaPublishOptions? Options)> _published = new();

    /// <summary>All messages published since the producer was created.</summary>
    public IReadOnlyCollection<(string Topic, object Message, KafkaPublishOptions? Options)> PublishedMessages
        => _published.ToArray();

    /// <inheritdoc />
    public Task PublishAsync<T>(
        T message,
        KafkaPublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var topic = options?.Topic ?? typeof(T).Name.ToLowerInvariant();
        _published.Enqueue((topic, message!, options));
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task PublishBatchAsync<T>(
        IReadOnlyCollection<T> messages,
        KafkaPublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        foreach (var m in messages)
            await PublishAsync(m, options, cancellationToken);
    }

    /// <summary>Returns all published messages of type <typeparamref name="T"/>.</summary>
    public IReadOnlyList<T> SentMessages<T>()
        => _published
            .Where(x => x.Message is T)
            .Select(x => (T)x.Message)
            .ToList();

    /// <summary>Clears all recorded messages.</summary>
    public void Reset() => _published.Clear();
}

/// <summary>
/// In-memory Kafka bus for integration testing. Supports publish/subscribe without a real broker.
/// </summary>
public sealed class InMemoryKafkaBus
{
    private readonly ConcurrentDictionary<string, List<object>> _messages = new();
    private readonly ConcurrentDictionary<string, List<Func<object, CancellationToken, Task>>> _handlers = new();

    /// <summary>Publishes a message to the in-memory bus.</summary>
    public async Task Publish<T>(T message, CancellationToken cancellationToken = default)
    {
        var key = typeof(T).FullName!;
        _messages.GetOrAdd(key, _ => new List<object>()).Add(message!);

        if (_handlers.TryGetValue(key, out var handlers))
        {
            foreach (var handler in handlers)
                await handler(message!, cancellationToken);
        }
    }

    /// <summary>Subscribes a handler to messages of type <typeparamref name="T"/>.</summary>
    public void Subscribe<T>(Func<T, CancellationToken, Task> handler)
    {
        var key = typeof(T).FullName!;
        _handlers.GetOrAdd(key, _ => new List<Func<object, CancellationToken, Task>>())
            .Add((obj, ct) => handler((T)obj, ct));
    }

    /// <summary>Returns all published messages of type <typeparamref name="T"/>.</summary>
    public IReadOnlyList<T> GetMessages<T>()
    {
        var key = typeof(T).FullName!;
        if (_messages.TryGetValue(key, out var list))
            return list.OfType<T>().ToList();
        return Array.Empty<T>();
    }

    /// <summary>Clears all recorded messages.</summary>
    public void Reset() => _messages.Clear();
}

/// <summary>
/// Test harness that wires together a <see cref="FakeKafkaProducer"/> and
/// <see cref="InMemoryKafkaBus"/> for handler integration tests.
/// </summary>
public sealed class KafkaTestHarness
{
    /// <summary>The fake producer used in this harness.</summary>
    public FakeKafkaProducer Producer { get; } = new();

    /// <summary>The in-memory bus used in this harness.</summary>
    public InMemoryKafkaBus Bus { get; } = new();

    /// <summary>Resets both the producer and bus.</summary>
    public void Reset()
    {
        Producer.Reset();
        Bus.Reset();
    }
}

/// <summary>Configurable mock <see cref="ITopicResolver"/> for test scenarios.</summary>
public sealed class MockTopicResolver : ITopicResolver
{
    private readonly Dictionary<Type, string> _mappings = new();

    /// <summary>Configures a static topic name for type <typeparamref name="T"/>.</summary>
    public MockTopicResolver ForType<T>(string topic)
    {
        _mappings[typeof(T)] = topic;
        return this;
    }

    /// <inheritdoc />
    public string ResolveTopic<T>()
        => _mappings.TryGetValue(typeof(T), out var t) ? t : typeof(T).Name.ToLowerInvariant();
}
