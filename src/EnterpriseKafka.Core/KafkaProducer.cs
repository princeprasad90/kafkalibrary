using Confluent.Kafka;
using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.Logging;

namespace EnterpriseKafka.Core;

public sealed class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IMessageSerializer _serializer;
    private readonly ITopicResolver _topicResolver;
    private readonly ILogger<KafkaProducer> _logger;
    private readonly IProducer<string, byte[]> _producer;

    public KafkaProducer(EnterpriseKafkaOptions options, IMessageSerializer serializer, ITopicResolver topicResolver, ILogger<KafkaProducer> logger)
    {
        _serializer = serializer;
        _topicResolver = topicResolver;
        _logger = logger;
        var config = new ProducerConfig { BootstrapServers = options.BootstrapServers, EnableIdempotence = options.Producer.EnableIdempotence };
        _producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    public async Task PublishAsync<T>(T message, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        var topic = options?.Topic ?? _topicResolver.ResolveTopic<T>();
        var correlationId = options?.CorrelationId ?? Guid.NewGuid().ToString("N");
        var headers = new Headers { { "x-correlation-id", System.Text.Encoding.UTF8.GetBytes(correlationId) } };
        if (options?.Headers is not null) foreach (var kv in options.Headers) headers.Add(kv.Key, System.Text.Encoding.UTF8.GetBytes(kv.Value));
        var delivery = await _producer.ProduceAsync(topic, new Message<string, byte[]> { Key = options?.Key, Value = _serializer.Serialize(message), Headers = headers }, cancellationToken);
        _logger.LogInformation("Produced topic={Topic} partition={Partition} offset={Offset} correlationId={CorrelationId}", topic, delivery.Partition, delivery.Offset, correlationId);
    }

    public async Task PublishBatchAsync<T>(IReadOnlyCollection<T> messages, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages) await PublishAsync(message, options, cancellationToken);
    }

    public void Dispose() => _producer.Dispose();
}
