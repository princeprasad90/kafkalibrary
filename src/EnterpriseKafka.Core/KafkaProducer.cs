using Confluent.Kafka;
using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Text;

namespace EnterpriseKafka.Core;

public sealed class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IMessageSerializer _serializer;
    private readonly ITopicResolver _topicResolver;
    private readonly ILogger<KafkaProducer> _logger;
    private readonly IKafkaRetryEngine _retryEngine;
    private readonly IProducer<string?, byte[]> _producer;

    public KafkaProducer(EnterpriseKafkaOptions options, IMessageSerializer serializer, ITopicResolver topicResolver, IKafkaRetryEngine retryEngine, ILogger<KafkaProducer> logger)
    {
        _serializer = serializer;
        _topicResolver = topicResolver;
        _retryEngine = retryEngine;
        _logger = logger;
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = options.Producer.EnableIdempotence,
            Acks = Enum.Parse<Acks>(options.Producer.Acks, true),
            LingerMs = options.Producer.LingerMs,
            BatchSize = options.Producer.BatchSizeBytes,
            ClientId = options.Producer.ClientId
        };

        ApplySecurity(config, options.Security);
        _producer = new ProducerBuilder<string?, byte[]>(config).Build();
    }

    public async Task<KafkaPublishResult> PublishAsync<T>(T message, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        var topic = options?.Topic ?? _topicResolver.ResolveTopic<T>();
        var correlationId = options?.CorrelationId ?? Guid.NewGuid().ToString("N");
        var context = new KafkaContext { Topic = topic, Key = options?.Key, CorrelationId = correlationId, Message = message! };
        DeliveryResult<string?, byte[]>? delivery = null;

        await _retryEngine.ExecuteAsync(async (_, token) =>
        {
            using var activity = KafkaTelemetry.ActivitySource.StartActivity("kafka.publish", ActivityKind.Producer);
            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.destination.name", topic);
            activity?.SetTag("correlation.id", correlationId);

            var kafkaMessage = new Message<string?, byte[]>
            {
                Key = options?.Key,
                Value = _serializer.Serialize(message),
                Headers = BuildHeaders(correlationId, _serializer.ContentType, options?.Headers),
                Timestamp = options?.Timestamp is null ? Timestamp.Default : new Timestamp(options.Timestamp.Value.UtcDateTime)
            };

            delivery = options?.Partition is int partition
                ? await _producer.ProduceAsync(new TopicPartition(topic, new Partition(partition)), kafkaMessage, token)
                : await _producer.ProduceAsync(topic, kafkaMessage, token);
        }, context, cancellationToken);

        var status = delivery!.Status switch
        {
            PersistenceStatus.Persisted => KafkaPersistenceStatus.Persisted,
            PersistenceStatus.PossiblyPersisted => KafkaPersistenceStatus.PossiblyPersisted,
            _ => KafkaPersistenceStatus.NotPersisted
        };

        _logger.LogInformation("Produced topic={Topic} partition={Partition} offset={Offset} correlationId={CorrelationId}", topic, delivery.Partition.Value, delivery.Offset.Value, correlationId);
        return new KafkaPublishResult(topic, delivery.Partition.Value, delivery.Offset.Value, correlationId, status);
    }

    public async Task<IReadOnlyCollection<KafkaPublishResult>> PublishBatchAsync<T>(IReadOnlyCollection<T> messages, KafkaPublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        var results = new List<KafkaPublishResult>(messages.Count);
        foreach (var message in messages) results.Add(await PublishAsync(message, options, cancellationToken));
        return results;
    }

    public void Dispose() => _producer.Dispose();

    private static Headers BuildHeaders(string correlationId, string contentType, IReadOnlyDictionary<string, string>? source)
    {
        var headers = new Headers
        {
            { "x-correlation-id", Encoding.UTF8.GetBytes(correlationId) },
            { "content-type", Encoding.UTF8.GetBytes(contentType) },
            { "traceparent", Encoding.UTF8.GetBytes(Activity.Current?.Id ?? string.Empty) }
        };

        if (source is not null)
        {
            foreach (var kv in source)
            {
                headers.Remove(kv.Key);
                headers.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));
            }
        }

        return headers;
    }

    private static void ApplySecurity(ProducerConfig config, SecurityOptions security)
    {
        if (Enum.TryParse<SecurityProtocol>(security.SecurityProtocol, true, out var protocol)) config.SecurityProtocol = protocol;
        if (Enum.TryParse<SaslMechanism>(security.SaslMechanism, true, out var mechanism)) config.SaslMechanism = mechanism;
        config.SaslUsername = security.SaslUsername;
        config.SaslPassword = security.SaslPassword;
        config.SslCaLocation = security.SslCaLocation;
    }
}
