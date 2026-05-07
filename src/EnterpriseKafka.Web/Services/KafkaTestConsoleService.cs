using System.Text;
using Confluent.Kafka;
using EnterpriseKafka.Core;
using EnterpriseKafka.Web.Hubs;
using Microsoft.AspNetCore.SignalR;

namespace EnterpriseKafka.Web.Services;

public sealed record KafkaTestMessage(
    DateTimeOffset ReceivedAt,
    string Topic,
    string? Key,
    int Partition,
    long Offset,
    string Payload,
    string? CorrelationId);

public sealed record KafkaConsumerState(bool IsRunning, string? Topic, string? GroupId, string Status);

public sealed class KafkaTestConsoleService(
    EnterpriseKafkaOptions options,
    IHubContext<KafkaMessagesHub> hubContext,
    ILogger<KafkaTestConsoleService> logger) : IHostedService, IDisposable
{
    private const int MaxRecentMessages = 100;
    private readonly object _syncRoot = new();
    private readonly List<KafkaTestMessage> _recentMessages = new();
    private readonly CancellationTokenSource _applicationStopping = new();
    private CancellationTokenSource? _consumerStopping;
    private Task? _consumerTask;
    private KafkaConsumerState _state = new(false, null, null, "Consumer is stopped.");

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _applicationStopping.Cancel();
        await StopConsumerAsync(cancellationToken);
    }

    public KafkaConsumerState CurrentState
    {
        get
        {
            lock (_syncRoot) return _state;
        }
    }

    public IReadOnlyList<KafkaTestMessage> RecentMessages
    {
        get
        {
            lock (_syncRoot) return _recentMessages.ToArray();
        }
    }

    public async Task<KafkaConsumerState> StartConsumerAsync(string topic, string? groupId, bool fromBeginning, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Topic is required.", nameof(topic));
        }

        var normalizedTopic = topic.Trim();
        var normalizedGroupId = string.IsNullOrWhiteSpace(groupId)
            ? $"enterprise-kafka-web-{Environment.MachineName.ToLowerInvariant()}"
            : groupId.Trim();

        await StopConsumerAsync(cancellationToken);

        KafkaConsumerState state;
        CancellationToken token;
        lock (_syncRoot)
        {
            _consumerStopping = CancellationTokenSource.CreateLinkedTokenSource(_applicationStopping.Token);
            token = _consumerStopping.Token;
            _state = new(true, normalizedTopic, normalizedGroupId, $"Consuming from '{normalizedTopic}'.");
            _consumerTask = Task.Run(() => ConsumeAsync(normalizedTopic, normalizedGroupId, fromBeginning, token), token);
            state = _state;
        }

        await hubContext.Clients.All.SendAsync("consumerStateChanged", state, cancellationToken);
        return state;
    }

    public async Task<KafkaConsumerState> StopConsumerAsync(CancellationToken cancellationToken = default)
    {
        Task? consumerTask;
        CancellationTokenSource? consumerStopping;
        KafkaConsumerState state;
        lock (_syncRoot)
        {
            consumerTask = _consumerTask;
            consumerStopping = _consumerStopping;
            _consumerTask = null;
            _consumerStopping = null;
            _state = new(false, null, null, "Consumer is stopped.");
            state = _state;
        }

        consumerStopping?.Cancel();
        if (consumerTask is not null)
        {
            try
            {
                await consumerTask.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
            }
            catch (TimeoutException)
            {
                logger.LogWarning("Timed out while stopping Kafka test consumer.");
            }
        }

        consumerStopping?.Dispose();
        await hubContext.Clients.All.SendAsync("consumerStateChanged", state, cancellationToken);
        return state;
    }

    private async Task ConsumeAsync(string topic, string groupId, bool fromBeginning, CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = fromBeginning ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
            EnableAutoCommit = true
        };

        try
        {
            using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
            consumer.Subscribe(topic);
            await hubContext.Clients.All.SendAsync("consumerStateChanged", CurrentState, cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(cancellationToken);
                if (result?.Message is null)
                {
                    continue;
                }

                var message = new KafkaTestMessage(
                    DateTimeOffset.UtcNow,
                    result.Topic,
                    result.Message.Key,
                    result.Partition.Value,
                    result.Offset.Value,
                    DecodePayload(result.Message.Value),
                    ReadHeader(result.Message.Headers, "x-correlation-id"));

                AddRecentMessage(message);
                await hubContext.Clients.All.SendAsync("messageReceived", message, cancellationToken);
            }

            consumer.Close();
        }
        catch (OperationCanceledException)
        {
        }
        catch (ConsumeException ex)
        {
            logger.LogError(ex, "Kafka consume failed for topic {Topic}", topic);
            var state = UpdateState(new(false, topic, groupId, $"Consumer error: {ex.Error.Reason}"));
            await hubContext.Clients.All.SendAsync("consumerStateChanged", state, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected Kafka test consumer failure for topic {Topic}", topic);
            var state = UpdateState(new(false, topic, groupId, "Consumer stopped because of an unexpected error."));
            await hubContext.Clients.All.SendAsync("consumerStateChanged", state, CancellationToken.None);
        }
    }

    private void AddRecentMessage(KafkaTestMessage message)
    {
        lock (_syncRoot)
        {
            _recentMessages.Insert(0, message);
            if (_recentMessages.Count > MaxRecentMessages)
            {
                _recentMessages.RemoveRange(MaxRecentMessages, _recentMessages.Count - MaxRecentMessages);
            }
        }
    }

    private KafkaConsumerState UpdateState(KafkaConsumerState state)
    {
        lock (_syncRoot)
        {
            _state = state;
            return _state;
        }
    }

    private static string DecodePayload(byte[]? payload) => payload is null ? string.Empty : Encoding.UTF8.GetString(payload);

    private static string? ReadHeader(Headers? headers, string key)
    {
        var header = headers?.LastOrDefault(h => string.Equals(h.Key, key, StringComparison.OrdinalIgnoreCase));
        return header is null ? null : Encoding.UTF8.GetString(header.GetValueBytes());
    }

    public void Dispose()
    {
        _applicationStopping.Cancel();
        _applicationStopping.Dispose();
        _consumerStopping?.Dispose();
    }
}
