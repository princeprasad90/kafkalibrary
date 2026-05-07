using Confluent.Kafka;
using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text;

namespace EnterpriseKafka.Core;

public sealed class KafkaConsumerHostedService(
    EnterpriseKafkaOptions options,
    ITopicResolver topicResolver,
    IMessageSerializer serializer,
    IKafkaRetryEngine retryEngine,
    IMessageFailureStrategy failureStrategy,
    IServiceScopeFactory scopeFactory,
    ILogger<KafkaConsumerHostedService> logger) : BackgroundService
{
    private static readonly ConcurrentDictionary<Type, Func<KafkaConsumerHostedService, byte[], object>> Deserializers = new();
    private static readonly ConcurrentDictionary<Type, Func<object, object, KafkaContext, CancellationToken, Task>> HandlerInvokers = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.Consumers.Count == 0)
        {
            logger.LogInformation("Kafka consumer hosted service started with no registered consumers.");
            return;
        }

        var workers = options.Consumers.Select(registration => Task.Run(() => RunConsumerAsync(registration, stoppingToken), stoppingToken));
        await Task.WhenAll(workers);
    }

    private async Task RunConsumerAsync(ConsumerRegistration registration, CancellationToken stoppingToken)
    {
        var topic = registration.Topic ?? topicResolver.ResolveTopic(registration.MessageType);
        var groupId = registration.GroupId ?? options.Consumer.GroupId;
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = options.Consumer.EnableAutoCommit,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            MaxPollIntervalMs = options.Consumer.MaxPollIntervalMs
        };

        ApplySecurity(config, options.Security);

        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(topic);
        logger.LogInformation("Kafka consumer subscribed topic={Topic} groupId={GroupId} handler={Handler}", topic, groupId, registration.HandlerType.Name);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, byte[]>? result = null;
            try
            {
                result = consumer.Consume(stoppingToken);
                if (result?.Message is null) continue;

                await ProcessMessageAsync(registration, topic, groupId, result, stoppingToken);

                if (options.Consumer.CommitAfterSuccessfulProcessing)
                {
                    consumer.StoreOffset(result);
                    consumer.Commit(result);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (KafkaConsumerStoppedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Kafka consumer polling failed topic={Topic} groupId={GroupId}", topic, groupId);
                if (result is null) await Task.Delay(options.Consumer.PollErrorDelay, stoppingToken);
            }
        }

        consumer.Close();
    }

    private async Task ProcessMessageAsync(ConsumerRegistration registration, string topic, string groupId, ConsumeResult<string, byte[]> result, CancellationToken cancellationToken)
    {
        var payload = Deserialize(registration.MessageType, result.Message.Value);
        var context = new KafkaContext
        {
            Topic = topic,
            Key = result.Message.Key,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value,
            ConsumerGroup = groupId,
            Message = payload,
            CorrelationId = ReadHeader(result.Message.Headers, "x-correlation-id") ?? Guid.NewGuid().ToString("N"),
            Headers = ReadHeaders(result.Message.Headers)
        };

        try
        {
            await retryEngine.ExecuteAsync(async (_, token) =>
            {
                using var scope = scopeFactory.CreateScope();
                var pipeline = new KafkaMiddlewarePipeline(scope.ServiceProvider, options);
                var handler = scope.ServiceProvider.GetRequiredService(registration.HandlerType);
                await pipeline.ExecuteAsync(context, ct => InvokeHandlerAsync(handler, registration.MessageType, payload, context, ct), token);
            }, context, cancellationToken);
        }
        catch (Exception ex)
        {
            await failureStrategy.HandleFailureAsync(context, ex, cancellationToken);
        }
    }

    private object Deserialize(Type messageType, byte[] payload)
        => Deserializers.GetOrAdd(messageType, BuildDeserializer)(this, payload);

    private T DeserializeTyped<T>(byte[] payload) => serializer.Deserialize<T>(payload);

    private static Task InvokeHandlerAsync(object handler, Type messageType, object payload, KafkaContext context, CancellationToken cancellationToken)
    {
        var invoker = HandlerInvokers.GetOrAdd(messageType, BuildHandlerInvoker);
        return invoker(handler, payload, context, cancellationToken);
    }

    private static Func<KafkaConsumerHostedService, byte[], object> BuildDeserializer(Type messageType)
    {
        var service = Expression.Parameter(typeof(KafkaConsumerHostedService), "service");
        var payload = Expression.Parameter(typeof(byte[]), "payload");
        var method = typeof(KafkaConsumerHostedService)
            .GetMethod(nameof(DeserializeTyped), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .MakeGenericMethod(messageType);
        var call = Expression.Call(service, method, payload);
        return Expression.Lambda<Func<KafkaConsumerHostedService, byte[], object>>(Expression.Convert(call, typeof(object)), service, payload).Compile();
    }

    private static Func<object, object, KafkaContext, CancellationToken, Task> BuildHandlerInvoker(Type messageType)
    {
        var handler = Expression.Parameter(typeof(object), "handler");
        var payload = Expression.Parameter(typeof(object), "payload");
        var context = Expression.Parameter(typeof(KafkaContext), "context");
        var cancellationToken = Expression.Parameter(typeof(CancellationToken), "cancellationToken");
        var contract = typeof(IKafkaHandler<>).MakeGenericType(messageType);
        var method = contract.GetMethod(nameof(IKafkaHandler<object>.HandleAsync))!;
        var call = Expression.Call(Expression.Convert(handler, contract), method, Expression.Convert(payload, messageType), context, cancellationToken);
        return Expression.Lambda<Func<object, object, KafkaContext, CancellationToken, Task>>(call, handler, payload, context, cancellationToken).Compile();
    }

    private static string? ReadHeader(Headers headers, string key)
    {
        var value = headers.LastOrDefault(h => string.Equals(h.Key, key, StringComparison.OrdinalIgnoreCase));
        return value is null ? null : Encoding.UTF8.GetString(value.GetValueBytes());
    }

    private static IDictionary<string, string> ReadHeaders(Headers headers)
        => headers
            .GroupBy(h => h.Key, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => Encoding.UTF8.GetString(group.Last().GetValueBytes()), StringComparer.OrdinalIgnoreCase);

    private static void ApplySecurity(ConsumerConfig config, SecurityOptions security)
    {
        if (Enum.TryParse<SecurityProtocol>(security.SecurityProtocol, true, out var protocol)) config.SecurityProtocol = protocol;
        if (Enum.TryParse<SaslMechanism>(security.SaslMechanism, true, out var mechanism)) config.SaslMechanism = mechanism;
        config.SaslUsername = security.SaslUsername;
        config.SaslPassword = security.SaslPassword;
        config.SslCaLocation = security.SslCaLocation;
    }
}
