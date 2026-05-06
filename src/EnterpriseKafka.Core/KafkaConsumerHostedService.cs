using Confluent.Kafka;
using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EnterpriseKafka.Core;

/// <summary>
/// Hosted service that starts a consumer loop for each <see cref="ConsumerRegistration"/>
/// configured in <see cref="EnterpriseKafkaOptions.Consumers"/>.
/// </summary>
public sealed class KafkaConsumerHostedService : BackgroundService
{
    private readonly EnterpriseKafkaOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<KafkaConsumerHostedService> _logger;
    private readonly IMessageSerializer _serializer;

    /// <summary>Initializes the hosted service.</summary>
    public KafkaConsumerHostedService(
        EnterpriseKafkaOptions options,
        IServiceScopeFactory scopeFactory,
        ILogger<KafkaConsumerHostedService> logger,
        IMessageSerializer serializer)
    {
        _options = options;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _serializer = serializer;
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_options.Consumers.Count == 0)
        {
            _logger.LogInformation(
                "Kafka consumer hosted service started. No consumers registered.");
            return Task.CompletedTask;
        }

        _logger.LogInformation(
            "Starting {Count} Kafka consumer loop(s).", _options.Consumers.Count);

        // Fire-and-forget each consumer loop — they respect stoppingToken.
        var tasks = _options.Consumers
            .Select(reg => Task.Run(() => RunConsumerLoopAsync(reg, stoppingToken), stoppingToken))
            .ToArray();

        // Return a task that completes when all loops finish (on cancellation).
        return Task.WhenAll(tasks);
    }

    private async Task RunConsumerLoopAsync(
        ConsumerRegistration registration,
        CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = registration.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) =>
                _logger.LogError("Kafka consumer error: {Reason} (isFatal={Fatal})", e.Reason, e.IsFatal))
            .Build();

        consumer.Subscribe(registration.Topic);
        _logger.LogInformation(
            "Subscribed to topic={Topic} groupId={GroupId}", registration.Topic, registration.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result = null;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (result is null) continue;

                    await ProcessMessageAsync(registration, result, stoppingToken);
                    consumer.Commit(result);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Unhandled error consuming topic={Topic} partition={Partition} offset={Offset}",
                        result?.Topic, result?.Partition.Value, result?.Offset.Value);

                    if (result is not null)
                    {
                        await TryHandleFailureAsync(registration, result, ex, stoppingToken);
                    }
                }
            }
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation(
                "Consumer loop stopped for topic={Topic}", registration.Topic);
        }
    }

    private async Task ProcessMessageAsync(
        ConsumerRegistration registration,
        ConsumeResult<string, byte[]> result,
        CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();

        // Deserialize using reflection so we handle the open generic at runtime.
        var message = DeserializeWithType(_serializer, registration.MessageType, result.Message.Value);

        // Extract correlation-id from headers.
        var correlationId = Guid.NewGuid().ToString("N");
        if (result.Message.Headers.TryGetLastBytes("x-correlation-id", out var cidBytes))
            correlationId = System.Text.Encoding.UTF8.GetString(cidBytes);

        var context = new KafkaContext
        {
            Topic = result.Topic,
            Key = result.Message.Key,
            Offset = result.Offset.Value,
            Partition = result.Partition.Value,
            ConsumerGroup = registration.GroupId,
            Message = message,
            CorrelationId = correlationId,
        };

        // Build and execute the middleware pipeline.
        var middlewares = BuildMiddlewares(scope.ServiceProvider, _options.Middlewares);
        var pipeline = new MiddlewarePipeline(middlewares);

        await pipeline.ExecuteAsync(context, async () =>
        {
            // Resolve and invoke the typed handler.
            var handler = scope.ServiceProvider.GetRequiredService(registration.HandlerType);
            var handleMethod = registration.HandlerType
                .GetMethod(nameof(IKafkaHandler<object>.HandleAsync))!;

            var task = (Task)handleMethod.Invoke(handler, [message, context, cancellationToken])!;
            await task;
        }, cancellationToken);
    }

    private async Task TryHandleFailureAsync(
        ConsumerRegistration registration,
        ConsumeResult<string, byte[]> result,
        Exception exception,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();

            IMessageFailureStrategy? strategy = null;
            if (registration.FailureStrategy is not null)
                strategy = scope.ServiceProvider.GetService(registration.FailureStrategy) as IMessageFailureStrategy;
            strategy ??= scope.ServiceProvider.GetService<IMessageFailureStrategy>();

            if (strategy is null) return;

            var context = new KafkaContext
            {
                Topic = result.Topic,
                Key = result.Message.Key,
                Offset = result.Offset.Value,
                Partition = result.Partition.Value,
                ConsumerGroup = registration.GroupId,
                Message = result.Message.Value,
            };

            await strategy.HandleFailureAsync(context, exception, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failure strategy threw an exception.");
        }
    }

    private static IEnumerable<IKafkaMiddleware> BuildMiddlewares(
        IServiceProvider sp,
        IEnumerable<Type> middlewareTypes)
    {
        foreach (var type in middlewareTypes)
        {
            if (sp.GetService(type) is IKafkaMiddleware mw)
                yield return mw;
        }
    }

    // Static non-async helper avoids C# 12 restriction on ref structs in async methods.
    private static object DeserializeWithType(
        IMessageSerializer serializer,
        Type messageType,
        byte[] bytes)
    {
        var method = typeof(KafkaConsumerHostedService)
            .GetMethod(nameof(InvokeDeserialize),
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(messageType);

        return method.Invoke(null, [serializer, bytes])!;
    }

    private static T InvokeDeserialize<T>(IMessageSerializer serializer, byte[] bytes)
        => serializer.Deserialize<T>(bytes); // byte[] implicitly converts to ReadOnlySpan<byte>
}
