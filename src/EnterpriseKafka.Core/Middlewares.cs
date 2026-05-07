using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace EnterpriseKafka.Core;

public static class KafkaTelemetry
{
    public const string ActivitySourceName = "EnterpriseKafka";
    public const string MeterName = "EnterpriseKafka";
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    public static readonly Meter Meter = new(MeterName);
    public static readonly Counter<long> ProcessedMessages = Meter.CreateCounter<long>("kafka.messages.processed");
    public static readonly Counter<long> FailedMessages = Meter.CreateCounter<long>("kafka.messages.failed");
    public static readonly Histogram<double> ProcessingDuration = Meter.CreateHistogram<double>("kafka.processing.duration.ms");
}

public sealed class LoggingMiddleware(ILogger<LoggingMiddleware> logger) : IKafkaMiddleware
{
    public async Task InvokeAsync(KafkaContext context, Func<CancellationToken, Task> next, CancellationToken cancellationToken = default)
    {
        var start = Stopwatch.GetTimestamp();
        using var scope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = context.CorrelationId,
            ["Topic"] = context.Topic,
            ["Partition"] = context.Partition,
            ["Offset"] = context.Offset
        });

        logger.LogInformation("Kafka message handling started topic={Topic} partition={Partition} offset={Offset}", context.Topic, context.Partition, context.Offset);
        await next(cancellationToken);
        logger.LogInformation("Kafka message handled topic={Topic} elapsedMs={Elapsed}", context.Topic, Stopwatch.GetElapsedTime(start).TotalMilliseconds);
    }
}

public sealed class TelemetryMiddleware : IKafkaMiddleware
{
    public async Task InvokeAsync(KafkaContext context, Func<CancellationToken, Task> next, CancellationToken cancellationToken = default)
    {
        using var activity = KafkaTelemetry.ActivitySource.StartActivity("kafka.process", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", context.Topic);
        activity?.SetTag("messaging.kafka.partition", context.Partition);
        activity?.SetTag("messaging.kafka.offset", context.Offset);
        activity?.SetTag("correlation.id", context.CorrelationId);

        var start = Stopwatch.GetTimestamp();
        try
        {
            await next(cancellationToken);
            KafkaTelemetry.ProcessedMessages.Add(1);
        }
        catch
        {
            KafkaTelemetry.FailedMessages.Add(1);
            throw;
        }
        finally
        {
            KafkaTelemetry.ProcessingDuration.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }
    }
}

public sealed class ValidationMiddleware : IKafkaMiddleware
{
    public Task InvokeAsync(KafkaContext context, Func<CancellationToken, Task> next, CancellationToken cancellationToken = default)
    {
        if (context.Message is null) throw new InvalidOperationException("Kafka message payload cannot be null.");
        if (string.IsNullOrWhiteSpace(context.Topic)) throw new InvalidOperationException("Kafka topic is required.");
        return next(cancellationToken);
    }
}

public sealed class KafkaMiddlewarePipeline(IServiceProvider serviceProvider, EnterpriseKafkaOptions options)
{
    public Task ExecuteAsync(KafkaContext context, Func<CancellationToken, Task> terminal, CancellationToken cancellationToken = default)
    {
        Func<CancellationToken, Task> next = terminal;
        foreach (var middlewareType in options.Middlewares.AsEnumerable().Reverse())
        {
            var current = next;
            next = async token =>
            {
                var middleware = (IKafkaMiddleware)serviceProvider.GetRequiredService(middlewareType);
                await middleware.InvokeAsync(context, current, token);
            };
        }

        return next(cancellationToken);
    }
}

public sealed class PollyKafkaRetryEngine(EnterpriseKafkaOptions options, ILogger<PollyKafkaRetryEngine> logger) : IKafkaRetryEngine
{
    public async Task ExecuteAsync(Func<int, CancellationToken, Task> operation, KafkaContext context, CancellationToken cancellationToken = default)
    {
        if (!options.Retry.Enabled || options.Retry.MaxRetries <= 0)
        {
            await operation(0, cancellationToken);
            return;
        }

        var retryOptions = new RetryStrategyOptions
        {
            MaxRetryAttempts = options.Retry.MaxRetries,
            Delay = TimeSpan.FromMilliseconds(options.Retry.BaseDelayMs),
            MaxDelay = TimeSpan.FromMilliseconds(options.Retry.MaxDelayMs),
            BackoffType = options.Retry.UseExponentialBackoff ? DelayBackoffType.Exponential : DelayBackoffType.Constant,
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            OnRetry = args =>
            {
                context.Attempt = args.AttemptNumber + 1;
                logger.LogWarning(args.Outcome.Exception, "Retrying Kafka operation topic={Topic} attempt={Attempt}", context.Topic, context.Attempt);
                return default;
            }
        };

        var pipeline = new ResiliencePipelineBuilder().AddRetry(retryOptions).Build();
        await pipeline.ExecuteAsync(async token => await operation(context.Attempt, token), cancellationToken);
    }
}

public sealed class IgnoreStrategy : IMessageFailureStrategy
{
    public Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default) => Task.CompletedTask;
}

public sealed class AlertOnlyStrategy(ILogger<AlertOnlyStrategy> logger) : IMessageFailureStrategy
{
    public Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default)
    {
        logger.LogError(exception, "Kafka message failed and requires support attention topic={Topic} partition={Partition} offset={Offset} correlationId={CorrelationId}", context.Topic, context.Partition, context.Offset, context.CorrelationId);
        return Task.CompletedTask;
    }
}

public sealed class StopConsumerStrategy : IMessageFailureStrategy
{
    public Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default) => throw new KafkaConsumerStoppedException(context, exception);
}

public sealed class DatabasePersistenceStrategy(ILogger<DatabasePersistenceStrategy> logger) : IMessageFailureStrategy
{
    public Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default)
    {
        logger.LogError(exception, "Persist Kafka failure to enterprise incident store topic={Topic} offset={Offset}", context.Topic, context.Offset);
        return Task.CompletedTask;
    }
}

public sealed class DeadLetterStrategy(IKafkaProducer producer, ITopicResolver topicResolver, ILogger<DeadLetterStrategy> logger) : IMessageFailureStrategy
{
    public async Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default)
    {
        var dlqTopic = topicResolver.ResolveDeadLetterTopic(context.Topic);
        logger.LogError(exception, "Publishing Kafka message to DLQ topic={DlqTopic} sourceTopic={SourceTopic}", dlqTopic, context.Topic);
        await producer.PublishAsync(context.Message, new KafkaPublishOptions(dlqTopic, context.Key, context.Headers.ToDictionary(kv => kv.Key, kv => kv.Value), context.CorrelationId), cancellationToken);
    }
}

public sealed class KafkaConsumerStoppedException(KafkaContext context, Exception innerException)
    : Exception($"Kafka consumer stopped by failure strategy for topic '{context.Topic}' offset '{context.Offset}'.", innerException);

public static class FailureStrategyFactory
{
    public static IMessageFailureStrategy Create(IServiceProvider serviceProvider, EnterpriseKafkaOptions options)
        => options.OnFailure.Strategy switch
        {
            FailureStrategyKind.DeadLetter => ActivatorUtilities.CreateInstance<DeadLetterStrategy>(serviceProvider),
            FailureStrategyKind.DatabasePersistence => ActivatorUtilities.CreateInstance<DatabasePersistenceStrategy>(serviceProvider),
            FailureStrategyKind.AlertOnly => ActivatorUtilities.CreateInstance<AlertOnlyStrategy>(serviceProvider),
            FailureStrategyKind.StopConsumer => ActivatorUtilities.CreateInstance<StopConsumerStrategy>(serviceProvider),
            FailureStrategyKind.Custom when options.OnFailure.CustomStrategyType is not null => (IMessageFailureStrategy)serviceProvider.GetRequiredService(options.OnFailure.CustomStrategyType),
            _ => new IgnoreStrategy()
        };
}
