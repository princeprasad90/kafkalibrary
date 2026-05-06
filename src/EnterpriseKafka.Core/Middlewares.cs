using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.Logging;

namespace EnterpriseKafka.Core;

/// <summary>Middleware that logs each message processing cycle with elapsed time.</summary>
public sealed class LoggingMiddleware(ILogger<LoggingMiddleware> logger) : IKafkaMiddleware
{
    /// <inheritdoc />
    public async Task InvokeAsync(KafkaContext context, Func<Task> next, CancellationToken cancellationToken = default)
    {
        var start = DateTime.UtcNow;
        await next();
        logger.LogInformation(
            "Handled topic={Topic} correlationId={CorrelationId} elapsedMs={Elapsed}",
            context.Topic, context.CorrelationId, (DateTime.UtcNow - start).TotalMilliseconds);
    }
}

/// <summary>Middleware placeholder for telemetry (see EnterpriseKafka.Telemetry for full impl).</summary>
public sealed class TelemetryMiddleware : IKafkaMiddleware
{
    /// <inheritdoc />
    public Task InvokeAsync(KafkaContext context, Func<Task> next, CancellationToken cancellationToken = default)
        => next();
}

/// <summary>Failure strategy that silently ignores all errors.</summary>
public sealed class IgnoreStrategy : IMessageFailureStrategy
{
    /// <inheritdoc />
    public Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}

/// <summary>Failure strategy that re-throws to stop the consumer loop.</summary>
public sealed class StopConsumerStrategy : IMessageFailureStrategy
{
    /// <inheritdoc />
    public Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException(
            $"Consumer stopped due to unhandled exception on topic={context.Topic}.", exception);
}

/// <summary>
/// Failure strategy that publishes the failed message to a dead-letter topic
/// (<c>{original-topic}-dlq</c>) via <see cref="IKafkaProducer"/>.
/// </summary>
public sealed class DeadLetterStrategy(IKafkaProducer producer) : IMessageFailureStrategy
{
    /// <inheritdoc />
    public async Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default)
    {
        var dlqTopic = $"{context.Topic}-dlq";
        var opts = new KafkaPublishOptions(
            Topic: dlqTopic,
            Key: context.Key,
            CorrelationId: context.CorrelationId);

        await producer.PublishAsync(context.Message, opts, cancellationToken);
    }
}

/// <summary>
/// Builds and executes a chain of <see cref="IKafkaMiddleware"/> instances around a terminal action.
/// </summary>
public sealed class MiddlewarePipeline
{
    private readonly IReadOnlyList<IKafkaMiddleware> _middlewares;

    /// <summary>Initialises the pipeline with an ordered list of middleware.</summary>
    public MiddlewarePipeline(IEnumerable<IKafkaMiddleware> middlewares)
        => _middlewares = middlewares.ToList();

    /// <summary>Executes the middleware chain, calling <paramref name="terminal"/> at the end.</summary>
    public Task ExecuteAsync(
        KafkaContext context,
        Func<Task> terminal,
        CancellationToken cancellationToken = default)
    {
        Func<Task> pipeline = terminal;

        // Build the chain from the end backwards so the first middleware executes first.
        for (var i = _middlewares.Count - 1; i >= 0; i--)
        {
            var middleware = _middlewares[i];
            var next = pipeline;
            pipeline = () => middleware.InvokeAsync(context, next, cancellationToken);
        }

        return pipeline();
    }
}
