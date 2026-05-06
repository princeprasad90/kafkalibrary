using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.Logging;

namespace EnterpriseKafka.Core;

public sealed class LoggingMiddleware(ILogger<LoggingMiddleware> logger) : IKafkaMiddleware
{
    public async Task InvokeAsync(KafkaContext context, Func<Task> next, CancellationToken cancellationToken = default)
    {
        var start = DateTime.UtcNow;
        await next();
        logger.LogInformation("Handled topic={Topic} correlationId={CorrelationId} elapsedMs={Elapsed}", context.Topic, context.CorrelationId, (DateTime.UtcNow - start).TotalMilliseconds);
    }
}

public sealed class TelemetryMiddleware : IKafkaMiddleware
{
    public Task InvokeAsync(KafkaContext context, Func<Task> next, CancellationToken cancellationToken = default) => next();
}

public sealed class IgnoreStrategy : IMessageFailureStrategy
{
    public Task HandleFailureAsync(KafkaContext context, Exception exception, CancellationToken cancellationToken = default) => Task.CompletedTask;
}
