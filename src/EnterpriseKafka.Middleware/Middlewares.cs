using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EnterpriseKafka.Middleware;

/// <summary>
/// Ensures an <c>x-correlation-id</c> is present in the context and stores it in
/// <see cref="KafkaContext.Items"/>.
/// </summary>
public sealed class CorrelationMiddleware : IKafkaMiddleware
{
    /// <inheritdoc />
    public Task InvokeAsync(
        KafkaContext context,
        Func<Task> next,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(context.CorrelationId))
            context.CorrelationId = Guid.NewGuid().ToString("N");

        context.Items["x-correlation-id"] = context.CorrelationId;
        return next();
    }
}

/// <summary>
/// Validates that the message is not null and the topic name is not empty before
/// passing control to the next middleware.
/// </summary>
public sealed class ValidationMiddleware : IKafkaMiddleware
{
    /// <inheritdoc />
    public Task InvokeAsync(
        KafkaContext context,
        Func<Task> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Message is null)
            throw new InvalidOperationException(
                $"Kafka message is null for topic={context.Topic}.");

        if (string.IsNullOrWhiteSpace(context.Topic))
            throw new InvalidOperationException("Kafka context has an empty topic.");

        return next();
    }
}

/// <summary>
/// Catches unhandled exceptions from downstream middleware/handlers, logs them,
/// and delegates to <see cref="IMessageFailureStrategy"/> if available.
/// </summary>
public sealed class ExceptionHandlingMiddleware(
    IMessageFailureStrategy? failureStrategy,
    ILogger<ExceptionHandlingMiddleware> logger) : IKafkaMiddleware
{
    /// <inheritdoc />
    public async Task InvokeAsync(
        KafkaContext context,
        Func<Task> next,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await next();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex,
                "Unhandled exception processing topic={Topic} correlationId={CorrelationId}",
                context.Topic, context.CorrelationId);

            if (failureStrategy is not null)
                await failureStrategy.HandleFailureAsync(context, ex, cancellationToken);
        }
    }
}

/// <summary>DI extension methods for the middleware package.</summary>
public static class MiddlewareExtensions
{
    /// <summary>
    /// Registers <see cref="CorrelationMiddleware"/>, <see cref="ValidationMiddleware"/>,
    /// and <see cref="ExceptionHandlingMiddleware"/> with the DI container.
    /// </summary>
    public static IServiceCollection AddKafkaMiddleware(this IServiceCollection services)
    {
        services.AddTransient<CorrelationMiddleware>();
        services.AddTransient<ValidationMiddleware>();
        services.AddTransient<ExceptionHandlingMiddleware>();
        return services;
    }
}
