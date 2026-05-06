using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace EnterpriseKafka.Retry;

/// <summary>Builds Polly <see cref="ResiliencePipeline"/> instances for Kafka operations.</summary>
public static class KafkaRetryPolicy
{
    /// <summary>Creates an immediate-retry pipeline.</summary>
    public static ResiliencePipeline BuildImmediate(int maxRetries = 3)
        => new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = maxRetries,
                Delay = TimeSpan.Zero,
            })
            .Build();

    /// <summary>Creates a fixed-delay retry pipeline.</summary>
    public static ResiliencePipeline BuildDelayed(int maxRetries = 3, TimeSpan? delay = null)
        => new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = maxRetries,
                Delay = delay ?? TimeSpan.FromMilliseconds(500),
                BackoffType = DelayBackoffType.Constant,
            })
            .Build();

    /// <summary>Creates an exponential-backoff retry pipeline.</summary>
    public static ResiliencePipeline BuildExponential(int maxRetries = 3, TimeSpan? baseDelay = null)
        => new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = maxRetries,
                Delay = baseDelay ?? TimeSpan.FromMilliseconds(250),
                BackoffType = DelayBackoffType.Exponential,
            })
            .Build();

    /// <summary>Creates a pipeline from <see cref="RetryOptions"/>.</summary>
    public static ResiliencePipeline FromOptions(RetryOptions opts)
    {
        if (opts.UseExponentialBackoff)
            return BuildExponential(opts.MaxRetries, TimeSpan.FromMilliseconds(opts.BaseDelayMs));
        return opts.BaseDelayMs == 0
            ? BuildImmediate(opts.MaxRetries)
            : BuildDelayed(opts.MaxRetries, TimeSpan.FromMilliseconds(opts.BaseDelayMs));
    }
}

/// <summary>
/// Middleware that wraps handler execution in a Polly retry pipeline derived from
/// <see cref="EnterpriseKafkaOptions.Retry"/>.
/// </summary>
public sealed class RetryMiddleware(
    EnterpriseKafkaOptions options,
    ILogger<RetryMiddleware> logger) : IKafkaMiddleware
{
    private readonly ResiliencePipeline _pipeline = KafkaRetryPolicy.FromOptions(options.Retry);

    /// <inheritdoc />
    public async Task InvokeAsync(
        KafkaContext context,
        Func<Task> next,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        await _pipeline.ExecuteAsync(async ct =>
        {
            if (attempt++ > 0)
                logger.LogWarning(
                    "Retrying message topic={Topic} correlationId={CorrelationId} attempt={Attempt}",
                    context.Topic, context.CorrelationId, attempt);

            await next();
        }, cancellationToken);
    }
}

/// <summary>DI extension methods for retry support.</summary>
public static class RetryExtensions
{
    /// <summary>Registers <see cref="RetryMiddleware"/> and adds it to the middleware pipeline.</summary>
    public static IServiceCollection AddKafkaRetry(this IServiceCollection services)
    {
        services.AddTransient<RetryMiddleware>();
        var opts = services.GetOrBuildOptions();
        if (opts is not null && !opts.Middlewares.Contains(typeof(RetryMiddleware)))
            opts.Middlewares.Add(typeof(RetryMiddleware));
        return services;
    }

    private static EnterpriseKafkaOptions? GetOrBuildOptions(this IServiceCollection services)
    {
        foreach (var d in services)
        {
            if (d.ServiceType == typeof(EnterpriseKafkaOptions)
                && d.ImplementationInstance is EnterpriseKafkaOptions o)
                return o;
        }
        return null;
    }
}
