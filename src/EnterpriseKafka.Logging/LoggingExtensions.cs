using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Context;

namespace EnterpriseKafka.Logging;

/// <summary>Provides helper methods for enriching Serilog log context with Kafka properties.</summary>
public static class KafkaLogContext
{
    /// <summary>
    /// Pushes standard Kafka properties onto the Serilog <see cref="LogContext"/> for the
    /// duration of the returned disposable.
    /// </summary>
    public static IDisposable Push(KafkaContext context, int? retryCount = null, int? payloadSize = null)
    {
        var disposables = new List<IDisposable>
        {
            LogContext.PushProperty("KafkaTopic", context.Topic),
            LogContext.PushProperty("KafkaPartition", context.Partition),
            LogContext.PushProperty("KafkaOffset", context.Offset),
            LogContext.PushProperty("KafkaCorrelationId", context.CorrelationId),
            LogContext.PushProperty("KafkaConsumerGroup", context.ConsumerGroup),
        };

        if (retryCount.HasValue)
            disposables.Add(LogContext.PushProperty("KafkaRetryCount", retryCount.Value));
        if (payloadSize.HasValue)
            disposables.Add(LogContext.PushProperty("KafkaPayloadSize", payloadSize.Value));

        return new CompositeDisposable(disposables);
    }

    private sealed class CompositeDisposable(IEnumerable<IDisposable> items) : IDisposable
    {
        private readonly IReadOnlyList<IDisposable> _items = items.ToList();
        public void Dispose() { foreach (var d in _items) d.Dispose(); }
    }
}

/// <summary>
/// Middleware that enriches the Serilog <see cref="LogContext"/> with Kafka metadata for every
/// message processed in the pipeline.
/// </summary>
public sealed class SerilogKafkaMiddleware : IKafkaMiddleware
{
    /// <inheritdoc />
    public async Task InvokeAsync(
        KafkaContext context,
        Func<Task> next,
        CancellationToken cancellationToken = default)
    {
        using (KafkaLogContext.Push(context))
        {
            await next();
        }
    }
}

/// <summary>DI extension methods for Kafka logging support.</summary>
public static class LoggingExtensions
{
    /// <summary>Registers <see cref="SerilogKafkaMiddleware"/> with the DI container.</summary>
    public static IServiceCollection AddKafkaLogging(this IServiceCollection services)
    {
        services.AddTransient<SerilogKafkaMiddleware>();
        return services;
    }

    /// <summary>
    /// Configures the <see cref="IHostBuilder"/> to use Serilog and enriches log context with
    /// Kafka-specific properties via <see cref="SerilogKafkaMiddleware"/>.
    /// </summary>
    public static IHostBuilder UseKafkaSerilog(
        this IHostBuilder hostBuilder,
        Action<LoggerConfiguration>? configure = null)
    {
        return hostBuilder.UseSerilog((ctx, loggerConfig) =>
        {
            loggerConfig
                .Enrich.FromLogContext()
                .WriteTo.Console();

            configure?.Invoke(loggerConfig);
        });
    }
}
