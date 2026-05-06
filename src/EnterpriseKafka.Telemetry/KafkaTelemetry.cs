using System.Diagnostics;
using System.Diagnostics.Metrics;
using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace EnterpriseKafka.Telemetry;

/// <summary>Static <see cref="ActivitySource"/> for EnterpriseKafka distributed tracing.</summary>
public static class KafkaTelemetry
{
    /// <summary>ActivitySource name used for all Kafka spans.</summary>
    public const string ActivitySourceName = "EnterpriseKafka";

    /// <summary>Version reported on activities.</summary>
    public const string ActivitySourceVersion = "1.0.0";

    /// <summary>The shared <see cref="ActivitySource"/> instance.</summary>
    public static readonly ActivitySource Source =
        new(ActivitySourceName, ActivitySourceVersion);
}

/// <summary>OTel metrics for the EnterpriseKafka framework.</summary>
public static class KafkaMetrics
{
    private static readonly Meter Meter = new("EnterpriseKafka.Metrics", "1.0.0");

    /// <summary>Counter incremented when a message is successfully produced.</summary>
    public static readonly Counter<long> MessagesProduced =
        Meter.CreateCounter<long>("kafka.messages.produced", description: "Messages published to Kafka.");

    /// <summary>Counter incremented when a message is successfully consumed.</summary>
    public static readonly Counter<long> MessagesConsumed =
        Meter.CreateCounter<long>("kafka.messages.consumed", description: "Messages consumed from Kafka.");

    /// <summary>Counter incremented when message processing fails.</summary>
    public static readonly Counter<long> MessagesFailed =
        Meter.CreateCounter<long>("kafka.messages.failed", description: "Messages that failed processing.");
}

/// <summary>
/// Middleware that starts an <see cref="Activity"/> span and records OTel metrics for each
/// message processed.
/// </summary>
public sealed class TelemetryMiddleware : IKafkaMiddleware
{
    /// <inheritdoc />
    public async Task InvokeAsync(
        KafkaContext context,
        Func<Task> next,
        CancellationToken cancellationToken = default)
    {
        using var activity = KafkaTelemetry.Source.StartActivity(
            $"kafka.consume {context.Topic}",
            ActivityKind.Consumer);

        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination", context.Topic);
        activity?.SetTag("messaging.kafka.partition", context.Partition);
        activity?.SetTag("messaging.kafka.offset", context.Offset);
        activity?.SetTag("messaging.conversation_id", context.CorrelationId);

        var tags = new TagList { { "topic", context.Topic } };

        try
        {
            await next();
            KafkaMetrics.MessagesConsumed.Add(1, tags);
        }
        catch
        {
            KafkaMetrics.MessagesFailed.Add(1, tags);
            activity?.SetStatus(ActivityStatusCode.Error);
            throw;
        }
    }
}

/// <summary>DI extension methods for telemetry support.</summary>
public static class TelemetryExtensions
{
    /// <summary>Registers <see cref="TelemetryMiddleware"/> with the DI container.</summary>
    public static IServiceCollection AddKafkaTelemetry(this IServiceCollection services)
    {
        services.AddTransient<TelemetryMiddleware>();
        return services;
    }
}
