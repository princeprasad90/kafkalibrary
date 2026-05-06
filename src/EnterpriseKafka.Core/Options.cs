using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace EnterpriseKafka.Core;

/// <summary>Registration for a typed Kafka consumer.</summary>
public sealed class ConsumerRegistration
{
    /// <summary>The <see cref="IKafkaHandler{T}"/> implementation type.</summary>
    public required Type HandlerType { get; init; }

    /// <summary>The message type this consumer handles.</summary>
    public required Type MessageType { get; init; }

    /// <summary>The Kafka topic to subscribe to.</summary>
    public required string Topic { get; init; }

    /// <summary>The consumer group id.</summary>
    public string GroupId { get; init; } = "enterprise-kafka";

    /// <summary>Optional failure strategy type (<see cref="IMessageFailureStrategy"/>).</summary>
    public Type? FailureStrategy { get; init; }
}

/// <summary>Top-level options for the EnterpriseKafka framework.</summary>
public sealed class EnterpriseKafkaOptions
{
    /// <summary>Kafka bootstrap servers (comma-separated).</summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Producer-specific options.</summary>
    public ProducerOptions Producer { get; set; } = new();

    /// <summary>Default consumer options.</summary>
    public ConsumerOptions Consumer { get; set; } = new();

    /// <summary>Retry options.</summary>
    public RetryOptions Retry { get; set; } = new();

    /// <summary>Manual topic-name overrides keyed by full type name.</summary>
    public Dictionary<string, string> TopicMappings { get; set; } = new();

    /// <summary>Registered middleware pipeline types (in order).</summary>
    public List<Type> Middlewares { get; } = new();

    /// <summary>Typed consumer registrations added via AddKafkaConsumer extensions.</summary>
    public List<ConsumerRegistration> Consumers { get; } = new();

    /// <summary>Adds a middleware to the pipeline.</summary>
    public EnterpriseKafkaOptions UseMiddleware<T>() where T : class, IKafkaMiddleware
    {
        Middlewares.Add(typeof(T));
        return this;
    }

    /// <summary>Enables structured logging middleware.</summary>
    public EnterpriseKafkaOptions UseLogging()
    {
        Middlewares.Add(typeof(LoggingMiddleware));
        return this;
    }

    /// <summary>Enables telemetry middleware.</summary>
    public EnterpriseKafkaOptions UseTelemetry()
    {
        Middlewares.Add(typeof(TelemetryMiddleware));
        return this;
    }

    /// <summary>Enables retry middleware (requires EnterpriseKafka.Retry).</summary>
    public EnterpriseKafkaOptions UseRetry()
    {
        // RetryMiddleware is registered by EnterpriseKafka.Retry; this is a marker.
        return this;
    }
}

/// <summary>Kafka producer configuration.</summary>
public sealed class ProducerOptions
{
    /// <summary>Whether to enable idempotent producers.</summary>
    public bool EnableIdempotence { get; set; } = true;
}

/// <summary>Default consumer configuration.</summary>
public sealed class ConsumerOptions
{
    /// <summary>Default consumer group id.</summary>
    public string GroupId { get; set; } = "enterprise-kafka";

    /// <summary>Number of concurrent consumer loops.</summary>
    public int Concurrency { get; set; } = 1;
}

/// <summary>Retry behaviour configuration.</summary>
public sealed class RetryOptions
{
    /// <summary>Maximum retry attempts.</summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>Base delay between retries in milliseconds.</summary>
    public int BaseDelayMs { get; set; } = 250;

    /// <summary>Use exponential backoff between retries.</summary>
    public bool UseExponentialBackoff { get; set; } = true;
}

/// <summary>DI extension methods for EnterpriseKafka.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Registers the EnterpriseKafka framework services.</summary>
    public static IServiceCollection AddEnterpriseKafka(
        this IServiceCollection services,
        Action<EnterpriseKafkaOptions>? configure = null)
    {
        var opts = new EnterpriseKafkaOptions();
        configure?.Invoke(opts);

        services.AddSingleton(opts);
        services.AddSingleton<ITopicResolver, DefaultTopicResolver>();
        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        services.AddHostedService<KafkaConsumerHostedService>();
        services.AddSingleton<IMessageFailureStrategy, IgnoreStrategy>();
        services.AddTransient<LoggingMiddleware>();
        services.AddTransient<TelemetryMiddleware>();

        // Register handlers from consumer registrations as scoped services
        foreach (var reg in opts.Consumers)
        {
            services.AddScoped(reg.HandlerType);
            if (reg.FailureStrategy is not null)
                services.AddScoped(reg.FailureStrategy);
        }

        return services;
    }
}
