using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EnterpriseKafka.Core;

public sealed class EnterpriseKafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public ProducerOptions Producer { get; set; } = new();
    public ConsumerOptions Consumer { get; set; } = new();
    public RetryOptions Retry { get; set; } = new();
    public FailureHandlingOptions OnFailure { get; set; } = new();
    public SecurityOptions Security { get; set; } = new();
    public TopicNamingOptions TopicNaming { get; set; } = new();
    public HealthCheckOptions HealthChecks { get; set; } = new();
    public ObservabilityOptions Observability { get; set; } = new();
    public Dictionary<string, string> TopicMappings { get; set; } = new();
    public List<KafkaTopicDefinition> Topics { get; } = new();
    public List<ConsumerRegistration> Consumers { get; } = new();
    public List<Type> Middlewares { get; } = new();

    public EnterpriseKafkaOptions UseMiddleware<T>() where T : class, IKafkaMiddleware
    {
        Middlewares.Add(typeof(T));
        return this;
    }

    public EnterpriseKafkaOptions UseLogging() => UseMiddleware<LoggingMiddleware>();

    public EnterpriseKafkaOptions UseTracing() => UseMiddleware<TelemetryMiddleware>();

    public EnterpriseKafkaOptions UseRetry()
    {
        Retry.Enabled = true;
        return this;
    }

    public EnterpriseKafkaOptions UseValidation() => UseMiddleware<ValidationMiddleware>();

    public EnterpriseKafkaOptions AddConsumer<TMessage, THandler>(string? topic = null, string? groupId = null)
        where THandler : class, IKafkaHandler<TMessage>
    {
        Consumers.Add(new ConsumerRegistration(typeof(TMessage), typeof(THandler), topic, groupId));
        return this;
    }
}

public sealed class ProducerOptions
{
    public bool EnableIdempotence { get; set; } = true;
    public string Acks { get; set; } = "all";
    public int LingerMs { get; set; } = 5;
    public int BatchSizeBytes { get; set; } = 100_000;
    public string? ClientId { get; set; }
}

public sealed class ConsumerOptions
{
    public string GroupId { get; set; } = "enterprise-kafka";
    public int Concurrency { get; set; } = 1;
    public bool EnableAutoCommit { get; set; }
    public bool CommitAfterSuccessfulProcessing { get; set; } = true;
    public int MaxPollIntervalMs { get; set; } = 300_000;
    public int ChannelCapacity { get; set; } = 1_000;
}

public sealed class RetryOptions
{
    public bool Enabled { get; set; } = true;
    public int MaxRetries { get; set; } = 3;
    public int BaseDelayMs { get; set; } = 250;
    public int MaxDelayMs { get; set; } = 30_000;
    public bool UseExponentialBackoff { get; set; } = true;
    public bool UseRetryTopics { get; set; }
}

public sealed class FailureHandlingOptions
{
    public FailureStrategyKind Strategy { get; private set; } = FailureStrategyKind.Ignore;
    public Type? CustomStrategyType { get; private set; }

    public FailureHandlingOptions UseDeadLetterQueue()
    {
        Strategy = FailureStrategyKind.DeadLetter;
        return this;
    }

    public FailureHandlingOptions Ignore()
    {
        Strategy = FailureStrategyKind.Ignore;
        return this;
    }

    public FailureHandlingOptions PersistToDatabase()
    {
        Strategy = FailureStrategyKind.DatabasePersistence;
        return this;
    }

    public FailureHandlingOptions AlertOnly()
    {
        Strategy = FailureStrategyKind.AlertOnly;
        return this;
    }

    public FailureHandlingOptions StopConsumer()
    {
        Strategy = FailureStrategyKind.StopConsumer;
        return this;
    }

    public FailureHandlingOptions UseCustomHandler<T>() where T : class, IMessageFailureStrategy
    {
        Strategy = FailureStrategyKind.Custom;
        CustomStrategyType = typeof(T);
        return this;
    }
}

public enum FailureStrategyKind
{
    Ignore,
    DeadLetter,
    DatabasePersistence,
    AlertOnly,
    StopConsumer,
    Custom
}

public sealed class SecurityOptions
{
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public string? SecurityProtocol { get; set; }
    public string? SslCaLocation { get; set; }
    public string? OAuthTokenEndpoint { get; set; }
    public string? KeyVaultUri { get; set; }
}

public sealed class TopicNamingOptions
{
    public string? Environment { get; set; }
    public bool EnvironmentAsPrefix { get; set; } = true;
    public string RetrySuffix { get; set; } = "retry";
    public string DeadLetterSuffix { get; set; } = "dlq";
    public bool AutoCreateTopics { get; set; }
}

public sealed class HealthCheckOptions
{
    public TimeSpan BrokerTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public long MaxAllowedLag { get; set; } = 10_000;
}

public sealed class ObservabilityOptions
{
    public bool EnableOpenTelemetry { get; set; } = true;
    public bool EnableMetrics { get; set; } = true;
    public bool IncludeMessageType { get; set; } = true;
}

public sealed record ConsumerRegistration(Type MessageType, Type HandlerType, string? Topic, string? GroupId);

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEnterpriseKafka(this IServiceCollection services, Action<EnterpriseKafkaOptions>? configure = null)
    {
        var opts = new EnterpriseKafkaOptions();
        configure?.Invoke(opts);
        services.AddSingleton(opts);
        services.AddSingleton<ITopicResolver, DefaultTopicResolver>();
        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        services.AddHostedService<KafkaConsumerHostedService>();
        services.TryAddSingleton<IKafkaRetryEngine, PollyKafkaRetryEngine>();
        services.AddSingleton<IMessageFailureStrategy>(sp => FailureStrategyFactory.Create(sp, opts));
        services.AddTransient<LoggingMiddleware>();
        services.AddTransient<TelemetryMiddleware>();
        services.AddTransient<ValidationMiddleware>();
        foreach (var middlewareType in opts.Middlewares)
        {
            services.TryAddTransient(middlewareType);
        }

        foreach (var consumer in opts.Consumers)
        {
            services.TryAddTransient(consumer.HandlerType);
        }

        if (opts.OnFailure.CustomStrategyType is not null)
        {
            services.TryAddTransient(opts.OnFailure.CustomStrategyType);
        }

        return services;
    }

    public static IServiceCollection AddKafkaConsumer<TMessage, THandler>(this IServiceCollection services, Action<EnterpriseKafkaOptions>? configure = null)
        where THandler : class, IKafkaHandler<TMessage>
        => services.AddEnterpriseKafka(options =>
        {
            configure?.Invoke(options);
            options.AddConsumer<TMessage, THandler>();
        });
}
