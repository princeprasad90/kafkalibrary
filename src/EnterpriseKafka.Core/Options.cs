using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace EnterpriseKafka.Core;

public sealed class EnterpriseKafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public ProducerOptions Producer { get; set; } = new();
    public ConsumerOptions Consumer { get; set; } = new();
    public RetryOptions Retry { get; set; } = new();
    public Dictionary<string, string> TopicMappings { get; set; } = new();
    public List<Type> Middlewares { get; } = new();

    public EnterpriseKafkaOptions UseMiddleware<T>() where T : class, IKafkaMiddleware
    {
        Middlewares.Add(typeof(T));
        return this;
    }
}

public sealed class ProducerOptions { public bool EnableIdempotence { get; set; } = true; }
public sealed class ConsumerOptions { public string GroupId { get; set; } = "enterprise-kafka"; public int Concurrency { get; set; } = 1; }
public sealed class RetryOptions { public int MaxRetries { get; set; } = 3; public int BaseDelayMs { get; set; } = 250; public bool UseExponentialBackoff { get; set; } = true; }

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
        services.AddSingleton<IMessageFailureStrategy, IgnoreStrategy>();
        services.AddTransient<LoggingMiddleware>();
        services.AddTransient<TelemetryMiddleware>();
        return services;
    }
}
