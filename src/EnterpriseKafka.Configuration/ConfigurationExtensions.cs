using EnterpriseKafka.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EnterpriseKafka.Configuration;

/// <summary>POCO matching the <c>EnterpriseKafka</c> section in appsettings.</summary>
public sealed class KafkaConfigurationSection
{
    /// <summary>Kafka bootstrap servers (comma-separated).</summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Producer settings.</summary>
    public KafkaProducerSection Producer { get; set; } = new();

    /// <summary>Consumer settings.</summary>
    public KafkaConsumerSection Consumer { get; set; } = new();

    /// <summary>Retry settings.</summary>
    public KafkaRetrySection Retry { get; set; } = new();

    /// <summary>Manual topic overrides.</summary>
    public Dictionary<string, string> TopicMappings { get; set; } = new();
}

/// <summary>Producer sub-section.</summary>
public sealed class KafkaProducerSection
{
    /// <summary>Enable idempotent producer.</summary>
    public bool EnableIdempotence { get; set; } = true;
}

/// <summary>Consumer sub-section.</summary>
public sealed class KafkaConsumerSection
{
    /// <summary>Default consumer group id.</summary>
    public string GroupId { get; set; } = "enterprise-kafka";

    /// <summary>Consumer loop concurrency.</summary>
    public int Concurrency { get; set; } = 1;
}

/// <summary>Retry sub-section.</summary>
public sealed class KafkaRetrySection
{
    /// <summary>Max retry attempts.</summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>Base delay in milliseconds.</summary>
    public int BaseDelayMs { get; set; } = 250;

    /// <summary>Use exponential backoff.</summary>
    public bool UseExponentialBackoff { get; set; } = true;
}

/// <summary>Validates <see cref="EnterpriseKafkaOptions"/> at startup.</summary>
public sealed class KafkaOptionsValidator : IValidateOptions<EnterpriseKafkaOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, EnterpriseKafkaOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
            return ValidateOptionsResult.Fail("EnterpriseKafka:BootstrapServers must not be empty.");

        if (options.Retry.MaxRetries < 0)
            return ValidateOptionsResult.Fail("EnterpriseKafka:Retry:MaxRetries must be >= 0.");

        if (options.Retry.BaseDelayMs < 0)
            return ValidateOptionsResult.Fail("EnterpriseKafka:Retry:BaseDelayMs must be >= 0.");

        return ValidateOptionsResult.Success;
    }
}

/// <summary>DI extension methods for configuration-driven setup.</summary>
public static class ConfigurationExtensions
{
    /// <summary>
    /// Loads <see cref="EnterpriseKafkaOptions"/> from the <c>EnterpriseKafka</c> configuration
    /// section and registers core services.
    /// </summary>
    public static IServiceCollection AddKafkaConfiguration(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var section = configuration.GetSection("EnterpriseKafka");
        var config = section.Get<KafkaConfigurationSection>() ?? new KafkaConfigurationSection();

        services.AddEnterpriseKafka(opts =>
        {
            opts.BootstrapServers = config.BootstrapServers;
            opts.Producer.EnableIdempotence = config.Producer.EnableIdempotence;
            opts.Consumer.GroupId = config.Consumer.GroupId;
            opts.Consumer.Concurrency = config.Consumer.Concurrency;
            opts.Retry.MaxRetries = config.Retry.MaxRetries;
            opts.Retry.BaseDelayMs = config.Retry.BaseDelayMs;
            opts.Retry.UseExponentialBackoff = config.Retry.UseExponentialBackoff;

            foreach (var (k, v) in config.TopicMappings)
                opts.TopicMappings[k] = v;
        });

        services.AddSingleton<IValidateOptions<EnterpriseKafkaOptions>, KafkaOptionsValidator>();

        return services;
    }
}
