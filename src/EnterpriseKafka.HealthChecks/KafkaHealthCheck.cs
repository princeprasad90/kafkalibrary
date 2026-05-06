using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EnterpriseKafka.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace EnterpriseKafka.HealthChecks;

/// <summary>Health check that tests TCP connectivity to the Kafka bootstrap servers.</summary>
public sealed class KafkaBrokerHealthCheck(
    EnterpriseKafkaOptions options,
    ILogger<KafkaBrokerHealthCheck> logger) : IHealthCheck
{
    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var config = new AdminClientConfig { BootstrapServers = options.BootstrapServers };
            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = await Task.Run(
                () => adminClient.GetMetadata(TimeSpan.FromSeconds(5)),
                cancellationToken);

            return HealthCheckResult.Healthy(
                $"Connected to {metadata.Brokers.Count} broker(s).",
                new Dictionary<string, object>
                {
                    ["brokers"] = metadata.Brokers.Count,
                    ["topics"] = metadata.Topics.Count,
                });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Kafka broker health check failed.");
            return HealthCheckResult.Unhealthy("Unable to connect to Kafka brokers.", ex);
        }
    }
}

/// <summary>
/// Health check that monitors consumer group lag and reports degraded/unhealthy
/// when lag exceeds the configured threshold.
/// </summary>
public sealed class KafkaConsumerLagHealthCheck(
    EnterpriseKafkaOptions options,
    ILogger<KafkaConsumerLagHealthCheck> logger,
    long lagThreshold = 10_000) : IHealthCheck
{
    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var groups = options.Consumers
                .Select(c => c.GroupId)
                .Distinct()
                .ToList();

            if (groups.Count == 0)
                return HealthCheckResult.Healthy("No consumer groups registered.");

            long estimatedLag = 0;
            foreach (var reg in options.Consumers)
            {
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = options.BootstrapServers,
                    GroupId = reg.GroupId,
                    EnableAutoCommit = false,
                };

                using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

                var adminConfig = new AdminClientConfig { BootstrapServers = options.BootstrapServers };
                using var adminClient = new AdminClientBuilder(adminConfig).Build();

                var metadata = await Task.Run(
                    () => adminClient.GetMetadata(reg.Topic, TimeSpan.FromSeconds(5)),
                    cancellationToken);

                foreach (var topicMeta in metadata.Topics)
                foreach (var partition in topicMeta.Partitions)
                {
                    var tp = new TopicPartition(reg.Topic, new Partition(partition.PartitionId));
                    var offsets = await Task.Run(
                        () => consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5)),
                        cancellationToken);
                    estimatedLag += offsets.High - offsets.Low;
                }
            }

            if (estimatedLag > lagThreshold)
                return HealthCheckResult.Degraded(
                    $"Estimated consumer lag ({estimatedLag}) exceeds threshold ({lagThreshold}).");

            return HealthCheckResult.Healthy($"Estimated consumer lag: {estimatedLag}.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Kafka consumer lag health check failed.");
            return HealthCheckResult.Unhealthy("Unable to retrieve consumer lag.", ex);
        }
    }
}

/// <summary>Extension methods for registering Kafka health checks.</summary>
public static class HealthCheckExtensions
{
    /// <summary>Adds a broker connectivity health check.</summary>
    public static IHealthChecksBuilder AddKafkaBroker(
        this IHealthChecksBuilder builder,
        string name = "kafka-broker",
        HealthStatus failureStatus = HealthStatus.Unhealthy,
        IEnumerable<string>? tags = null)
    {
        builder.Services.AddSingleton<KafkaBrokerHealthCheck>();
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredService<KafkaBrokerHealthCheck>(),
            failureStatus,
            tags));
    }

    /// <summary>Adds a consumer-lag health check.</summary>
    public static IHealthChecksBuilder AddKafkaConsumerLag(
        this IHealthChecksBuilder builder,
        long lagThreshold = 10_000,
        string name = "kafka-consumer-lag",
        HealthStatus failureStatus = HealthStatus.Degraded,
        IEnumerable<string>? tags = null)
    {
        builder.Services.AddSingleton(sp =>
            new KafkaConsumerLagHealthCheck(
                sp.GetRequiredService<EnterpriseKafkaOptions>(),
                sp.GetRequiredService<ILogger<KafkaConsumerLagHealthCheck>>(),
                lagThreshold));

        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredService<KafkaConsumerLagHealthCheck>(),
            failureStatus,
            tags));
    }
}
