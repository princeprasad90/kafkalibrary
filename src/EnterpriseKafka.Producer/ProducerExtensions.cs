using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;
using Microsoft.Extensions.DependencyInjection;

namespace EnterpriseKafka.Producer;

/// <summary>Extension methods for <see cref="IKafkaProducer"/> and producer DI registration.</summary>
public static class ProducerExtensions
{
    /// <summary>
    /// Publishes a message to an explicit topic, overriding any resolved topic name.
    /// </summary>
    /// <typeparam name="T">Message type.</typeparam>
    /// <param name="producer">The producer instance.</param>
    /// <param name="topic">The target Kafka topic.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="key">Optional partition key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static Task PublishToTopicAsync<T>(
        this IKafkaProducer producer,
        string topic,
        T message,
        string? key = null,
        CancellationToken cancellationToken = default)
    {
        var opts = new KafkaPublishOptions(Topic: topic, Key: key);
        return producer.PublishAsync(message, opts, cancellationToken);
    }

    /// <summary>
    /// Publishes a message with a simple linear retry on transient failures.
    /// </summary>
    /// <typeparam name="T">Message type.</typeparam>
    /// <param name="producer">The producer instance.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="retryCount">Number of additional attempts after the first failure.</param>
    /// <param name="options">Optional publish options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task PublishWithRetryAsync<T>(
        this IKafkaProducer producer,
        T message,
        int retryCount = 3,
        KafkaPublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var attempts = 0;
        while (true)
        {
            try
            {
                await producer.PublishAsync(message, options, cancellationToken);
                return;
            }
            catch (Exception) when (attempts < retryCount)
            {
                attempts++;
                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempts), cancellationToken);
            }
        }
    }

    /// <summary>
    /// Registers <see cref="IKafkaProducer"/> with custom options and all required core services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional configuration callback.</param>
    public static IServiceCollection AddKafkaProducer(
        this IServiceCollection services,
        Action<EnterpriseKafkaOptions>? configure = null)
    {
        // Re-use the core registration path.
        services.AddEnterpriseKafka(configure);
        return services;
    }
}
