using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;
using Microsoft.Extensions.DependencyInjection;

namespace EnterpriseKafka.Consumer;

/// <summary>Extension methods for registering typed Kafka consumers.</summary>
public static class ConsumerExtensions
{
    /// <summary>
    /// Registers a typed consumer for <typeparamref name="TMessage"/> using
    /// <typeparamref name="THandler"/>, with optional configuration.
    /// </summary>
    /// <typeparam name="TMessage">The message type consumed.</typeparam>
    /// <typeparam name="THandler">The handler implementation.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional callback to customise the registration.</param>
    public static IServiceCollection AddKafkaConsumer<TMessage, THandler>(
        this IServiceCollection services,
        Action<ConsumerRegistration>? configure = null)
        where THandler : class, IKafkaHandler<TMessage>
    {
        var options = services.GetOrCreateEnterpriseKafkaOptions();

        var topicResolver = new DefaultTopicResolver(options);
        var topic = topicResolver.ResolveTopic<TMessage>();

        var reg = new ConsumerRegistration
        {
            HandlerType = typeof(THandler),
            MessageType = typeof(TMessage),
            Topic = topic,
            GroupId = options.Consumer.GroupId,
        };

        // Allow caller to override topic, groupId, failure strategy etc.
        configure?.Invoke(reg);

        options.Consumers.Add(reg);
        services.AddScoped<THandler>();

        if (reg.FailureStrategy is not null)
            services.AddScoped(reg.FailureStrategy);

        return services;
    }

    /// <summary>
    /// Registers a typed consumer for <typeparamref name="TMessage"/> using
    /// <typeparamref name="THandler"/> with explicit topic and group id.
    /// </summary>
    /// <typeparam name="TMessage">The message type consumed.</typeparam>
    /// <typeparam name="THandler">The handler implementation.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="topic">The Kafka topic to subscribe to.</param>
    /// <param name="groupId">The consumer group id.</param>
    public static IServiceCollection AddKafkaConsumer<TMessage, THandler>(
        this IServiceCollection services,
        string topic,
        string groupId = "enterprise-kafka")
        where THandler : class, IKafkaHandler<TMessage>
    {
        var options = services.GetOrCreateEnterpriseKafkaOptions();

        var reg = new ConsumerRegistration
        {
            HandlerType = typeof(THandler),
            MessageType = typeof(TMessage),
            Topic = topic,
            GroupId = groupId,
        };

        options.Consumers.Add(reg);
        services.AddScoped<THandler>();

        return services;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Retrieves the singleton <see cref="EnterpriseKafkaOptions"/> from the service collection,
    /// or creates and registers one if not yet present.
    /// </summary>
    internal static EnterpriseKafkaOptions GetOrCreateEnterpriseKafkaOptions(
        this IServiceCollection services)
    {
        // Find an existing singleton registration.
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(EnterpriseKafkaOptions)
                && descriptor.Lifetime == ServiceLifetime.Singleton
                && descriptor.ImplementationInstance is EnterpriseKafkaOptions existing)
            {
                return existing;
            }
        }

        // None found – create a new one and register it.
        var opts = new EnterpriseKafkaOptions();
        services.AddSingleton(opts);
        return opts;
    }
}
