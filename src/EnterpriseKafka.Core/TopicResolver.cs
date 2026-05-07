using EnterpriseKafka.Abstractions;

namespace EnterpriseKafka.Core;

public sealed class DefaultTopicResolver(EnterpriseKafkaOptions options) : ITopicResolver
{
    public string ResolveTopic<T>() => ResolveTopic(typeof(T));

    public string ResolveTopic(Type messageType)
    {
        if (messageType.FullName is not null && options.TopicMappings.TryGetValue(messageType.FullName, out var mapped)) return ApplyEnvironment(mapped);
        if (options.TopicMappings.TryGetValue(messageType.Name, out mapped)) return ApplyEnvironment(mapped);
        var consumer = messageType.GetCustomAttributes(typeof(KafkaConsumerAttribute), true).Cast<KafkaConsumerAttribute>().FirstOrDefault();
        if (consumer is not null) return ApplyEnvironment(consumer.Topic);
        var attr = messageType.GetCustomAttributes(typeof(KafkaTopicAttribute), true).Cast<KafkaTopicAttribute>().FirstOrDefault();
        return ApplyEnvironment(attr?.Name ?? messageType.Name.ToLowerInvariant());
    }

    public string ResolveRetryTopic(string sourceTopic, int attempt) => $"{sourceTopic}.{options.TopicNaming.RetrySuffix}{attempt}";

    public string ResolveDeadLetterTopic(string sourceTopic) => $"{sourceTopic}.{options.TopicNaming.DeadLetterSuffix}";

    private string ApplyEnvironment(string topic)
    {
        if (string.IsNullOrWhiteSpace(options.TopicNaming.Environment)) return topic;
        return options.TopicNaming.EnvironmentAsPrefix
            ? $"{options.TopicNaming.Environment}.{topic}"
            : $"{topic}.{options.TopicNaming.Environment}";
    }
}
