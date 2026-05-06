using EnterpriseKafka.Abstractions;

namespace EnterpriseKafka.Core;

public sealed class DefaultTopicResolver(EnterpriseKafkaOptions options) : ITopicResolver
{
    public string ResolveTopic<T>()
    {
        var type = typeof(T);
        if (options.TopicMappings.TryGetValue(type.FullName ?? type.Name, out var mapped)) return mapped;
        var attr = type.GetCustomAttributes(typeof(KafkaTopicAttribute), true).Cast<KafkaTopicAttribute>().FirstOrDefault();
        return attr?.Name ?? type.Name.ToLowerInvariant();
    }
}
