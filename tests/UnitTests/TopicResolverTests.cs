using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;
using FluentAssertions;
using Xunit;

namespace UnitTests;

[KafkaTopic("payment-created")]
public sealed class PaymentCreated { }

public class TopicResolverTests
{
    [Fact]
    public void Resolves_AttributeTopic()
    {
        var resolver = new DefaultTopicResolver(new EnterpriseKafkaOptions());
        resolver.ResolveTopic<PaymentCreated>().Should().Be("payment-created");
    }

    [Fact]
    public void Resolves_EnvironmentTopicAndRetryTopics()
    {
        var resolver = new DefaultTopicResolver(new EnterpriseKafkaOptions
        {
            TopicNaming = new TopicNamingOptions { Environment = "prod" }
        });

        resolver.ResolveTopic<PaymentCreated>().Should().Be("prod.payment-created");
        resolver.ResolveRetryTopic("prod.payment-created", 2).Should().Be("prod.payment-created.retry2");
        resolver.ResolveDeadLetterTopic("prod.payment-created").Should().Be("prod.payment-created.dlq");
    }
}
