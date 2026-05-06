using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;

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
}
