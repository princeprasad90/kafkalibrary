using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Testing;
using FluentAssertions;

namespace UnitTests;

public class KafkaProducerTests
{
    [Fact]
    public async Task FakeProducer_RecordsPublishedMessage()
    {
        var producer = new FakeKafkaProducer();
        var msg = new PaymentCreated();

        await producer.PublishAsync(msg, new KafkaPublishOptions(Topic: "payment-created"));

        producer.PublishedMessages.Should().HaveCount(1);
        producer.SentMessages<PaymentCreated>().Should().ContainSingle();
    }

    [Fact]
    public async Task FakeProducer_BatchPublish_RecordsAllMessages()
    {
        var producer = new FakeKafkaProducer();
        var messages = new[] { new PaymentCreated(), new PaymentCreated() };

        await producer.PublishBatchAsync(messages, new KafkaPublishOptions(Topic: "payment-created"));

        producer.SentMessages<PaymentCreated>().Should().HaveCount(2);
    }

    [Fact]
    public async Task FakeProducer_UsesTypeName_WhenTopicNotProvided()
    {
        var producer = new FakeKafkaProducer();
        await producer.PublishAsync(new PaymentCreated());

        producer.PublishedMessages.Should().ContainSingle(m => m.Topic == "paymentcreated");
    }

    [Fact]
    public void MockTopicResolver_ReturnsConfiguredTopic()
    {
        var resolver = new MockTopicResolver().ForType<PaymentCreated>("payments");
        resolver.ResolveTopic<PaymentCreated>().Should().Be("payments");
    }

    [Fact]
    public void MockTopicResolver_FallsBackToTypeName()
    {
        var resolver = new MockTopicResolver();
        resolver.ResolveTopic<PaymentCreated>().Should().Be("paymentcreated");
    }

    [Fact]
    public async Task FakeProducer_Reset_ClearsMessages()
    {
        var producer = new FakeKafkaProducer();
        await producer.PublishAsync(new PaymentCreated());
        producer.Reset();
        producer.PublishedMessages.Should().BeEmpty();
    }
}
