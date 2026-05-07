using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;

namespace UnitTests;

public sealed class EnterpriseKafkaOptionsTests
{
    [Fact]
    public void UseHelpers_RegisterMiddlewareAndFailureStrategy()
    {
        var options = new EnterpriseKafkaOptions()
            .UseLogging()
            .UseTracing()
            .UseValidation()
            .UseRetry();

        options.OnFailure.UseDeadLetterQueue();

        options.Middlewares.Should().ContainInOrder(typeof(LoggingMiddleware), typeof(TelemetryMiddleware), typeof(ValidationMiddleware));
        options.Retry.Enabled.Should().BeTrue();
        options.OnFailure.Strategy.Should().Be(FailureStrategyKind.DeadLetter);
    }

    [Fact]
    public void AddConsumer_StoresStronglyTypedRegistration()
    {
        var options = new EnterpriseKafkaOptions()
            .AddConsumer<OrderCreated, OrderCreatedHandler>("orders.created", "orders-service");

        options.Consumers.Should().ContainSingle(registration =>
            registration.MessageType == typeof(OrderCreated)
            && registration.HandlerType == typeof(OrderCreatedHandler)
            && registration.Topic == "orders.created"
            && registration.GroupId == "orders-service");
    }
}

public sealed record OrderCreated(string Id);

public sealed class OrderCreatedHandler : IKafkaHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, KafkaContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
}
