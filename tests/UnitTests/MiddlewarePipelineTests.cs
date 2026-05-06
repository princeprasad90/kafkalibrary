using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Core;
using FluentAssertions;

namespace UnitTests;

public class MiddlewarePipelineTests
{
    private static KafkaContext MakeContext() => new()
    {
        Topic = "test-topic",
        Message = new object(),
    };

    [Fact]
    public async Task Pipeline_ExecutesMiddleware_InOrder()
    {
        var order = new List<string>();

        var m1 = new LambdaMiddleware(async (ctx, next, _) =>
        {
            order.Add("m1-before");
            await next();
            order.Add("m1-after");
        });
        var m2 = new LambdaMiddleware(async (ctx, next, _) =>
        {
            order.Add("m2-before");
            await next();
            order.Add("m2-after");
        });

        var pipeline = new MiddlewarePipeline([m1, m2]);
        await pipeline.ExecuteAsync(MakeContext(), () =>
        {
            order.Add("terminal");
            return Task.CompletedTask;
        });

        order.Should().Equal("m1-before", "m2-before", "terminal", "m2-after", "m1-after");
    }

    [Fact]
    public async Task Pipeline_WithNoMiddleware_ExecutesTerminal()
    {
        var executed = false;
        var pipeline = new MiddlewarePipeline([]);
        await pipeline.ExecuteAsync(MakeContext(), () =>
        {
            executed = true;
            return Task.CompletedTask;
        });
        executed.Should().BeTrue();
    }

    [Fact]
    public async Task Pipeline_ShortCircuits_WhenMiddlewareDoesNotCallNext()
    {
        var terminalCalled = false;
        var blocking = new LambdaMiddleware((ctx, next, _) => Task.CompletedTask); // doesn't call next

        var pipeline = new MiddlewarePipeline([blocking]);
        await pipeline.ExecuteAsync(MakeContext(), () =>
        {
            terminalCalled = true;
            return Task.CompletedTask;
        });

        terminalCalled.Should().BeFalse();
    }

    [Fact]
    public async Task IgnoreStrategy_DoesNotThrow()
    {
        var strategy = new IgnoreStrategy();
        var act = async () => await strategy.HandleFailureAsync(
            MakeContext(),
            new Exception("test"));

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StopConsumerStrategy_Throws()
    {
        var strategy = new StopConsumerStrategy();
        var act = async () => await strategy.HandleFailureAsync(
            MakeContext(),
            new Exception("oops"));

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Consumer stopped*");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private sealed class LambdaMiddleware(
        Func<KafkaContext, Func<Task>, CancellationToken, Task> fn) : IKafkaMiddleware
    {
        public Task InvokeAsync(KafkaContext ctx, Func<Task> next, CancellationToken ct = default)
            => fn(ctx, next, ct);
    }
}
