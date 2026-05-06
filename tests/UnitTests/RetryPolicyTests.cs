using EnterpriseKafka.Core;
using EnterpriseKafka.Retry;
using FluentAssertions;

namespace UnitTests;

public class RetryPolicyTests
{
    [Fact]
    public void BuildImmediate_CreatesValidPipeline()
    {
        var pipeline = KafkaRetryPolicy.BuildImmediate(maxRetries: 2);
        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void BuildDelayed_CreatesValidPipeline()
    {
        var pipeline = KafkaRetryPolicy.BuildDelayed(maxRetries: 2, delay: TimeSpan.FromMilliseconds(1));
        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void BuildExponential_CreatesValidPipeline()
    {
        var pipeline = KafkaRetryPolicy.BuildExponential(maxRetries: 2, baseDelay: TimeSpan.FromMilliseconds(1));
        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void FromOptions_Exponential_CreatesExponentialPipeline()
    {
        var opts = new RetryOptions { MaxRetries = 2, BaseDelayMs = 1, UseExponentialBackoff = true };
        var pipeline = KafkaRetryPolicy.FromOptions(opts);
        pipeline.Should().NotBeNull();
    }

    [Fact]
    public void FromOptions_Immediate_WhenBaseDelayIsZero()
    {
        var opts = new RetryOptions { MaxRetries = 2, BaseDelayMs = 0, UseExponentialBackoff = false };
        var pipeline = KafkaRetryPolicy.FromOptions(opts);
        pipeline.Should().NotBeNull();
    }

    [Fact]
    public async Task Pipeline_Retries_OnFailure()
    {
        var attempts = 0;
        var pipeline = KafkaRetryPolicy.BuildImmediate(maxRetries: 3);

        await pipeline.ExecuteAsync(async ct =>
        {
            attempts++;
            if (attempts < 3) throw new InvalidOperationException("transient");
            await Task.CompletedTask;
        });

        attempts.Should().Be(3);
    }
}
