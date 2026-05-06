using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EnterpriseKafka.Core;

public sealed class KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Kafka consumer hosted service started. Register typed consumers via AddConsumer extensions.");
        return Task.CompletedTask;
    }
}
