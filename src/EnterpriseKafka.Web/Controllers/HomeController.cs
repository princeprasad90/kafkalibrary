using System.Text.Json;
using EnterpriseKafka.Abstractions;
using EnterpriseKafka.Web.Services;
using Microsoft.AspNetCore.Mvc;

namespace EnterpriseKafka.Web.Controllers;

public sealed record PublishRequest(string Topic, string? Key, string Payload);
public sealed record ConsumerRequest(string Topic, string? GroupId, bool FromBeginning);

public class HomeController(IKafkaProducer producer, KafkaTestConsoleService testConsole, ILogger<HomeController> logger) : Controller
{
    public IActionResult Index() => View();

    [HttpPost]
    public async Task<IActionResult> Publish([FromBody] PublishRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Topic))
            return BadRequest(new { success = false, message = "Topic is required." });

        JsonElement message;
        try
        {
            message = JsonSerializer.Deserialize<JsonElement>(request.Payload);
        }
        catch (JsonException)
        {
            return BadRequest(new { success = false, message = "Payload is not valid JSON." });
        }

        try
        {
            var options = new KafkaPublishOptions(Topic: request.Topic, Key: string.IsNullOrWhiteSpace(request.Key) ? null : request.Key);
            await producer.PublishAsync(message, options);
            return Ok(new { success = true, message = $"Published to topic '{request.Topic}'." });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to publish message to topic {Topic}", request.Topic);
            return StatusCode(500, new { success = false, message = "An error occurred while publishing the message." });
        }
    }

    [HttpPost]
    public async Task<IActionResult> StartConsumer([FromBody] ConsumerRequest request, CancellationToken cancellationToken)
    {
        try
        {
            var state = await testConsole.StartConsumerAsync(request.Topic, request.GroupId, request.FromBeginning, cancellationToken);
            return Ok(new { success = true, state });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { success = false, message = ex.Message });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to start Kafka test consumer for topic {Topic}", request.Topic);
            return StatusCode(500, new { success = false, message = "An error occurred while starting the consumer." });
        }
    }

    [HttpPost]
    public async Task<IActionResult> StopConsumer(CancellationToken cancellationToken)
    {
        var state = await testConsole.StopConsumerAsync(cancellationToken);
        return Ok(new { success = true, state });
    }

    [HttpGet]
    public IActionResult ConsumerState() => Ok(testConsole.CurrentState);

    [HttpGet]
    public IActionResult Messages() => Ok(testConsole.RecentMessages);
}
