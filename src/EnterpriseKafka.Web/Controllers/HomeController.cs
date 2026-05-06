using EnterpriseKafka.Abstractions;
using Microsoft.AspNetCore.Mvc;

namespace EnterpriseKafka.Web.Controllers;

/// <summary>Dashboard and status controller.</summary>
public class HomeController : Controller
{
    /// <summary>Renders the developer dashboard.</summary>
    public IActionResult Index() => View();
}

/// <summary>Kafka publish and topic management API.</summary>
[Route("api/kafka")]
public class KafkaController(IKafkaProducer producer) : Controller
{
    /// <summary>Publishes a raw JSON payload to the specified topic.</summary>
    [HttpPost("publish")]
    public async Task<IActionResult> Publish(
        [FromBody] PublishRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Topic))
            return BadRequest("Topic is required.");

        var opts = new KafkaPublishOptions(Topic: request.Topic, Key: request.Key);
        await producer.PublishAsync(request.Payload, opts, cancellationToken);
        return Ok(new { published = true, topic = request.Topic });
    }

    /// <summary>Returns the resolved topic name for a known type name.</summary>
    [HttpGet("topics/{typeName}")]
    public IActionResult Topics(string typeName)
    {
        // Basic lookup — real impl could scan assemblies.
        return Ok(new { typeName, resolvedTopic = typeName.ToLowerInvariant() });
    }
}

/// <summary>Request body for the Publish action.</summary>
public sealed class PublishRequest
{
    /// <summary>Target Kafka topic.</summary>
    public string? Topic { get; set; }

    /// <summary>Optional partition key.</summary>
    public string? Key { get; set; }

    /// <summary>Raw JSON payload (stored as dynamic object).</summary>
    public object? Payload { get; set; }
}

/// <summary>Consumer monitoring controller.</summary>
[Route("monitor")]
public class MonitorController : Controller
{
    /// <summary>Renders the consumer status dashboard.</summary>
    public IActionResult Index() => View();

    /// <summary>Returns consumer status as JSON.</summary>
    [HttpGet("status")]
    public IActionResult Status()
        => Json(new { status = "running", timestamp = DateTime.UtcNow });
}

/// <summary>Load-test endpoint for developer testing.</summary>
[Route("loadtest")]
public class LoadTestController(IKafkaProducer producer) : Controller
{
    /// <summary>Publishes <paramref name="count"/> test messages to the given topic.</summary>
    [HttpPost]
    public async Task<IActionResult> Run(
        [FromQuery] string topic = "load-test",
        [FromQuery] int count = 100,
        CancellationToken cancellationToken = default)
    {
        if (count is < 1 or > 10_000)
            return BadRequest("Count must be between 1 and 10,000.");

        var messages = Enumerable.Range(1, count)
            .Select(i => new { id = i, timestamp = DateTime.UtcNow, topic })
            .Cast<object>()
            .ToList();

        var opts = new KafkaPublishOptions(Topic: topic);
        await producer.PublishBatchAsync(messages, opts, cancellationToken);

        return Ok(new { sent = count, topic });
    }
}

