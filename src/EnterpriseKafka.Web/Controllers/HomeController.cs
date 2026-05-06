using System.Text.Json;
using EnterpriseKafka.Abstractions;
using Microsoft.AspNetCore.Mvc;

namespace EnterpriseKafka.Web.Controllers;

public class HomeController(IKafkaProducer producer) : Controller
{
    public IActionResult Index() => View();

    [HttpPost]
    public async Task<IActionResult> Publish([FromForm] string topic, [FromForm] string? key, [FromForm] string payload)
    {
        if (string.IsNullOrWhiteSpace(topic))
            return BadRequest(new { success = false, message = "Topic is required." });

        try
        {
            var message = JsonSerializer.Deserialize<JsonElement>(payload);
            var options = new KafkaPublishOptions(Topic: topic, Key: string.IsNullOrWhiteSpace(key) ? null : key);
            await producer.PublishAsync(message, options);
            return Ok(new { success = true, message = $"Published to topic '{topic}'." });
        }
        catch (JsonException)
        {
            return BadRequest(new { success = false, message = "Payload is not valid JSON." });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { success = false, message = ex.Message });
        }
    }
}
