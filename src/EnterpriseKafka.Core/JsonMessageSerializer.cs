using System.Text.Json;
using EnterpriseKafka.Abstractions;

namespace EnterpriseKafka.Core;

public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    public string ContentType => "application/json";
    public byte[] Serialize<T>(T message) => JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
    public T Deserialize<T>(ReadOnlySpan<byte> payload) => JsonSerializer.Deserialize<T>(payload, JsonOptions)!;
}
