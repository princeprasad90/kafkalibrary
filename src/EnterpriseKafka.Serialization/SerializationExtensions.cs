using System.Text.Json;
using EnterpriseKafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace EnterpriseKafka.Serialization;

/// <summary>Wraps a message payload with metadata for envelope-based messaging.</summary>
/// <typeparam name="T">The message payload type.</typeparam>
/// <param name="CorrelationId">Unique correlation identifier.</param>
/// <param name="Timestamp">UTC timestamp when the envelope was created.</param>
/// <param name="Version">Schema/contract version of the payload.</param>
/// <param name="Payload">The actual message payload.</param>
public sealed record MessageEnvelope<T>(
    string CorrelationId,
    DateTimeOffset Timestamp,
    string Version,
    T Payload)
{
    /// <summary>Creates a new envelope with current time and default version.</summary>
    public static MessageEnvelope<T> Wrap(T payload, string? correlationId = null, string version = "1.0")
        => new(correlationId ?? Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow, version, payload);
}

/// <summary>
/// Serialiser that wraps each message in a <see cref="MessageEnvelope{T}"/> before
/// serialising, and unwraps on deserialisation.
/// </summary>
/// <typeparam name="T">Message payload type.</typeparam>
public sealed class MessageEnvelopeSerializer<T>(IMessageSerializer inner) : IMessageSerializer
{
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    /// <inheritdoc />
    public byte[] Serialize<TMsg>(TMsg message)
    {
        if (message is T typed)
        {
            var envelope = MessageEnvelope<T>.Wrap(typed);
            return JsonSerializer.SerializeToUtf8Bytes(envelope, JsonOpts);
        }
        return inner.Serialize(message);
    }

    /// <inheritdoc />
    public TMsg Deserialize<TMsg>(ReadOnlySpan<byte> payload)
    {
        if (typeof(TMsg) == typeof(T))
        {
            var envelope = JsonSerializer.Deserialize<MessageEnvelope<T>>(payload, JsonOpts)!;
            return (TMsg)(object)envelope.Payload!;
        }
        return inner.Deserialize<TMsg>(payload);
    }
}

/// <summary>DI extension methods for serialisation support.</summary>
public static class SerializationExtensions
{
    /// <summary>Registers the default JSON <see cref="IMessageSerializer"/>.</summary>
    public static IServiceCollection AddKafkaJsonSerializer(this IServiceCollection services)
    {
        services.AddSingleton<IMessageSerializer, EnterpriseKafka.Core.JsonMessageSerializer>();
        return services;
    }

    /// <summary>Registers a custom <see cref="IMessageSerializer"/>.</summary>
    public static IServiceCollection AddKafkaSerializer<TSerializer>(this IServiceCollection services)
        where TSerializer : class, IMessageSerializer
    {
        services.AddSingleton<IMessageSerializer, TSerializer>();
        return services;
    }
}
