using EnterpriseKafka.Core;
using FluentAssertions;

namespace UnitTests;

public class SerializerTests
{
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public void RoundTrip_SimpleObject()
    {
        var original = new SampleMessage { Id = 42, Name = "test" };
        var bytes = _serializer.Serialize(original);
        var result = _serializer.Deserialize<SampleMessage>(bytes);

        result.Id.Should().Be(42);
        result.Name.Should().Be("test");
    }

    [Fact]
    public void Serialize_ProducesNonEmptyBytes()
    {
        var bytes = _serializer.Serialize(new SampleMessage { Id = 1, Name = "x" });
        bytes.Should().NotBeEmpty();
    }

    [Fact]
    public void Deserialize_HandlesNullableProperties()
    {
        var original = new SampleMessage { Id = 1, Name = null };
        var bytes = _serializer.Serialize(original);
        var result = _serializer.Deserialize<SampleMessage>(bytes);
        result.Name.Should().BeNull();
    }

    [Fact]
    public void RoundTrip_Collection()
    {
        var list = new List<int> { 1, 2, 3 };
        var bytes = _serializer.Serialize(list);
        var result = _serializer.Deserialize<List<int>>(bytes);
        result.Should().Equal(1, 2, 3);
    }

    private sealed class SampleMessage
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }
}
