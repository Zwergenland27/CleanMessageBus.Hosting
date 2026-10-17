using System.Text.Json;
using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting;

/// <summary>
/// Default serializer based on System.Text.Json.
/// </summary>
public sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Creates the serializer with <see cref="JsonSerializerDefaults.Web"/>.
    /// </summary>
    public SystemTextJsonMessageSerializer()
        : this(new JsonSerializerOptions(JsonSerializerDefaults.Web))
    {
    }

    /// <summary>
    /// Creates the serializer with the given options.
    /// </summary>
    public SystemTextJsonMessageSerializer(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize(object value, Type type) =>
        JsonSerializer.SerializeToUtf8Bytes(value, type, _options);

    /// <inheritdoc />
    public object Deserialize(ReadOnlyMemory<byte> data, Type type) =>
        JsonSerializer.Deserialize(data.Span, type, _options)
        ?? throw new MessageDeserializationException($"The payload was deserialized to null for type '{type.FullName}'.");
}