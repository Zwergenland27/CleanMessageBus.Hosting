using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting;

/// <summary>
/// Options of a single mapped event handler.
/// </summary>
public sealed class EventHandlerOptions
{
    /// <summary>
    /// Unique handler name within the application; transports derive queue names from it, so it must stay stable.
    /// Defaults to <c>{eventName}Handler</c>; transports prefix it with the name of the consuming application.
    /// Required if several handlers handle events with the same name.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Maximum number of unacknowledged messages fetched from the broker for this handler.
    /// Messages are always processed one after another; with 1 (default) the next message is only fetched once
    /// the previous one is done, so the remaining messages stay available for other instances.
    /// Defaults to <see cref="MessageBusBuilder.DefaultPrefetchCount"/>.
    /// </summary>
    public int? PrefetchCount { get; set; }

    /// <summary>
    /// Retry behaviour. Defaults to <see cref="MessageBusBuilder.DefaultRetryPolicy"/>.
    /// </summary>
    public RetryPolicy? RetryPolicy { get; set; }

    /// <summary>
    /// Transport-specific settings, usually set through extension methods of a transport package.
    /// </summary>
    public IDictionary<string, object?> Properties { get; } = new Dictionary<string, object?>(StringComparer.Ordinal);
}