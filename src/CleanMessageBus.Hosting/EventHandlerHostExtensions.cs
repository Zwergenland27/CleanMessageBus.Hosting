using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CleanMessageBus.Hosting;

/// <summary>
/// Maps event handlers, similar to Minimal API endpoints.
/// </summary>
public static class EventHandlerHostExtensions
{
    /// <summary>
    /// Maps a handler for a domain event of the current application.
    /// </summary>
    /// <param name="host">The built host (e.g. <c>WebApplication</c>), before it is started.</param>
    /// <param name="handler">
    /// Delegate whose first parameter is the event. Further parameters may be <see cref="EventContext"/>,
    /// <see cref="MessageEnvelope"/>, <see cref="CancellationToken"/> or services resolved from the handler scope.
    /// </param>
    /// <param name="configure">Optional handler options.</param>
    public static IHost MapDomainEventHandler<TEvent>(
        this IHost host,
        Delegate handler,
        Action<EventHandlerOptions>? configure = null)
        where TEvent : IDomainEvent
    {
        ArgumentNullException.ThrowIfNull(host);

        var settings = GetSettings(host);
        return Map(host, typeof(TEvent), EventKind.Domain, settings.ApplicationName, handler, configure);
    }

    /// <summary>
    /// Maps a handler for an integration event published by <paramref name="sourceApplication"/>.
    /// </summary>
    /// <param name="host">The built host (e.g. <c>WebApplication</c>), before it is started.</param>
    /// <param name="sourceApplication">Name of the application that publishes the event.</param>
    /// <param name="handler">
    /// Delegate whose first parameter is the event. Further parameters may be <see cref="EventContext"/>,
    /// <see cref="MessageEnvelope"/>, <see cref="CancellationToken"/> or services resolved from the handler scope.
    /// </param>
    /// <param name="configure">Optional handler options.</param>
    public static IHost MapIntegrationEventHandler<TEvent>(
        this IHost host,
        string sourceApplication,
        Delegate handler,
        Action<EventHandlerOptions>? configure = null)
        where TEvent : IIntegrationEvent
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceApplication);

        return Map(host, typeof(TEvent), EventKind.Integration, sourceApplication, handler, configure);
    }

    private static IHost Map(
        IHost host,
        Type eventType,
        EventKind kind,
        string sourceApplication,
        Delegate handler,
        Action<EventHandlerOptions>? configure)
    {
        ArgumentNullException.ThrowIfNull(handler);

        if (eventType.IsAbstract || eventType.IsInterface)
        {
            throw new ArgumentException($"Handlers must be mapped for concrete event types, but '{eventType.FullName}' is abstract.");
        }

        var settings = GetSettings(host);
        var registry = host.Services.GetRequiredService<EventHandlerRegistry>();

        var options = new EventHandlerOptions();
        configure?.Invoke(options);

        var eventName = EventNames.For(eventType);
        var name = options.Name ?? $"{eventName}Handler";

        var descriptor = new EventHandlerDescriptor(
            name,
            eventType,
            eventName,
            kind,
            sourceApplication,
            options.PrefetchCount ?? settings.DefaultPrefetchCount,
            options.RetryPolicy ?? settings.DefaultRetryPolicy,
            new Dictionary<string, object?>(options.Properties, StringComparer.Ordinal));

        var invoker = HandlerInvokerFactory.Create(eventType, handler, host.Services);
        registry.Add(descriptor, invoker);

        return host;
    }

    private static MessageBusSettings GetSettings(IHost host) =>
        host.Services.GetService<MessageBusSettings>()
        ?? throw new InvalidOperationException(
            "CleanMessageBus is not registered. Call services.AddCleanMessageBus(...) before building the host.");
}