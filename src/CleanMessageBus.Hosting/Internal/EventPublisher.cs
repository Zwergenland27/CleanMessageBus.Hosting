using System.Diagnostics;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Telemetry;

namespace CleanMessageBus.Hosting.Internal;

/// <summary>
/// Creates the envelope (name, trace headers, payload) and hands it to the transport.
/// </summary>
internal sealed class EventPublisher : IEventPublisher
{
    private readonly MessageBusSettings _settings;
    private readonly IMessageSerializer _serializer;
    private readonly TransportAccessor _transport;

    public EventPublisher(MessageBusSettings settings, IMessageSerializer serializer, TransportAccessor transport)
    {
        _settings = settings;
        _serializer = serializer;
        _transport = transport;
    }

    public ValueTask PublishDomainEventAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IDomainEvent =>
        PublishAsync(@event, EventKind.Domain, options: null, cancellationToken);

    public ValueTask PublishDomainEventAsync<TEvent>(TEvent @event, PublishOptions options, CancellationToken cancellationToken = default)
        where TEvent : IDomainEvent =>
        PublishAsync(@event, EventKind.Domain, options ?? throw new ArgumentNullException(nameof(options)), cancellationToken);

    public ValueTask PublishIntegrationEventAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : IIntegrationEvent =>
        PublishAsync(@event, EventKind.Integration, options: null, cancellationToken);

    public ValueTask PublishIntegrationEventAsync<TEvent>(TEvent @event, PublishOptions options, CancellationToken cancellationToken = default)
        where TEvent : IIntegrationEvent =>
        PublishAsync(@event, EventKind.Integration, options ?? throw new ArgumentNullException(nameof(options)), cancellationToken);

    private async ValueTask PublishAsync<TEvent>(TEvent? @event, EventKind kind, PublishOptions? options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(@event);

        // The runtime type decides the name, so events passed as IDomainEvent are routed correctly.
        var eventType = @event.GetType();
        var eventName = EventNames.For(eventType);
        var messageId = options?.MessageId ?? Guid.NewGuid();
        var target = _transport.Transport;

        var parent = default(ActivityContext);
        if (options is not null)
        {
            // Published later (e.g. by an outbox worker): the stored context is the only parent.
            // The ambient activity of the publishing component has nothing to do with the event.
            Activity.Current = null;
            if (options.TraceContext is { } stored && stored.TryGetParent(out var storedParent))
            {
                parent = storedParent;
            }
        }

        using var activity = MessagingTelemetry.StartSend(eventName, kind, messageId, _settings.ApplicationName, target.Name, parent);
        if (activity is not null && options?.TraceContext is { } storedContext)
        {
            TraceContext.ApplyBaggage(activity, storedContext);
        }

        try
        {
            var headers = new Dictionary<string, string>(StringComparer.Ordinal);

            // Without a listener on the CleanMessageBus source there is no send activity;
            // the trace is then continued from the current activity or the stored context.
            var propagated = activity is not null
                ? MessageTraceContext.Capture(activity)
                : options is null ? MessageTraceContext.Capture(Activity.Current) : options.TraceContext;
            propagated?.WriteTo(headers);

            var envelope = new MessageEnvelope
            {
                MessageId = messageId,
                EventName = eventName,
                Kind = kind,
                SourceApplication = _settings.ApplicationName,
                Body = _serializer.Serialize(@event, eventType),
                ContentType = _serializer.ContentType,
                Timestamp = options?.Timestamp ?? DateTimeOffset.UtcNow,
                Headers = headers.AsReadOnly(),
            };

            await target.PublishAsync(envelope, cancellationToken).ConfigureAwait(false);
            MessagingTelemetry.RecordSent(eventName, kind, target.Name, error: null);
        }
        catch (Exception exception)
        {
            MessagingTelemetry.SetError(activity, exception);
            MessagingTelemetry.RecordSent(eventName, kind, target.Name, exception);
            throw;
        }
    }
}