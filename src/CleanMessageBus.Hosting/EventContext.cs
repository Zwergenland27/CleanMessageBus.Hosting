using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting;

/// <summary>
/// Metadata of the message currently being handled. Can be requested as handler parameter.
/// </summary>
public sealed class EventContext
{
    /// <summary>
    /// Creates the context.
    /// </summary>
    public EventContext(MessageEnvelope envelope, EventHandlerDescriptor handler, DeliveryInfo delivery)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(handler);

        Envelope = envelope;
        Handler = handler;
        Delivery = delivery;
    }

    /// <summary>
    /// The received envelope.
    /// </summary>
    public MessageEnvelope Envelope { get; }

    /// <summary>
    /// The handler processing the message.
    /// </summary>
    public EventHandlerDescriptor Handler { get; }

    /// <summary>
    /// Information about the current delivery.
    /// </summary>
    public DeliveryInfo Delivery { get; }

    /// <summary>
    /// Shortcut for <see cref="MessageEnvelope.MessageId"/>.
    /// </summary>
    public Guid MessageId => Envelope.MessageId;

    /// <summary>
    /// Shortcut for <see cref="MessageEnvelope.Headers"/>.
    /// </summary>
    public IReadOnlyDictionary<string, string> Headers => Envelope.Headers;
}