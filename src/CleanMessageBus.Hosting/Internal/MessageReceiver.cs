using System.Diagnostics;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Abstractions.Transport;
using CleanMessageBus.Hosting.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CleanMessageBus.Hosting.Internal;

/// <summary>
/// Processes a received message: telemetry → deserialization → new DI scope → handler,
/// and translates failures into a <see cref="ProcessingResult"/> for the transport.
/// </summary>
internal sealed class MessageReceiver : IMessageReceiver
{
    private readonly EventHandlerRegistry _registry;
    private readonly IMessageSerializer _serializer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly TransportAccessor _transport;
    private readonly ILogger<MessageReceiver> _logger;

    public MessageReceiver(
        EventHandlerRegistry registry,
        IMessageSerializer serializer,
        IServiceScopeFactory scopeFactory,
        TransportAccessor transport,
        ILogger<MessageReceiver> logger)
    {
        _registry = registry;
        _serializer = serializer;
        _scopeFactory = scopeFactory;
        _transport = transport;
        _logger = logger;
    }

    public async ValueTask<ProcessingResult> ReceiveAsync(
        MessageEnvelope envelope,
        EventHandlerDescriptor handler,
        DeliveryInfo delivery,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(handler);

        // The consumer span must only be parented by the trace context of the message,
        // not by whatever activity happens to be ambient in the transport's receive loop.
        Activity.Current = null;

        var system = _transport.SystemName;
        using var activity = MessagingTelemetry.StartProcess(envelope, handler, delivery, system);
        var started = Stopwatch.GetTimestamp();

        var result = await ProcessAsync(envelope, handler, delivery, cancellationToken).ConfigureAwait(false);

        MessagingTelemetry.CompleteProcess(activity, envelope, handler, result, system, Stopwatch.GetElapsedTime(started));
        return result;
    }

    private async ValueTask<ProcessingResult> ProcessAsync(
        MessageEnvelope envelope,
        EventHandlerDescriptor handler,
        DeliveryInfo delivery,
        CancellationToken cancellationToken)
    {
        try
        {
            var @event = Deserialize(envelope, handler);
            var invoker = _registry.GetInvoker(handler.Name);
            var context = new EventContext(envelope, handler, delivery);

            var scope = _scopeFactory.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                await invoker(scope.ServiceProvider, context, @event, cancellationToken).ConfigureAwait(false);
            }

            return ProcessingResult.Succeeded;
        }
        catch (RetryAfterException exception)
        {
            _logger.LogInformation(
                "Handler {Handler} requested a retry of message {MessageId} after {Delay}.",
                handler.Name,
                envelope.MessageId,
                exception.Delay);
            return ProcessingResult.Retry(exception.Delay, exception, countsAsFailedAttempt: false);
        }
        catch (MessageDeserializationException exception)
        {
            _logger.LogError(
                exception,
                "Message {MessageId} ({EventName}) could not be deserialized for handler {Handler} and is dead-lettered.",
                envelope.MessageId,
                envelope.EventName,
                handler.Name);
            return ProcessingResult.DeadLetter(exception, "The message could not be deserialized.");
        }
        catch (OperationCanceledException exception) when (cancellationToken.IsCancellationRequested)
        {
            return ProcessingResult.Retry(TimeSpan.Zero, exception, countsAsFailedAttempt: false);
        }
        catch (Exception exception)
        {
            if (handler.RetryPolicy.TryGetRetryDelay(delivery.Attempt, out var delay))
            {
                _logger.LogWarning(
                    exception,
                    "Handler {Handler} failed to process message {MessageId} (attempt {Attempt}). Retrying in {Delay}.",
                    handler.Name,
                    envelope.MessageId,
                    delivery.Attempt,
                    delay);
                return ProcessingResult.Retry(delay, exception);
            }

            _logger.LogError(
                exception,
                "Handler {Handler} failed to process message {MessageId} after {Attempt} attempt(s). The message is dead-lettered.",
                handler.Name,
                envelope.MessageId,
                delivery.Attempt);
            return ProcessingResult.DeadLetter(exception, $"The handler failed after {delivery.Attempt} attempt(s).");
        }
    }

    private object Deserialize(MessageEnvelope envelope, EventHandlerDescriptor handler)
    {
        try
        {
            return _serializer.Deserialize(envelope.Body, handler.EventType);
        }
        catch (Exception exception) when (exception is not MessageDeserializationException)
        {
            throw new MessageDeserializationException(
                $"Message '{envelope.MessageId}' could not be deserialized to '{handler.EventType.FullName}'.",
                exception);
        }
    }
}