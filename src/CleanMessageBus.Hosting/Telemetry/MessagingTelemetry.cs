using System.Diagnostics;
using System.Diagnostics.Metrics;
using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting.Telemetry;

/// <summary>
/// Activities and metrics following the OpenTelemetry semantic conventions for messaging.
/// Uses only System.Diagnostics, so no OpenTelemetry package is required.
/// </summary>
internal static class MessagingTelemetry
{
    private static readonly ActivitySource Source = CleanMessageBusDiagnostics.ActivitySource;

    private static readonly Meter Meter = new(
        CleanMessageBusDiagnostics.MeterName,
        typeof(MessagingTelemetry).Assembly.GetName().Version?.ToString());

    private static readonly Counter<long> SentMessages = Meter.CreateCounter<long>(
        "messaging.client.sent.messages", "{message}", "Number of messages sent.");

    private static readonly Counter<long> ConsumedMessages = Meter.CreateCounter<long>(
        "messaging.client.consumed.messages", "{message}", "Number of messages delivered to handlers.");

    private static readonly Histogram<double> ProcessDuration = Meter.CreateHistogram<double>(
        "messaging.process.duration", "s", "Duration of processing a message by a handler.");

    // parent: explicit parent; default means Activity.Current.
    public static Activity? StartSend(
        string eventName,
        EventKind kind,
        Guid messageId,
        string sourceApplication,
        string system,
        ActivityContext parent)
    {
        var activity = Source.StartActivity($"send {eventName}", ActivityKind.Producer, parent);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("messaging.system", system);
            activity.SetTag("messaging.operation.type", "send");
            activity.SetTag("messaging.operation.name", "send");
            activity.SetTag("messaging.destination.name", eventName);
            activity.SetTag("messaging.message.id", messageId.ToString());
            activity.SetTag("cleanmessagebus.event.kind", KindTag(kind));
            activity.SetTag("cleanmessagebus.source_application", sourceApplication);
        }

        return activity;
    }

    public static void RecordSent(string eventName, EventKind kind, string system, Exception? error)
    {
        var tags = new TagList
        {
            { "messaging.system", system },
            { "messaging.operation.name", "send" },
            { "messaging.destination.name", eventName },
            { "cleanmessagebus.event.kind", KindTag(kind) },
        };

        if (error is not null)
        {
            tags.Add("error.type", error.GetType().FullName);
        }

        SentMessages.Add(1, tags);
    }

    /// <summary>
    /// Starts the consumer activity. The parent is taken exclusively from the message headers.
    /// </summary>
    /// <remarks>
    /// Must be called from an async method that has cleared <see cref="Activity.Current"/>.
    /// </remarks>
    public static Activity? StartProcess(MessageEnvelope envelope, EventHandlerDescriptor handler, DeliveryInfo delivery, string system)
    {
        var parent = TraceContext.TryExtractParent(envelope.Headers, out var parentContext) ? parentContext : default;

        var activity = Source.StartActivity($"process {envelope.EventName}", ActivityKind.Consumer, parent);
        if (activity is null)
        {
            return null;
        }

        TraceContext.ApplyBaggage(activity, envelope.Headers);

        if (activity.IsAllDataRequested)
        {
            activity.SetTag("messaging.system", system);
            activity.SetTag("messaging.operation.type", "process");
            activity.SetTag("messaging.operation.name", "process");
            activity.SetTag("messaging.destination.name", envelope.EventName);
            activity.SetTag("messaging.consumer.group.name", handler.Name);
            activity.SetTag("messaging.message.id", envelope.MessageId.ToString());
            activity.SetTag("cleanmessagebus.event.kind", KindTag(envelope.Kind));
            activity.SetTag("cleanmessagebus.source_application", envelope.SourceApplication);
            activity.SetTag("cleanmessagebus.delivery.attempt", delivery.Attempt);
        }

        return activity;
    }

    public static void CompleteProcess(
        Activity? activity,
        MessageEnvelope envelope,
        EventHandlerDescriptor handler,
        ProcessingResult result,
        string system,
        TimeSpan duration)
    {
        var outcome = result.Outcome switch
        {
            ProcessingOutcome.Succeeded => "succeeded",
            ProcessingOutcome.Retry => "retry",
            _ => "dead_letter",
        };

        if (activity is not null)
        {
            activity.SetTag("cleanmessagebus.outcome", outcome);
            if (result.Exception is not null)
            {
                SetError(activity, result.Exception);
            }
            else if (result.Outcome == ProcessingOutcome.DeadLetter)
            {
                activity.SetStatus(ActivityStatusCode.Error, result.Reason);
            }
        }

        var tags = new TagList
        {
            { "messaging.system", system },
            { "messaging.operation.name", "process" },
            { "messaging.destination.name", envelope.EventName },
            { "messaging.consumer.group.name", handler.Name },
            { "cleanmessagebus.outcome", outcome },
        };

        if (result.Exception is not null)
        {
            tags.Add("error.type", result.Exception.GetType().FullName);
        }

        ConsumedMessages.Add(1, tags);
        ProcessDuration.Record(duration.TotalSeconds, tags);
    }

    public static void SetError(Activity? activity, Exception exception)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.SetTag("error.type", exception.GetType().FullName);

        if (activity.IsAllDataRequested)
        {
            activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                { "exception.type", exception.GetType().FullName },
                { "exception.message", exception.Message },
                { "exception.stacktrace", exception.ToString() },
            }));
        }
    }

    private static string KindTag(EventKind kind) => kind == EventKind.Domain ? "domain" : "integration";
}