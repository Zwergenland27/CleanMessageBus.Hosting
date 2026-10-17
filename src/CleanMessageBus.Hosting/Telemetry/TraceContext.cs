using System.Diagnostics;
using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting.Telemetry;

/// <summary>
/// W3C trace context and baggage propagation through message headers.
/// </summary>
internal static class TraceContext
{
    public static void Inject(Activity? activity, IDictionary<string, string> headers) =>
        MessageTraceContext.Capture(activity)?.WriteTo(headers);

    public static bool TryExtractParent(IReadOnlyDictionary<string, string> headers, out ActivityContext parent)
    {
        parent = default;
        return MessageTraceContext.FromHeaders(headers) is { } context && context.TryGetParent(out parent);
    }

    public static void ApplyBaggage(Activity? activity, IReadOnlyDictionary<string, string> headers)
    {
        if (activity is not null && MessageTraceContext.FromHeaders(headers) is { } context)
        {
            ApplyBaggage(activity, context);
        }
    }

    public static void ApplyBaggage(Activity activity, MessageTraceContext context)
    {
        foreach (var (key, value) in context.GetBaggage())
        {
            activity.AddBaggage(key, value);
        }
    }
}