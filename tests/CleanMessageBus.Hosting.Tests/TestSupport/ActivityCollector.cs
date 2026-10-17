using System.Collections.Concurrent;
using System.Diagnostics;
using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting.Tests.TestSupport;

/// <summary>
/// Records stopped activities of CleanMessageBus and of the test source.
/// </summary>
internal sealed class ActivityCollector : IDisposable
{
    private const string TestSourceName = "CleanMessageBus.Hosting.Tests";

    public static readonly ActivitySource TestSource = new(TestSourceName);

    private readonly ConcurrentQueue<Activity> stopped = new();
    private readonly ActivityListener listener;

    public ActivityCollector()
    {
        listener = new ActivityListener
        {
            ShouldListenTo = source =>
                source.Name is CleanMessageBusDiagnostics.SourceName or TestSourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => stopped.Enqueue(activity),
        };

        ActivitySource.AddActivityListener(listener);
    }

    public IReadOnlyList<Activity> StoppedInTrace(ActivityTraceId traceId) =>
        stopped.Where(activity => activity.TraceId == traceId).ToArray();

    public void Dispose() => listener.Dispose();
}