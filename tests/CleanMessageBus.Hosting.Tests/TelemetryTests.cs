using System.Diagnostics;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Tests.TestSupport;

namespace CleanMessageBus.Hosting.Tests;

public sealed class TelemetryTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task PublishDomainEventAsync_ShouldWriteTraceParentHeader_WhenActivityIsActive()
    {
        using var collector = new ActivityCollector();
        await using var bus = await TestBus.StartAsync();

        ActivityTraceId traceId;
        using (var root = ActivityCollector.TestSource.StartActivity("test-root"))
        {
            traceId = root!.TraceId;
            await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));
        }

        var envelope = Assert.Single(bus.Transport.PublishedMessages);
        Assert.True(envelope.Headers.TryGetValue(MessageHeaderNames.TraceParent, out var traceParent));
        Assert.Contains(traceId.ToHexString(), traceParent!);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldContinueTraceInHandler_WhenPublishedWithinActivity()
    {
        using var collector = new ActivityCollector();
        var handled = new TaskCompletionSource<ActivityTraceId?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { handled.TrySetResult(Activity.Current?.TraceId); }));

        ActivityTraceId traceId;
        using (var root = ActivityCollector.TestSource.StartActivity("test-root"))
        {
            traceId = root!.TraceId;
            await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));
        }

        Assert.Equal(traceId, await handled.Task.WaitAsync(Timeout));
        await Eventually.AssertAsync(
            () => collector.StoppedInTrace(traceId).Any(a => a.Kind == ActivityKind.Consumer),
            "the consumer activity should be stopped");

        var activities = collector.StoppedInTrace(traceId);
        var producer = Assert.Single(activities, a => a.Kind == ActivityKind.Producer);
        var consumer = Assert.Single(activities, a => a.Kind == ActivityKind.Consumer);
        Assert.Equal(producer.SpanId, consumer.ParentSpanId);
        Assert.Equal("send OrderPlaced", producer.DisplayName);
        Assert.Equal("process OrderPlaced", consumer.DisplayName);
        Assert.Equal("in-memory", consumer.GetTagItem("messaging.system"));
        Assert.Equal("OrderPlacedHandler", consumer.GetTagItem("messaging.consumer.group.name"));
        Assert.Equal("succeeded", consumer.GetTagItem("cleanmessagebus.outcome"));
        Assert.Equal(ActivityStatusCode.Unset, consumer.Status);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldPropagateBaggage_WhenRootActivityHasBaggage()
    {
        using var collector = new ActivityCollector();
        var handled = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) =>
            {
                handled.TrySetResult(Activity.Current?.GetBaggageItem("tenant"));
            }));

        using (var root = ActivityCollector.TestSource.StartActivity("test-root"))
        {
            root!.AddBaggage("tenant", "tenant 1,a=b");
            await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));
        }

        Assert.Equal("tenant 1,a=b", await handled.Task.WaitAsync(Timeout));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldMarkConsumerActivityAsError_WhenMessageIsDeadLettered()
    {
        using var collector = new ActivityCollector();
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>(
                (OrderPlaced _) => { throw new InvalidOperationException("permanent"); },
                o => o.RetryPolicy = RetryPolicy.None));

        ActivityTraceId traceId;
        using (var root = ActivityCollector.TestSource.StartActivity("test-root"))
        {
            traceId = root!.TraceId;
            await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));
        }

        await Eventually.AssertAsync(
            () => collector.StoppedInTrace(traceId).Any(a => a.Kind == ActivityKind.Consumer),
            "the consumer activity should be stopped");

        var consumer = Assert.Single(collector.StoppedInTrace(traceId), a => a.Kind == ActivityKind.Consumer);
        Assert.Equal(ActivityStatusCode.Error, consumer.Status);
        Assert.Equal("dead_letter", consumer.GetTagItem("cleanmessagebus.outcome"));
        Assert.Equal(typeof(InvalidOperationException).FullName, consumer.GetTagItem("error.type"));
        Assert.Contains(consumer.Events, e => e.Name == "exception");
    }

    [Fact]
    public async Task ReceiveAsync_ShouldStartNewTrace_WhenMessageHasNoTraceContext()
    {
        using var collector = new ActivityCollector();
        var handled = new TaskCompletionSource<Activity?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { handled.TrySetResult(Activity.Current); }));

        using (ActivityCollector.TestSource.StartActivity("unrelated-ambient-activity"))
        {
            await bus.Transport.PublishAsync(
                TestBus.CreateEnvelope(new OrderPlaced(Guid.NewGuid()), EventKind.Domain, TestBus.ApplicationName),
                CancellationToken.None);
        }

        var consumer = await handled.Task.WaitAsync(Timeout);
        Assert.NotNull(consumer);
        Assert.Equal(ActivityKind.Consumer, consumer.Kind);
        Assert.Equal(default, consumer.ParentSpanId);
    }
}