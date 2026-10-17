using System.Diagnostics;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Tests.TestSupport;

namespace CleanMessageBus.Hosting.Tests;

/// <summary>Publishing events that were stored earlier, as an outbox worker does.</summary>
public sealed class OutboxPublishTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task PublishDomainEventAsync_ShouldUseStoredMessageIdAndTimestamp_WhenOptionsProvideThem()
    {
        await using var bus = await TestBus.StartAsync();
        var options = new PublishOptions { MessageId = Guid.NewGuid(), Timestamp = DateTimeOffset.UnixEpoch };

        await bus.Publisher.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()), options);

        var envelope = Assert.Single(bus.Transport.PublishedMessages);
        Assert.Equal(options.MessageId, envelope.MessageId);
        Assert.Equal(DateTimeOffset.UnixEpoch, envelope.Timestamp);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldContinueStoredTrace_WhenWorkerHasOwnActivity()
    {
        using var collector = new ActivityCollector();
        await using var bus = await TestBus.StartAsync();

        // The handler that raised the event and wrote it to the outbox.
        MessageTraceContext stored;
        ActivityTraceId raisingTraceId;
        ActivitySpanId raisingSpanId;
        using (var raising = ActivityCollector.TestSource.StartActivity("raising-handler"))
        {
            raisingTraceId = raising!.TraceId;
            raisingSpanId = raising.SpanId;
            stored = MessageTraceContext.Capture()!;
        }

        // The outbox worker publishes later within its own, unrelated activity.
        using (ActivityCollector.TestSource.StartActivity("outbox-worker"))
        {
            await bus.Publisher.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()), new PublishOptions { TraceContext = stored });
        }

        var envelope = Assert.Single(bus.Transport.PublishedMessages);
        Assert.Equal(raisingTraceId, MessageTraceContext.FromHeaders(envelope.Headers)!.TryGetParent(out var parent) ? parent.TraceId : default);
        var producer = Assert.Single(collector.StoppedInTrace(raisingTraceId), a => a.Kind == ActivityKind.Producer);
        Assert.Equal(raisingSpanId, producer.ParentSpanId);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldStartNewTrace_WhenOptionsHaveNoTraceContext()
    {
        using var collector = new ActivityCollector();
        await using var bus = await TestBus.StartAsync();

        ActivityTraceId workerTraceId;
        using (var worker = ActivityCollector.TestSource.StartActivity("outbox-worker"))
        {
            workerTraceId = worker!.TraceId;
            await bus.Publisher.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()), new PublishOptions());
        }

        var envelope = Assert.Single(bus.Transport.PublishedMessages);
        Assert.True(MessageTraceContext.FromHeaders(envelope.Headers)!.TryGetParent(out var parent));
        Assert.NotEqual(workerTraceId, parent.TraceId);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldContinueStoredTraceInHandler_WhenEventIsConsumed()
    {
        var handled = new TaskCompletionSource<(ActivityTraceId? TraceId, string? Tenant)>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var collector = new ActivityCollector();
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) =>
            {
                handled.TrySetResult((Activity.Current?.TraceId, Activity.Current?.GetBaggageItem("tenant")));
            }));

        MessageTraceContext stored;
        ActivityTraceId raisingTraceId;
        using (var raising = ActivityCollector.TestSource.StartActivity("raising-handler"))
        {
            raising!.AddBaggage("tenant", "tenant-1");
            raisingTraceId = raising.TraceId;
            stored = MessageTraceContext.Capture()!;
        }

        await bus.Publisher.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()), new PublishOptions { TraceContext = stored });

        var (traceId, tenant) = await handled.Task.WaitAsync(Timeout);
        Assert.Equal(raisingTraceId, traceId);
        Assert.Equal("tenant-1", tenant);
    }
}