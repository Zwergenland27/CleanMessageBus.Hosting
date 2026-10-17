using System.Collections.Concurrent;
using System.Text;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Tests.TestSupport;
using Microsoft.Extensions.DependencyInjection;

namespace CleanMessageBus.Hosting.Tests;

public sealed class MessageConsumptionTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task PublishDomainEventAsync_ShouldInvokeHandler_WhenHandlerIsMapped()
    {
        var handled = new TaskCompletionSource<OrderPlaced>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced e) => { handled.TrySetResult(e); }));
        var @event = new OrderPlaced(Guid.NewGuid());

        await bus.PublishDomainEventAsync(@event);

        Assert.Equal(@event, await handled.Task.WaitAsync(Timeout));
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldInvokeEveryHandler_WhenSeveralHandlersAreMapped()
    {
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
        {
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { first.TrySetResult(); }, o => o.Name = "first");
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { second.TrySetResult(); }, o => o.Name = "second");
        });

        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));

        await Task.WhenAll(first.Task, second.Task).WaitAsync(Timeout);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldProvideEventContext_WhenHandlerRequestsIt()
    {
        var handled = new TaskCompletionSource<EventContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _, EventContext context) => { handled.TrySetResult(context); }));

        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));

        var context = await handled.Task.WaitAsync(Timeout);
        Assert.Equal(Assert.Single(bus.Transport.PublishedMessages).MessageId, context.MessageId);
        Assert.Equal("OrderPlacedHandler", context.Handler.Name);
        Assert.Equal(1, context.Delivery.Attempt);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldInvokeIntegrationHandler_WhenSourceApplicationMatches()
    {
        var handled = new TaskCompletionSource<PaymentReceived>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
            host.MapIntegrationEventHandler<PaymentReceived>(
                "payment-service",
                (PaymentReceived e) => { handled.TrySetResult(e); }));
        var @event = new PaymentReceived(Guid.NewGuid());

        await bus.Transport.PublishAsync(
            TestBus.CreateEnvelope(@event, EventKind.Integration, "payment-service"),
            CancellationToken.None);

        Assert.Equal(@event, await handled.Task.WaitAsync(Timeout));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldNotInvokeIntegrationHandler_WhenSourceApplicationDiffers()
    {
        var foreignCalls = 0;
        var ownHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var bus = await TestBus.StartAsync(host =>
        {
            host.MapIntegrationEventHandler<PaymentReceived>(
                "payment-service",
                (PaymentReceived _) => { Interlocked.Increment(ref foreignCalls); },
                o => o.Name = "ForeignPaymentReceivedHandler");
            host.MapIntegrationEventHandler<PaymentReceived>(
                TestBus.ApplicationName,
                (PaymentReceived _) => { ownHandled.TrySetResult(); },
                o => o.Name = "OwnPaymentReceivedHandler");
        });

        await bus.PublishIntegrationEventAsync(new PaymentReceived(Guid.NewGuid()));

        await ownHandled.Task.WaitAsync(Timeout);
        await Task.Delay(50);
        Assert.Equal(0, Volatile.Read(ref foreignCalls));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldRetry_WhenHandlerFailsOnce()
    {
        var attempts = new ConcurrentQueue<int>();
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _, EventContext context) =>
            {
                attempts.Enqueue(context.Delivery.Attempt);
                if (context.Delivery.Attempt == 1)
                {
                    throw new InvalidOperationException("transient");
                }
            }));

        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));

        await Eventually.AssertAsync(() => attempts.Count == 2, "the handler should be called twice");
        Assert.Equal(new[] { 1, 2 }, attempts.ToArray());
        Assert.Empty(bus.Transport.DeadLetters);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldDeadLetter_WhenRetriesAreExhausted()
    {
        var calls = 0;
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) =>
            {
                Interlocked.Increment(ref calls);
                throw new InvalidOperationException("permanent");
            }));

        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));

        await Eventually.AssertAsync(() => bus.Transport.DeadLetters.Count == 1, "the message should be dead-lettered");
        var deadLetter = Assert.Single(bus.Transport.DeadLetters);
        Assert.Equal("OrderPlacedHandler", deadLetter.HandlerName);
        Assert.Equal(3, deadLetter.Attempt);
        Assert.Equal(ProcessingOutcome.DeadLetter, deadLetter.Result.Outcome);
        Assert.IsType<InvalidOperationException>(deadLetter.Result.Exception);
        Assert.Equal(3, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldDeadLetterImmediately_WhenRetryPolicyIsNone()
    {
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>(
                (OrderPlaced _) => { throw new InvalidOperationException("permanent"); },
                o => o.RetryPolicy = RetryPolicy.None));

        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));

        await Eventually.AssertAsync(() => bus.Transport.DeadLetters.Count == 1, "the message should be dead-lettered");
        Assert.Equal(1, Assert.Single(bus.Transport.DeadLetters).Attempt);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldNotCountAttempt_WhenHandlerThrowsRetryAfterException()
    {
        var attempts = new ConcurrentQueue<int>();
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _, EventContext context) =>
            {
                attempts.Enqueue(context.Delivery.Attempt);
                if (attempts.Count < 3)
                {
                    throw new RetryAfterException(TimeSpan.FromMilliseconds(10));
                }
            }));

        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));

        await Eventually.AssertAsync(() => attempts.Count == 3, "the handler should be called three times");
        Assert.Equal(new[] { 1, 1, 1 }, attempts.ToArray());
        Assert.Empty(bus.Transport.DeadLetters);
    }

    [Fact]
    public async Task ReceiveAsync_ShouldDeadLetterWithoutInvokingHandler_WhenPayloadCannotBeDeserialized()
    {
        var calls = 0;
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { Interlocked.Increment(ref calls); }));

        await bus.Transport.PublishAsync(
            TestBus.CreateEnvelope("OrderPlaced", EventKind.Domain, TestBus.ApplicationName, Encoding.UTF8.GetBytes("{ not json")),
            CancellationToken.None);

        await Eventually.AssertAsync(() => bus.Transport.DeadLetters.Count == 1, "the message should be dead-lettered");
        var deadLetter = Assert.Single(bus.Transport.DeadLetters);
        Assert.Equal(1, deadLetter.Attempt);
        Assert.IsType<MessageDeserializationException>(deadLetter.Result.Exception);
        Assert.Equal(0, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldProcessSequentially_WhenPrefetchCountIsGreaterThanOne()
    {
        var current = 0;
        var maximum = 0;
        var processed = 0;
        await using var bus = await TestBus.StartAsync(host =>
            host.MapDomainEventHandler<OrderPlaced>(async (OrderPlaced _) =>
            {
                var running = Interlocked.Increment(ref current);
                InterlockedMax(ref maximum, running);
                await Task.Delay(20);
                Interlocked.Decrement(ref current);
                Interlocked.Increment(ref processed);
            },
            o => o.PrefetchCount = 5));

        for (var i = 0; i < 5; i++)
        {
            await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));
        }

        await Eventually.AssertAsync(() => Volatile.Read(ref processed) == 5, "all messages should be processed");
        Assert.Equal(1, Volatile.Read(ref maximum));
    }

    [Fact]
    public async Task ReceiveAsync_ShouldUseNewScopePerMessage_WhenHandlerRequestsScopedService()
    {
        var probes = new ConcurrentQueue<ScopedProbe>();
        await using var bus = await TestBus.StartAsync(
            host => host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _, ScopedProbe probe) => { probes.Enqueue(probe); }),
            configureServices: s => s.AddScoped<ScopedProbe>());

        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));
        await bus.PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));

        await Eventually.AssertAsync(
            () => probes.Count == 2 && probes.All(p => p.IsDisposed),
            "both messages should be handled and their scopes disposed");
        var handled = probes.ToArray();
        Assert.NotEqual(handled[0].Id, handled[1].Id);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldDeliverBufferedMessages_WhenPublishedBeforeTransportStarts()
    {
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var host = TestBus.Build();
        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { handled.TrySetResult(); });

        await using (var scope = host.Services.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<IEventPublisher>()
                .PublishDomainEventAsync(new OrderPlaced(Guid.NewGuid()));
        }

        await host.StartAsync();
        try
        {
            await handled.Task.WaitAsync(Timeout);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value
               && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }
}