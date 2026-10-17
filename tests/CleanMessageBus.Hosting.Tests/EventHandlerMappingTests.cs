using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Tests.TestSupport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CleanMessageBus.Hosting.Tests;

public sealed class EventHandlerMappingTests
{
    [Fact]
    public void MapDomainEventHandler_ShouldUseEventNameAndApplicationDefaults_WhenNoOptionsAreGiven()
    {
        using var host = TestBus.Build();

        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { });

        var handler = Assert.Single(Handlers(host));
        Assert.Equal("OrderPlacedHandler", handler.Name);
        Assert.Equal("OrderPlaced", handler.EventName);
        Assert.Equal(typeof(OrderPlaced), handler.EventType);
        Assert.Equal(EventKind.Domain, handler.Kind);
        Assert.Equal(TestBus.ApplicationName, handler.SourceApplication);
        Assert.Equal(1, handler.PrefetchCount);
        Assert.Equal(2, handler.RetryPolicy.MaxRetries);
        Assert.Empty(handler.Properties);
    }

    [Fact]
    public void MapDomainEventHandler_ShouldUseEventNameAttribute_WhenEventDefinesIt()
    {
        using var host = TestBus.Build();

        host.MapDomainEventHandler<OrderShipped>((OrderShipped _) => { });

        var handler = Assert.Single(Handlers(host));
        Assert.Equal("order-shippedHandler", handler.Name);
        Assert.Equal("order-shipped", handler.EventName);
    }

    [Fact]
    public void MapIntegrationEventHandler_ShouldUseEventNameWithHandlerSuffix_WhenNoNameIsGiven()
    {
        using var host = TestBus.Build();

        host.MapIntegrationEventHandler<PaymentReceived>("payment-service", (PaymentReceived _) => { });

        var handler = Assert.Single(Handlers(host));
        Assert.Equal("PaymentReceivedHandler", handler.Name);
        Assert.Equal("payment-service", handler.SourceApplication);
        Assert.Equal(EventKind.Integration, handler.Kind);
    }

    [Fact]
    public void MapDomainEventHandler_ShouldApplyOptions_WhenOptionsAreConfigured()
    {
        using var host = TestBus.Build();

        host.MapDomainEventHandler<OrderPlaced>(
            (OrderPlaced _) => { },
            options =>
            {
                options.Name = "send-confirmation";
                options.PrefetchCount = 4;
                options.RetryPolicy = RetryPolicy.None;
                options.Properties["single-active-consumer"] = true;
            });

        var handler = Assert.Single(Handlers(host));
        Assert.Equal("send-confirmation", handler.Name);
        Assert.Equal(4, handler.PrefetchCount);
        Assert.Same(RetryPolicy.None, handler.RetryPolicy);
        Assert.True(handler.Properties["single-active-consumer"] is true);
    }

    [Fact]
    public void MapDomainEventHandler_ShouldAllowSeveralHandlers_WhenNamesAreUnique()
    {
        using var host = TestBus.Build();

        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { }, o => o.Name = "first");
        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { }, o => o.Name = "second");

        Assert.Equal(new[] { "first", "second" }, Handlers(host).Select(h => h.Name).ToArray());
    }

    [Fact]
    public void MapDomainEventHandler_ShouldThrow_WhenHandlerNameIsAlreadyMapped()
    {
        using var host = TestBus.Build();
        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { });

        Assert.Throws<InvalidOperationException>(() => host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { }));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(EventHandlerDescriptor.MaxPrefetchCount + 1)]
    public void MapDomainEventHandler_ShouldThrow_WhenPrefetchCountIsOutOfRange(int prefetchCount)
    {
        using var host = TestBus.Build();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { }, o => o.PrefetchCount = prefetchCount));
    }

    [Fact]
    public void MapDomainEventHandler_ShouldThrow_WhenEventTypeIsAbstract()
    {
        using var host = TestBus.Build();

        Assert.Throws<ArgumentException>(() =>
            host.MapDomainEventHandler<IDomainEvent>((IDomainEvent _) => { }));
    }

    [Fact]
    public void MapIntegrationEventHandler_ShouldThrow_WhenSourceApplicationIsEmpty()
    {
        using var host = TestBus.Build();

        Assert.Throws<ArgumentException>(() =>
            host.MapIntegrationEventHandler<PaymentReceived>(" ", (PaymentReceived _) => { }));
    }

    [Fact]
    public async Task MapDomainEventHandler_ShouldThrow_WhenHostIsAlreadyStarted()
    {
        await using var bus = await TestBus.StartAsync();

        Assert.Throws<InvalidOperationException>(() =>
            bus.Host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { }));
    }

    [Fact]
    public void MapDomainEventHandler_ShouldThrow_WhenMessageBusIsNotRegistered()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        using var host = builder.Build();

        Assert.Throws<InvalidOperationException>(() =>
            host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { }));
    }

    private static IReadOnlyList<EventHandlerDescriptor> Handlers(IHost host) =>
        host.Services.GetRequiredService<EventHandlerRegistry>().Handlers;
}