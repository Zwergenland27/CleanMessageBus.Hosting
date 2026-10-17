using System.Text.Json;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Tests.TestSupport;

namespace CleanMessageBus.Hosting.Tests;

public sealed class EventPublisherTests
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task PublishDomainEventAsync_ShouldSendEnvelopeToTransport_WhenEventIsPublished()
    {
        await using var bus = await TestBus.StartAsync();
        var @event = new OrderPlaced(Guid.NewGuid());

        await bus.PublishDomainEventAsync(@event);

        var envelope = Assert.Single(bus.Transport.PublishedMessages);
        Assert.NotEqual(Guid.Empty, envelope.MessageId);
        Assert.Equal("OrderPlaced", envelope.EventName);
        Assert.Equal(EventKind.Domain, envelope.Kind);
        Assert.Equal(TestBus.ApplicationName, envelope.SourceApplication);
        Assert.Equal("application/json", envelope.ContentType);
        Assert.Equal(@event, JsonSerializer.Deserialize<OrderPlaced>(envelope.Body.Span, Json));
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldUseRuntimeType_WhenEventIsPassedAsInterface()
    {
        await using var bus = await TestBus.StartAsync();
        IDomainEvent @event = new OrderShipped(Guid.NewGuid());

        await bus.PublishDomainEventAsync(@event);

        var envelope = Assert.Single(bus.Transport.PublishedMessages);
        Assert.Equal("order-shipped", envelope.EventName);
        Assert.Equal(@event, JsonSerializer.Deserialize<OrderShipped>(envelope.Body.Span, Json));
    }

    [Fact]
    public async Task PublishIntegrationEventAsync_ShouldSendIntegrationEnvelope_WhenEventIsPublished()
    {
        await using var bus = await TestBus.StartAsync();

        await bus.PublishIntegrationEventAsync(new PaymentReceived(Guid.NewGuid()));

        var envelope = Assert.Single(bus.Transport.PublishedMessages);
        Assert.Equal("PaymentReceived", envelope.EventName);
        Assert.Equal(EventKind.Integration, envelope.Kind);
        Assert.Equal(TestBus.ApplicationName, envelope.SourceApplication);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldCreateNewMessageId_WhenEventIsPublishedTwice()
    {
        await using var bus = await TestBus.StartAsync();
        var @event = new OrderPlaced(Guid.NewGuid());

        await bus.PublishDomainEventAsync(@event);
        await bus.PublishDomainEventAsync(@event);

        var ids = bus.Transport.PublishedMessages.Select(m => m.MessageId).ToArray();
        Assert.NotEqual(ids[0], ids[1]);
    }

    [Fact]
    public async Task PublishDomainEventAsync_ShouldThrow_WhenEventIsNull()
    {
        await using var bus = await TestBus.StartAsync();

        await Assert.ThrowsAsync<ArgumentNullException>(() => bus.PublishDomainEventAsync<OrderPlaced>(null!));
    }
}