using System.Text.Json;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CleanMessageBus.Hosting.Tests.TestSupport;

/// <summary>
/// A host with the message bus on the in-memory transport.
/// </summary>
internal sealed class TestBus : IAsyncDisposable
{
    public const string ApplicationName = "test-app";

    private TestBus(IHost host) => Host = host;

    public IHost Host { get; }

    public InMemoryTransport Transport => Host.Services.GetRequiredService<InMemoryTransport>();

    public static IHost Build(
        Action<MessageBusBuilder>? configureBus = null,
        Action<IServiceCollection>? configureServices = null)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        configureServices?.Invoke(builder.Services);

        builder.Services.AddCleanMessageBus(bus =>
        {
            bus.ApplicationName = ApplicationName;
            bus.DefaultRetryPolicy = RetryPolicy.Fixed(2, TimeSpan.Zero);
            bus.UseInMemoryTransport();
            configureBus?.Invoke(bus);
        });

        return builder.Build();
    }

    public static async Task<TestBus> StartAsync(
        Action<IHost>? map = null,
        Action<MessageBusBuilder>? configureBus = null,
        Action<IServiceCollection>? configureServices = null)
    {
        var host = Build(configureBus, configureServices);
        map?.Invoke(host);
        await host.StartAsync();
        return new TestBus(host);
    }

    public IEventPublisher Publisher => Host.Services.GetRequiredService<IEventPublisher>();

    public Task PublishDomainEventAsync<TEvent>(TEvent @event)
        where TEvent : IDomainEvent =>
        Publisher.PublishDomainEventAsync(@event).AsTask();

    public Task PublishIntegrationEventAsync<TEvent>(TEvent @event)
        where TEvent : IIntegrationEvent =>
        Publisher.PublishIntegrationEventAsync(@event).AsTask();

    public static MessageEnvelope CreateEnvelope<TEvent>(TEvent @event, EventKind kind, string sourceApplication) =>
        CreateEnvelope(
            EventNames.For<TEvent>(),
            kind,
            sourceApplication,
            JsonSerializer.SerializeToUtf8Bytes(@event, new JsonSerializerOptions(JsonSerializerDefaults.Web)));

    public static MessageEnvelope CreateEnvelope(string eventName, EventKind kind, string sourceApplication, byte[] body) =>
        new()
        {
            MessageId = Guid.NewGuid(),
            EventName = eventName,
            Kind = kind,
            SourceApplication = sourceApplication,
            Body = body,
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
    }
}