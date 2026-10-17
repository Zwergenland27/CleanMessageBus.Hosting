using CleanMessageBus.Abstractions;
using CleanMessageBus.Abstractions.Transport;
using CleanMessageBus.Hosting.Internal;
using CleanMessageBus.Hosting.Tests.TestSupport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CleanMessageBus.Hosting.Tests;

public sealed class MessageBusHostedServiceTests
{
    static MessageBusHostedServiceTests()
    {
        MessageBusHostedService.InitialRetryDelay = TimeSpan.FromMilliseconds(20);
        MessageBusHostedService.MaxRetryDelay = TimeSpan.FromMilliseconds(50);
    }

    [Fact]
    public async Task StartAsync_ShouldNotBlockHost_WhenTransportFailsToStart()
    {
        using var host = TestBus.Build(bus => bus.UseTransport<FlakyTransport>());
        var transport = host.Services.GetRequiredService<FlakyTransport>();
        transport.FailuresBeforeStart = 2;

        await host.StartAsync();
        try
        {
            await Eventually.AssertAsync(() => transport.IsStarted, "the transport should start after retries");
            Assert.Equal(3, transport.StartAttempts);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task StartAsync_ShouldPassHandlers_WhenTransportStarts()
    {
        using var host = TestBus.Build(bus => bus.UseTransport<FlakyTransport>());
        var transport = host.Services.GetRequiredService<FlakyTransport>();
        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { });

        await host.StartAsync();
        try
        {
            await Eventually.AssertAsync(() => transport.IsStarted, "the transport should start");
            var context = transport.StartContext!;
            Assert.Equal(TestBus.ApplicationName, context.ApplicationName);
            Assert.Equal("OrderPlacedHandler", Assert.Single(context.Handlers).Name);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task StopAsync_ShouldStopTransport_WhenTransportWasStarted()
    {
        using var host = TestBus.Build(bus => bus.UseTransport<FlakyTransport>());
        var transport = host.Services.GetRequiredService<FlakyTransport>();

        await host.StartAsync();
        await Eventually.AssertAsync(() => transport.IsStarted, "the transport should start");
        await host.StopAsync();

        Assert.True(transport.IsStopped);
    }

    [Fact]
    public async Task StartAsync_ShouldThrow_WhenNoTransportIsConfigured()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddCleanMessageBus(bus => bus.ApplicationName = "app");
        using var host = builder.Build();

        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
    }

    private sealed class FlakyTransport : IBrokerTransport
    {
        private int startAttempts;

        public int FailuresBeforeStart { get; set; }

        public int StartAttempts => Volatile.Read(ref startAttempts);

        public bool IsStarted { get; private set; }

        public bool IsStopped { get; private set; }

        public TransportStartContext? StartContext { get; private set; }

        public string Name => "flaky";

        public Task StartAsync(TransportStartContext context, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref startAttempts) <= FailuresBeforeStart)
            {
                throw new InvalidOperationException("Broker unavailable.");
            }

            StartContext = context;
            IsStarted = true;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            IsStopped = true;
            return Task.CompletedTask;
        }

        public ValueTask PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}