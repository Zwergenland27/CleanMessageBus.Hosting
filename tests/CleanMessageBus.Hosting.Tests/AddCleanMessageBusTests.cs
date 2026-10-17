using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.InMemory;
using CleanMessageBus.Hosting.Tests.TestSupport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CleanMessageBus.Hosting.Tests;

public sealed class AddCleanMessageBusTests
{
    [Fact]
    public void AddCleanMessageBus_ShouldThrow_WhenCalledTwice()
    {
        var services = new ServiceCollection();
        services.AddCleanMessageBus(bus => bus.ApplicationName = "app");

        Assert.Throws<InvalidOperationException>(() => services.AddCleanMessageBus(bus => bus.ApplicationName = "app"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(EventHandlerDescriptor.MaxPrefetchCount + 1)]
    public void AddCleanMessageBus_ShouldThrow_WhenDefaultPrefetchCountIsOutOfRange(int prefetchCount)
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddCleanMessageBus(bus => bus.DefaultPrefetchCount = prefetchCount));
    }

    [Fact]
    public void AddCleanMessageBus_ShouldApplyDefaults_WhenHandlerDoesNotOverrideThem()
    {
        var retryPolicy = RetryPolicy.Fixed(7, TimeSpan.FromSeconds(1));
        using var host = TestBus.Build(bus =>
        {
            bus.DefaultPrefetchCount = 10;
            bus.DefaultRetryPolicy = retryPolicy;
        });

        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { });

        var handler = Assert.Single(host.Services.GetRequiredService<EventHandlerRegistry>().Handlers);
        Assert.Equal(10, handler.PrefetchCount);
        Assert.Same(retryPolicy, handler.RetryPolicy);
    }

    [Fact]
    public void AddCleanMessageBus_ShouldUseHostApplicationName_WhenApplicationNameIsNotConfigured()
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { ApplicationName = "fallback-app" });
        builder.Logging.ClearProviders();
        builder.Services.AddCleanMessageBus(bus => bus.UseInMemoryTransport());
        using var host = builder.Build();

        host.MapDomainEventHandler<OrderPlaced>((OrderPlaced _) => { });

        var handler = Assert.Single(host.Services.GetRequiredService<EventHandlerRegistry>().Handlers);
        Assert.Equal("fallback-app", handler.SourceApplication);
    }
}