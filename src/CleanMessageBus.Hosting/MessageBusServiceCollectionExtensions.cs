using CleanMessageBus.Abstractions;
using CleanMessageBus.Abstractions.Transport;
using CleanMessageBus.Hosting.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace CleanMessageBus.Hosting;

/// <summary>
/// Registers CleanMessageBus.
/// </summary>
public static class MessageBusServiceCollectionExtensions
{
    /// <summary>
    /// Registers the message bus. Handlers are mapped afterwards on the built host via
    /// <c>MapDomainEventHandler</c>/<c>MapIntegrationEventHandler</c> and started automatically with the host.
    /// </summary>
    public static IServiceCollection AddCleanMessageBus(this IServiceCollection services, Action<MessageBusBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        if (services.Any(descriptor => descriptor.ServiceType == typeof(EventHandlerRegistry)))
        {
            throw new InvalidOperationException("AddCleanMessageBus has already been called.");
        }

        var builder = new MessageBusBuilder(services);
        configure(builder);

        ArgumentOutOfRangeException.ThrowIfLessThan(builder.DefaultPrefetchCount, 1, nameof(MessageBusBuilder.DefaultPrefetchCount));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            builder.DefaultPrefetchCount, EventHandlerDescriptor.MaxPrefetchCount, nameof(MessageBusBuilder.DefaultPrefetchCount));
        ArgumentNullException.ThrowIfNull(builder.DefaultRetryPolicy, nameof(MessageBusBuilder.DefaultRetryPolicy));

        var applicationName = builder.ApplicationName;
        var prefetchCount = builder.DefaultPrefetchCount;
        var retryPolicy = builder.DefaultRetryPolicy;
        var jsonOptions = builder.JsonSerializerOptions;

        services.AddSingleton(sp => new MessageBusSettings(
            ResolveApplicationName(applicationName, sp),
            prefetchCount,
            retryPolicy));

        services.AddSingleton(_ => new EventHandlerRegistry());
        services.TryAddSingleton<IMessageSerializer>(_ => new SystemTextJsonMessageSerializer(jsonOptions));

        return services;
    }

    private static string ResolveApplicationName(string? configured, IServiceProvider services)
    {
        var name = configured ?? services.GetService<IHostEnvironment>()?.ApplicationName;
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidOperationException(
                "The message bus application name is not configured. Set MessageBusBuilder.ApplicationName.");
        }

        return name;
    }
}