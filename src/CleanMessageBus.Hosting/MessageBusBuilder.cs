using System.Text.Json;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CleanMessageBus.Hosting;

/// <summary>
/// Configures the message bus inside <c>AddCleanMessageBus</c>.
/// </summary>
public sealed class MessageBusBuilder
{
    internal MessageBusBuilder(IServiceCollection services) => Services = services;

    /// <summary>
    /// The service collection, for extension methods of transport packages.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Name of the current application. Used as source of domain events and published integration events.
    /// Defaults to <c>IHostEnvironment.ApplicationName</c>.
    /// </summary>
    public string? ApplicationName { get; set; }

    /// <summary>
    /// Default for <see cref="EventHandlerOptions.PrefetchCount"/>. Defaults to 1.
    /// </summary>
    public int DefaultPrefetchCount { get; set; } = 1;

    /// <summary>
    /// Default for <see cref="EventHandlerOptions.RetryPolicy"/>.
    /// </summary>
    public RetryPolicy DefaultRetryPolicy { get; set; } = RetryPolicy.Default;

    /// <summary>
    /// Options of the default System.Text.Json serializer.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Uses <typeparamref name="TTransport"/> (registered as singleton) as transport.
    /// </summary>
    public MessageBusBuilder UseTransport<TTransport>()
        where TTransport : class, IBrokerTransport
    {
        Services.TryAddSingleton<TTransport>();
        Services.Replace(ServiceDescriptor.Singleton<IBrokerTransport>(sp => sp.GetRequiredService<TTransport>()));
        return this;
    }

    /// <summary>
    /// Replaces the default System.Text.Json serializer.
    /// </summary>
    public MessageBusBuilder UseSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        Services.Replace(ServiceDescriptor.Singleton<IMessageSerializer, TSerializer>());
        return this;
    }
}
