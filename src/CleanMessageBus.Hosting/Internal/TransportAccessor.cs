using CleanMessageBus.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;

namespace CleanMessageBus.Hosting.Internal;

/// <summary>
/// Resolves the configured transport lazily with a helpful error if none is configured.
/// </summary>
internal sealed class TransportAccessor
{
    private readonly IServiceProvider _services;
    private IBrokerTransport? _transport;

    public TransportAccessor(IServiceProvider services) => _services = services;

    public IBrokerTransport Transport => _transport ??= _services.GetService<IBrokerTransport>()
                                                       ?? throw new InvalidOperationException(
                                                           "No message bus transport is configured. Call UseInMemoryTransport(), UseTransport<T>() "
                                                           + "or the extension of a transport package (e.g. UseRabbitMq()) inside AddCleanMessageBus.");

    /// <summary>
    /// Transport name for telemetry; does not throw if no transport is configured.
    /// </summary>
    public string SystemName => (_transport ?? _services.GetService<IBrokerTransport>())?.Name ?? "unknown";
}