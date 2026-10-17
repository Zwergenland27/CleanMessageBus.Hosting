namespace CleanMessageBus.Hosting.InMemory;

/// <summary>
/// Registers the in memory transport.
/// </summary>
public static class InMemoryMessageBusExtensions
{
    /// <summary>
    /// Uses the in-memory transport, intended for tests and local development.
    /// </summary>
    public static MessageBusBuilder UseInMemoryTransport(this MessageBusBuilder bus) => bus.UseTransport<InMemoryTransport>();
}