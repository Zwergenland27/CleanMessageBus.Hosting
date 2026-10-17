namespace CleanMessageBus.Hosting.Internal;

/// <summary>
/// Compiled call of a mapped handler delegate.
/// </summary>
internal delegate ValueTask HandlerInvoker(
    IServiceProvider services,
    EventContext context,
    object @event,
    CancellationToken cancellationToken);