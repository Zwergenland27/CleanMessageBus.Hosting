using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Internal;

namespace CleanMessageBus.Hosting;

/// <summary>
/// All event handlers mapped in the current application.
/// </summary>
public sealed class EventHandlerRegistry
{
    private readonly object _sync = new();
    private readonly List<EventHandlerDescriptor> _descriptors = [];
    private readonly Dictionary<string, HandlerInvoker> _invokers = new(StringComparer.Ordinal);
    private bool _frozen;

    internal EventHandlerRegistry()
    {
    }

    /// <summary>
    /// The mapped handlers in registration order.
    /// </summary>
    public IReadOnlyList<EventHandlerDescriptor> Handlers
    {
        get
        {
            lock (_sync)
            {
                return _descriptors.ToArray();
            }
        }
    }

    /// <summary>
    /// Whether the bus has started; handlers can no longer be mapped.
    /// </summary>
    public bool IsFrozen
    {
        get
        {
            lock (_sync)
            {
                return _frozen;
            }
        }
    }

    internal void Add(EventHandlerDescriptor descriptor, HandlerInvoker invoker)
    {
        lock (_sync)
        {
            if (_frozen)
            {
                throw new InvalidOperationException(
                    "Event handlers must be mapped before the host is started.");
            }

            if (_invokers.ContainsKey(descriptor.Name))
            {
                throw new InvalidOperationException(
                    $"An event handler named '{descriptor.Name}' is already mapped. "
                    + "Set a unique name through EventHandlerOptions.Name.");
            }

            _invokers.Add(descriptor.Name, invoker);
            _descriptors.Add(descriptor);
        }
    }

    internal IReadOnlyList<EventHandlerDescriptor> Freeze()
    {
        lock (_sync)
        {
            _frozen = true;
            return _descriptors.ToArray();
        }
    }

    internal HandlerInvoker GetInvoker(string handlerName)
    {
        lock (_sync)
        {
            return _invokers.TryGetValue(handlerName, out var invoker)
                ? invoker
                : throw new InvalidOperationException($"No event handler named '{handlerName}' is mapped.");
        }
    }
}