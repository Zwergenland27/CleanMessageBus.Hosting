using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting.Tests.TestSupport;

public sealed record OrderPlaced(Guid OrderId) : IDomainEvent;

[EventName("order-shipped")]
public sealed record OrderShipped(Guid OrderId) : IDomainEvent;

public sealed record PaymentReceived(Guid PaymentId) : IIntegrationEvent;

public interface IGreeter
{
    string Greet(string name);
}

public sealed class Greeter : IGreeter
{
    public string Greet(string name) => $"Hello {name}";
}

public sealed class FormalGreeter : IGreeter
{
    public string Greet(string name) => $"Good day {name}";
}

public sealed class ScopedProbe : IDisposable
{
    public Guid Id { get; } = Guid.NewGuid();

    public bool IsDisposed { get; private set; }

    public void Dispose() => IsDisposed = true;
}