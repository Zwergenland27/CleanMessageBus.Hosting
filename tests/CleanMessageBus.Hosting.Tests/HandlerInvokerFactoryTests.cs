using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Internal;
using CleanMessageBus.Hosting.Tests.TestSupport;
using Microsoft.Extensions.DependencyInjection;

namespace CleanMessageBus.Hosting.Tests;

public sealed class HandlerInvokerFactoryTests
{
    [Fact]
    public async Task Create_ShouldPassEventAndResolvedService_WhenHandlerRequestsThem()
    {
        await using var services = BuildServices(s => s.AddSingleton<IGreeter, Greeter>());
        var @event = new OrderPlaced(Guid.NewGuid());
        string? greeting = null;

        var invoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced e, IGreeter greeter) => { greeting = greeter.Greet(e.OrderId.ToString()); },
            services);
        await invoker(services, CreateContext(), @event, CancellationToken.None);

        Assert.Equal($"Hello {@event.OrderId}", greeting);
    }

    [Fact]
    public async Task Create_ShouldPassBusProvidedParameters_WhenHandlerRequestsThem()
    {
        await using var services = BuildServices();
        using var cancellation = new CancellationTokenSource();
        var context = CreateContext();
        EventContext? receivedContext = null;
        MessageEnvelope? receivedEnvelope = null;
        CancellationToken receivedToken = default;
        IServiceProvider? receivedServices = null;

        var invoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _, EventContext ctx, MessageEnvelope envelope, CancellationToken token, IServiceProvider sp) =>
            {
                receivedContext = ctx;
                receivedEnvelope = envelope;
                receivedToken = token;
                receivedServices = sp;
            },
            services);
        await invoker(services, context, new OrderPlaced(Guid.NewGuid()), cancellation.Token);

        Assert.Same(context, receivedContext);
        Assert.Same(context.Envelope, receivedEnvelope);
        Assert.Equal(cancellation.Token, receivedToken);
        Assert.Same(services, receivedServices);
    }

    [Fact]
    public async Task Create_ShouldUseDefaultValue_WhenOptionalServiceIsNotRegistered()
    {
        await using var services = BuildServices();
        var called = false;
        IGreeter? received = new Greeter();

        var invoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _, IGreeter? greeter = null) =>
            {
                called = true;
                received = greeter;
            },
            services);
        await invoker(services, CreateContext(), new OrderPlaced(Guid.NewGuid()), CancellationToken.None);

        Assert.True(called);
        Assert.Null(received);
    }

    [Fact]
    public async Task Create_ShouldResolveKeyedService_WhenParameterHasFromKeyedServicesAttribute()
    {
        await using var services = BuildServices(s =>
        {
            s.AddSingleton<IGreeter, Greeter>();
            s.AddKeyedSingleton<IGreeter, FormalGreeter>("formal");
        });
        string? greeting = null;

        var invoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _, [FromKeyedServices("formal")] IGreeter greeter) => { greeting = greeter.Greet("Florian"); },
            services);
        await invoker(services, CreateContext(), new OrderPlaced(Guid.NewGuid()), CancellationToken.None);

        Assert.Equal("Good day Florian", greeting);
    }

    [Fact]
    public async Task Create_ShouldAwaitHandler_WhenHandlerIsAsync()
    {
        await using var services = BuildServices();
        var completed = false;

        var invoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            async (OrderPlaced _) =>
            {
                await Task.Delay(10);
                completed = true;
            },
            services);
        await invoker(services, CreateContext(), new OrderPlaced(Guid.NewGuid()), CancellationToken.None);

        Assert.True(completed);
    }

    [Fact]
    public async Task Create_ShouldSupportValueTask_WhenHandlerReturnsValueTask()
    {
        await using var services = BuildServices();
        var called = false;

        var invoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _) =>
            {
                called = true;
                return ValueTask.CompletedTask;
            },
            services);
        await invoker(services, CreateContext(), new OrderPlaced(Guid.NewGuid()), CancellationToken.None);

        Assert.True(called);
    }

    [Fact]
    public async Task Create_ShouldSupportResultTasks_WhenHandlerReturnsTaskOrValueTaskOfT()
    {
        await using var services = BuildServices();
        var calls = 0;

        var taskInvoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _) => Task.FromResult(++calls),
            services);
        var valueTaskInvoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _) => ValueTask.FromResult(++calls),
            services);
        await taskInvoker(services, CreateContext(), new OrderPlaced(Guid.NewGuid()), CancellationToken.None);
        await valueTaskInvoker(services, CreateContext(), new OrderPlaced(Guid.NewGuid()), CancellationToken.None);

        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Create_ShouldPropagateException_WhenHandlerThrows()
    {
        await using var services = BuildServices();

        var invoker = HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _) => { throw new InvalidOperationException("boom"); },
            services);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await invoker(services, CreateContext(), new OrderPlaced(Guid.NewGuid()), CancellationToken.None));
        Assert.Equal("boom", exception.Message);
    }

    [Fact]
    public void Create_ShouldThrow_WhenFirstParameterIsNotTheEvent()
    {
        Assert.Throws<ArgumentException>(() => HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (IGreeter _, OrderPlaced _) => { },
            validationServices: null));
    }

    [Fact]
    public void Create_ShouldThrow_WhenHandlerHasNoParameters()
    {
        Assert.Throws<ArgumentException>(() => HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            () => { },
            validationServices: null));
    }

    [Fact]
    public async Task Create_ShouldThrow_WhenServiceIsNotRegistered()
    {
        await using var services = BuildServices();

        var exception = Assert.Throws<InvalidOperationException>(() => HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _, IGreeter _) => { },
            services));
        Assert.Contains(nameof(IGreeter), exception.Message);
    }

    [Fact]
    public async Task Create_ShouldThrow_WhenKeyedServiceIsNotRegistered()
    {
        await using var services = BuildServices(s => s.AddSingleton<IGreeter, Greeter>());

        Assert.Throws<InvalidOperationException>(() => HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _, [FromKeyedServices("formal")] IGreeter _) => { },
            services));
    }

    [Fact]
    public void Create_ShouldThrow_WhenReturnTypeIsNotSupported()
    {
        Assert.Throws<ArgumentException>(() => HandlerInvokerFactory.Create(
            typeof(OrderPlaced),
            (OrderPlaced _) => 42,
            validationServices: null));
    }

    private static ServiceProvider BuildServices(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static EventContext CreateContext()
    {
        var envelope = TestBus.CreateEnvelope(new OrderPlaced(Guid.NewGuid()), EventKind.Domain, TestBus.ApplicationName);
        var handler = new EventHandlerDescriptor(
            "OrderPlaced",
            typeof(OrderPlaced),
            "OrderPlaced",
            EventKind.Domain,
            TestBus.ApplicationName,
            prefetchCount: 1,
            RetryPolicy.None);

        return new EventContext(envelope, handler, new DeliveryInfo(1));
    }
}