using CleanMessageBus.Abstractions.Transport;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CleanMessageBus.Hosting.Internal;

/// <summary>
/// Starts the transport with all mapped handlers when the host starts.
/// Start failures (e.g. broker not reachable) are retried in the background, so the application starts anyway.
/// </summary>
internal sealed class MessageBusHostedService : BackgroundService
{
    internal static TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    internal static TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    private readonly MessageBusSettings _settings;
    private readonly EventHandlerRegistry _handlers;
    private readonly IMessageReceiver _receiver;
    private readonly TransportAccessor _transportAccessor;
    private readonly ILogger<MessageBusHostedService> _logger;

    private IBrokerTransport? _transport;
    private TransportStartContext? _startContext;
    private volatile bool _transportStarted;

    public MessageBusHostedService(
        MessageBusSettings settings,
        EventHandlerRegistry handlers,
        IMessageReceiver receiver,
        TransportAccessor transportAccessor,
        ILogger<MessageBusHostedService> logger)
    {
        _settings = settings;
        _handlers = handlers;
        _receiver = receiver;
        _transportAccessor = transportAccessor;
        _logger = logger;
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        // Configuration errors fail fast; connectivity problems are retried in ExecuteAsync.
        _transport = _transportAccessor.Transport;
        _startContext = new TransportStartContext(
            _settings.ApplicationName,
            _handlers.Freeze(),
            _receiver);

        return base.StartAsync(cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);

        if (_transportStarted && _transport is not null)
        {
            await _transport.StopAsync(cancellationToken).ConfigureAwait(false);
            _transportStarted = false;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var delay = InitialRetryDelay;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _transport!.StartAsync(_startContext!, stoppingToken).ConfigureAwait(false);
                _transportStarted = true;
                _logger.LogInformation(
                    "Message bus transport {Transport} started for application {Application} with {HandlerCount} handler(s).",
                    _transport.Name,
                    _startContext!.ApplicationName,
                    _startContext.Handlers.Count);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Starting message bus transport {Transport} failed. Retrying in {Delay}.",
                    _transport!.Name,
                    delay);

                try
                {
                    await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks));
            }
        }
    }
}