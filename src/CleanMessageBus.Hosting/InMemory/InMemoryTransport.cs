using System.Collections.Concurrent;
using System.Threading.Channels;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Abstractions.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CleanMessageBus.Hosting.InMemory;

/// <summary>
/// Transport for tests and local development. Routes messages within the process using one queue per handler
/// with one sequential consumer each, supports delayed retries and keeps dead-lettered messages in
/// <see cref="DeadLetters"/>. Messages published before the transport has started are buffered.
/// <see cref="EventHandlerDescriptor.PrefetchCount"/> has no effect, as there is no broker to fetch from.
/// </summary>
public sealed class InMemoryTransport : IBrokerTransport, IAsyncDisposable
{
    private readonly object _sync = new();
    private readonly List<MessageEnvelope> _pending = [];
    private readonly List<Task> _workers = [];
    private readonly ConcurrentQueue<MessageEnvelope> _published = new();
    private readonly ConcurrentQueue<DeadLetteredMessage> _deadLetters = new();
    private readonly ILogger<InMemoryTransport> _logger;

    private Dictionary<string, HandlerQueue>? _queues;
    private CancellationTokenSource? _stopping;

    /// <summary>Creates the transport.</summary>
    public InMemoryTransport(ILogger<InMemoryTransport>? logger = null)
    {
        this._logger = logger ?? NullLogger<InMemoryTransport>.Instance;
    }

    /// <inheritdoc />
    public string Name => "in-memory";

    /// <summary>All messages passed to <see cref="PublishAsync"/>.</summary>
    public IReadOnlyCollection<MessageEnvelope> PublishedMessages => _published.ToArray();

    /// <summary>All dead-lettered messages.</summary>
    public IReadOnlyCollection<DeadLetteredMessage> DeadLetters => _deadLetters.ToArray();

    /// <inheritdoc />
    public Task StartAsync(TransportStartContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        lock (_sync)
        {
            if (_queues is not null)
            {
                throw new InvalidOperationException("The in-memory transport has already been started.");
            }

            _stopping = new CancellationTokenSource();
            var token = _stopping.Token;
            var created = new Dictionary<string, HandlerQueue>(StringComparer.Ordinal);

            foreach (var handler in context.Handlers)
            {
                var queue = new HandlerQueue(
                    handler,
                    Channel.CreateUnbounded<Delivery>(new UnboundedChannelOptions { SingleReader = true }));
                created.Add(handler.Name, queue);
                _workers.Add(Task.Run(() => ConsumeAsync(queue, context.Receiver, token), CancellationToken.None));
            }

            _queues = created;
            foreach (var envelope in _pending)
            {
                Route(envelope);
            }

            _pending.Clear();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Task[] running;
        lock (_sync)
        {
            _stopping?.Cancel();
            running = _workers.ToArray();
        }

        try
        {
            await Task.WhenAll(running).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>
    /// Publishes a message. Also usable in tests to simulate messages of other applications,
    /// e.g. integration events with a foreign <see cref="MessageEnvelope.SourceApplication"/>.
    /// </summary>
    public ValueTask PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        cancellationToken.ThrowIfCancellationRequested();

        _published.Enqueue(envelope);
        lock (_sync)
        {
            if (_queues is null)
            {
                _pending.Add(envelope);
            }
            else
            {
                Route(envelope);
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        lock (_sync)
        {
            _stopping?.Cancel();
            _stopping?.Dispose();
            _stopping = null;
        }

        return ValueTask.CompletedTask;
    }

    private void Route(MessageEnvelope envelope)
    {
        foreach (var queue in _queues!.Values)
        {
            var handler = queue.Handler;
            if (handler.Kind == envelope.Kind
                && string.Equals(handler.EventName, envelope.EventName, StringComparison.Ordinal)
                && string.Equals(handler.SourceApplication, envelope.SourceApplication, StringComparison.Ordinal))
            {
                queue.Channel.Writer.TryWrite(new Delivery(envelope, 1));
            }
        }
    }

    private async Task ConsumeAsync(HandlerQueue queue, IMessageReceiver receiver, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var delivery in queue.Channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                ProcessingResult result;
                try
                {
                    result = await receiver
                        .ReceiveAsync(delivery.Envelope, queue.Handler, new DeliveryInfo(delivery.Attempt), cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogError(exception, "The message receiver failed unexpectedly for handler {Handler}.", queue.Handler.Name);
                    result = ProcessingResult.DeadLetter(exception, "The message receiver failed unexpectedly.");
                }

                switch (result.Outcome)
                {
                    case ProcessingOutcome.Retry:
                        var attempt = result.CountsAsFailedAttempt ? delivery.Attempt + 1 : delivery.Attempt;
                        _ = RequeueAsync(queue, delivery with { Attempt = attempt }, result.RetryDelay, cancellationToken);
                        break;
                    case ProcessingOutcome.DeadLetter:
                        _deadLetters.Enqueue(new DeadLetteredMessage(delivery.Envelope, queue.Handler.Name, delivery.Attempt, result));
                        break;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task RequeueAsync(HandlerQueue queue, Delivery delivery, TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }

            queue.Channel.Writer.TryWrite(delivery);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private sealed record HandlerQueue(EventHandlerDescriptor Handler, Channel<Delivery> Channel);

    private sealed record Delivery(MessageEnvelope Envelope, int Attempt);
}