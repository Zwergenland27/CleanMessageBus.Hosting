# CleanMessageBus.Hosting

Minimal-API-style event handlers and built-in OpenTelemetry instrumentation
for CleanMessageBus. Transports (e.g. `CleanMessageBus.RabbitMQ`) plug in via `IBrokerTransport`.

## Quick start

```csharp
builder.Services.AddCleanMessageBus(bus =>
{
    bus.ApplicationName = "payment";            // defaults to IHostEnvironment.ApplicationName
    bus.UseInMemoryTransport();                 // or a transport package, e.g. bus.UseRabbitMq(...)
});

var app = builder.Build();

app.MapDomainEventHandler<TripBooked>(
    async (TripBooked e, IFormationService formations, CancellationToken ct) =>
        await formations.RefreshAsync(e.TripId, ct));

app.MapIntegrationEventHandler<FormationUpdated>(
    "order-service",                        // application that publishes the event
    async (OrderShipped e, EventContext context, IDbApi api, CancellationToken ct) =>
        await api.FetchAsync(e.TrainNumber, ct),
    options =>
    {
        options.Name = "order-marketing-handler";       // stable, unique name (transports derive queue names from it)
        options.PrefetchCount = 1;              // default: fetch the next message only when the previous one is done
        options.RetryPolicy = RetryPolicy.Exponential(5, TimeSpan.FromSeconds(10));
    });

app.Run();                                      // the bus starts with the host
```

Publishing – event types do not have to be registered:

```csharp
foreach (var domainEvent in trip.DomainEvents)
{
    await publisher.PublishDomainEventAsync(domainEvent, ct); // runtime type decides the event name
}
```

### Publishing from an outbox

If events are written to an outbox and published later by a worker, the trace context of the code that raised the
event is gone by the time the worker publishes. Capture it when writing the outbox entry and pass it back on publish:

```csharp
// when storing the event (same async flow as the raising handler or request)
var entry = new OutboxEntry(Guid.NewGuid(), domainEvent, DateTimeOffset.UtcNow, MessageTraceContext.Capture());

// in the outbox worker
await publisher.PublishDomainEventAsync(entry.Event, new PublishOptions
{
    MessageId = entry.Id,            // stable id: consumers can detect duplicates if the worker publishes twice
    Timestamp = entry.OccurredAt,
    TraceContext = entry.TraceContext,
}, ct);
```

`MessageTraceContext` holds `traceparent`, `tracestate` and `baggage` as strings and can be stored as three columns
or as JSON. With `PublishOptions`, the stored context is the only parent of the send span; the worker's own activity
is ignored, and without a stored context the message starts a new trace.

## Handler parameters

The first parameter is the event. Further parameters are bound once at registration (via reflection,
compiled to a delegate, so there is no reflection per message):

| Parameter | Value |
| --- | --- |
| `EventContext` | message id, headers, delivery attempt, handler descriptor |
| `MessageEnvelope` | raw envelope |
| `CancellationToken` | cancelled on shutdown |
| `IServiceProvider` | the handler scope |
| `[FromKeyedServices("key")] T` | keyed service |
| any other `T` | service from the handler scope; optional if the parameter has a default value |

Unresolvable parameters fail at `Map*` time, not at the first message.
Handlers may return `void`, `Task`, `ValueTask`, `Task<T>` or `ValueTask<T>`.

## Error handling

| Situation | Result |
| --- | --- |
| handler throws | retry according to the `RetryPolicy`, then dead-letter |
| handler throws `RetryAfterException` | delayed retry, attempt counter unchanged (rate limits) |
| payload cannot be deserialized | dead-letter immediately, without calling the handler |
| shutdown | redelivery, attempt counter unchanged |

The transport decides *how* (queues, TTLs, DLX); this package decides *what*.

## Processing and prefetch

Messages of one handler are always processed one after another, each in a new DI scope.
`PrefetchCount` only limits how many unacknowledged messages the transport fetches for the handler.
With `1`, the remaining messages stay in the broker and can be consumed by other instances,
which is what you want for rate-limited work. Higher values reduce round trips for fast handlers.

## OpenTelemetry

Instrumentation is built in and uses only `System.Diagnostics`. Register the source and meter:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(CleanMessageBusDiagnostics.SourceName))
    .WithMetrics(m => m.AddMeter(CleanMessageBusDiagnostics.MeterName));
```

Producer and consumer spans follow the messaging semantic conventions; W3C `traceparent`, `tracestate`
and `baggage` are propagated in the message headers. Metrics: `messaging.client.sent.messages`,
`messaging.client.consumed.messages`, `messaging.process.duration`.

## Startup behaviour

Handlers must be mapped before the host starts. On start, a hosted service hands all handlers to the
transport, which declares queues and bindings. Publish destinations are declared by the transport on demand. If the transport cannot start (e.g. broker unreachable), the host
still starts and the transport start is retried with exponential backoff (max. 30 s).
A missing transport registration fails fast.

## Local development across repositories

Every package lives in its own repository and is referenced as package, never as project.
To test unreleased changes of `CleanMessageBus.Abstractions` locally:

```bash
mkdir -p ~/.nuget/local-feed
dotnet nuget add source ~/.nuget/local-feed --name local     # once, user-level config

# in the CleanMessageBus.Abstractions repository
dotnet pack -c Release -o ~/.nuget/local-feed -p:Version=0.1.1-dev.1
```

Then reference that version here. Use a new pre-release version for each local pack, because NuGet
caches packages by version (`dotnet nuget locals global-packages --clear` otherwise).

## Build & test

```bash
dotnet test
dotnet pack -c Release -o ./artifacts
```
