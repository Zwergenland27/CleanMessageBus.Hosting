using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting.InMemory;

/// <summary>
/// A message the in-memory transport moved to its dead-letter list.
/// </summary>
/// <param name="Envelope">The message.</param>
/// <param name="HandlerName">The handler that failed.</param>
/// <param name="Attempt">The last delivery attempt.</param>
/// <param name="Result">The result that caused dead-lettering.</param>
public sealed record DeadLetteredMessage(
    MessageEnvelope Envelope,
    string HandlerName,
    int Attempt,
    ProcessingResult Result);