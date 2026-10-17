using CleanMessageBus.Abstractions;

namespace CleanMessageBus.Hosting.Internal;

internal sealed class MessageBusSettings
{
    public MessageBusSettings(string applicationName, int defaultPrefetchCount, RetryPolicy defaultRetryPolicy)
    {
        ApplicationName = applicationName;
        DefaultPrefetchCount = defaultPrefetchCount;
        DefaultRetryPolicy = defaultRetryPolicy;
    }

    public string ApplicationName { get; }

    public int DefaultPrefetchCount { get; }

    public RetryPolicy DefaultRetryPolicy { get; }
}