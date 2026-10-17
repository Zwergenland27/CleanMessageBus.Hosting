namespace CleanMessageBus.Hosting.Tests.TestSupport;

internal static class Eventually
{
    public static async Task AssertAsync(Func<bool> condition, string because, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                Assert.Fail($"Condition was not met in time: {because}");
            }

            await Task.Delay(10);
        }
    }
}