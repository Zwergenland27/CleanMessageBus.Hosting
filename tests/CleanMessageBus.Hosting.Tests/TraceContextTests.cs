using System.Diagnostics;
using CleanMessageBus.Abstractions;
using CleanMessageBus.Hosting.Telemetry;

namespace CleanMessageBus.Hosting.Tests;

public sealed class TraceContextTests
{
    [Fact]
    public void TryExtractParent_ShouldReturnInjectedContext_WhenHeadersWereWrittenByInject()
    {
        using var activity = new Activity("test").SetIdFormat(ActivityIdFormat.W3C).Start();
        activity.TraceStateString = "vendor=value";
        var headers = new Dictionary<string, string>();

        TraceContext.Inject(activity, headers);
        var found = TraceContext.TryExtractParent(headers, out var parent);

        Assert.True(found);
        Assert.Equal(activity.TraceId, parent.TraceId);
        Assert.Equal(activity.SpanId, parent.SpanId);
        Assert.Equal("vendor=value", parent.TraceState);
        Assert.True(parent.IsRemote);
    }

    [Fact]
    public void TryExtractParent_ShouldReturnFalse_WhenTraceParentIsMissingOrInvalid()
    {
        Assert.False(TraceContext.TryExtractParent(new Dictionary<string, string>(), out _));
        Assert.False(TraceContext.TryExtractParent(
            new Dictionary<string, string> { [MessageHeaderNames.TraceParent] = "invalid" },
            out _));
    }

    [Fact]
    public void Inject_ShouldWriteNothing_WhenActivityIsNull()
    {
        var headers = new Dictionary<string, string>();

        TraceContext.Inject(null, headers);

        Assert.Empty(headers);
    }

    [Fact]
    public void ApplyBaggage_ShouldDecodeEntries_WhenBaggageWasEncodedByInject()
    {
        using var source = new Activity("source").SetIdFormat(ActivityIdFormat.W3C).Start();
        source.AddBaggage("tenant", "tenant 1,a=b");
        source.AddBaggage("user", "42");
        var headers = new Dictionary<string, string>();
        TraceContext.Inject(source, headers);
        source.Stop();

        using var target = new Activity("target").Start();
        TraceContext.ApplyBaggage(target, headers);

        Assert.Equal("tenant 1,a=b", target.GetBaggageItem("tenant"));
        Assert.Equal("42", target.GetBaggageItem("user"));
    }
}