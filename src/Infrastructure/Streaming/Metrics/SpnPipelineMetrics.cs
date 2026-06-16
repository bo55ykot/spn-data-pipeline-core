using System.Diagnostics.Metrics;

namespace DataPipeline.Infrastructure.Streaming.Metrics;

/// <summary>
/// Enterprise performance telemetry tracking for high-load SPN data distribution.
/// </summary>
public class SpnPipelineMetrics
{
    private static readonly Meter SpnMeter = new("DataPipeline.Streaming.Spn", "1.0.0");
    private static readonly Counter<long> IngestedPayloadsCounter = SpnMeter.CreateCounter<long>("spn_payloads_ingested_total");
    private static readonly Counter<long> BackpressureDroppedEvents = SpnMeter.CreateCounter<long>("spn_backpressure_throttled_total");

    public void RecordIngestionSuccess() => IngestedPayloadsCounter.Add(1);
    public void RecordThrottledEvent() => BackpressureDroppedEvents.Add(1);
}

