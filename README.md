# spn-data-pipeline-core
High-throughput, asynchronous data ingestion framework built on .NET 8, designed for sub-millisecond real-time event streaming and explicit memory backpressure management.
#
// sample call for intialisation of API-engine through Dependency Injection (DI)
public class SpnIncomingController : ControllerBase
{
    private readonly ISpnIngestionEngine _spnEngine;

    public SpnIncomingController(ISpnIngestionEngine spnEngine)
    {
        _spnEngine = spnEngine;
    }

    [HttpPost("ingest-spn")]
    public async Task<IActionResult> PostSpnMetric([FromBody] string rawPayload, CancellationToken ct)
    {
        // high precision API-engine call with Backpressure
        await _spnEngine.EnqueuePayloadAsync(rawPayload, ct);
        return Ok(new { status = "Ingested under Backpressure Management" });
    }
}

