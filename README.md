# SPN Data Ingestion Core Framework

High-throughput, asynchronous data ingestion engine built on .NET 8, designed for sub-millisecond real-time event streaming and explicit memory backpressure management.

## 🚀 Integration & Sample Call (API Usage)

Here is a sample production-ready integration of the API-engine inside an enterprise controller via Dependency Injection (DI):

```csharp
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using DataPipeline.Core.Domain.Streaming;

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
        // High precision API-engine call with Backpressure support
        await _spnEngine.EnqueuePayloadAsync(rawPayload, ct);
        
        return Ok(new { status = "Ingested under Backpressure Management" });
    }
}
```
