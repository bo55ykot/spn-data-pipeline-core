using System.Threading;
using System.Threading.Tasks;

namespace DataPipeline.Core.Domain.Streaming;

/// <summary>
/// Defines the architectural contract for high-throughput SPN payload processing nodes.
/// </summary>
public interface ISpnIngestionEngine
{
    Task EnqueuePayloadAsync(string payload, CancellationToken cancellationToken);
    bool TryEnqueuePayload(string payload);
}

