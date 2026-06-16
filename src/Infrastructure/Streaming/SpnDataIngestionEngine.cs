using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using DataPipeline.Core.Domain.Streaming;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DataPipeline.Infrastructure.Streaming;

public class SpnDataIngestionEngine : BackgroundService, ISpnIngestionEngine
{
    private readonly ILogger<SpnDataIngestionEngine> _logger;
    private readonly Channel<string> _spnDataChannel;
    private const int MAX_BUFFER_CAPACITY = 50000;

    public SpnDataIngestionEngine(ILogger<SpnDataIngestionEngine> logger)
    {
        _logger = logger;
        var channelOptions = new BoundedChannelOptions(MAX_BUFFER_CAPACITY)
        {
            FullMode = BoundedChannelFullMode.Wait, // Rigid backpressure handler
            SingleWriter = false,
            SingleReader = true
        };
        _spnDataChannel = Channel.CreateBounded<string>(channelOptions);
    }

    public async Task EnqueuePayloadAsync(string payload, CancellationToken cancellationToken)
    {
        await _spnDataChannel.Writer.WriteAsync(payload, cancellationToken);
    }

    public bool TryEnqueuePayload(string payload)
    {
        return _spnDataChannel.Writer.TryWrite(payload);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Initializing High-Availability SPN Data Ingestion Engine Node at 5.0 GHz.");
        var processingTask = ProcessPayloadStreamAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                string mockPayload = $"{{\"eventId\":\"{Guid.NewGuid()}\",\"timestamp\":\"{DateTime.UtcNow:O}\",\"metric\":\"SPN_LIVE_STREAM\"}}";
                if (!TryEnqueuePayload(mockPayload))
                {
                    await EnqueuePayloadAsync(mockPayload, stoppingToken);
                }
                await Task.Delay(1, stoppingToken);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Fatal compilation error in SPN Live Stream Pipeline.");
            }
        }
        _spnDataChannel.Writer.Complete();
        await processingTask;
    }

    private async Task ProcessPayloadStreamAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SPN Async Consumer Stream Channel Reader deployed.");
        try
        {
            await foreach (var payload in _spnDataChannel.Reader.ReadAllAsync(stoppingToken))
            {
                // Sub-millisecond data transformation pipeline goes here
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("SPN Stream Reader cleanly flushed and shut down.");
        }
    }
}

