// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.MCP_gRPC;
using Aspire.Dashboard.Otlp.Storage;
using Google.Protobuf.WellKnownTypes;

namespace Aspire.Dashboard.MCP;

public sealed class McpService
{
    private readonly ILogger<McpService> _logger;
    private readonly TelemetryRepository _telemetryRepository;
    private readonly McpModel _mcpModel;

    public McpService(ILogger<McpService> logger, TelemetryRepository telemetryRepository, McpModel mcpModel)
    {
        _logger = logger;
        _telemetryRepository = telemetryRepository;
        _mcpModel = mcpModel;
    }

    public async Task<GetTracesServiceResponse> GetTraces(GetTracesServiceRequest request)
    {
        _logger.LogDebug("GetTraces called for {resourceName}", request.ResourceName);

        var cancellationTokenSource = new CancellationTokenSource();
        CancellationToken cancellationToken = cancellationTokenSource.Token;

        var resp = new GetTracesServiceResponse();

        var task = _mcpModel.GetTraces(request.ResourceName, cancellationToken);
        resp.Traces = await task.ConfigureAwait(false);
        _logger.LogDebug("Response json: {trace}", resp.Traces);
        return resp;
    }

    public Task<GetTraceDetailsServiceResponse> GetTraceDetails(GetTraceDetailsServiceRequest request)
    {
        _logger.LogDebug("GetTraceDetails called. TraceId: {traceId}", request.TraceId);
        var resp = new GetTraceDetailsServiceResponse();
        resp.TraceDetails = _mcpModel.GetTrace(request.TraceId);
        _logger.LogDebug("Response json: {trace}", resp.TraceDetails);
        return Task.FromResult(resp);
    }

    public async Task<GetStructuredLogsServiceResponse> GetStructuredLogs(GetStructuredLogsServiceRequest request)
    {
        _logger.LogDebug("GetStructuredLog called. Resource: {ResourceName}", request.ResourceName);

        var cancellationTokenSource = new CancellationTokenSource();
        CancellationToken cancellationToken = cancellationTokenSource.Token;

        var resp = new GetStructuredLogsServiceResponse();
        var task = _mcpModel.GetStructuredLogs(request.ResourceName, cancellationToken);
        resp.LogResults = await task.ConfigureAwait(false);
        _logger.LogDebug("Response json: {logs}", resp.LogResults);
        return resp;
    }

    public async Task<GetConsoleLogsServiceResponse> GetConsoleLogs(GetConsoleLogsServiceRequest request)
    {
        _logger.LogDebug("GetConsoleLog called. Resource: {ResourceName}", request.ResourceName);

        var cancellationTokenSource = new CancellationTokenSource();
        CancellationToken cancellationToken = cancellationTokenSource.Token;

        var resp = new GetConsoleLogsServiceResponse();
        var task = _mcpModel.GetConsoleLogs(request.ResourceName, cancellationToken);
        resp.LogResults = await task.ConfigureAwait(false);
        _logger.LogDebug("Response json: {logs}", resp.LogResults);
        return resp;
    }

    public Task<GetTraceStructuredLogsServiceResponse> GetTraceStructuredLogs(GetTraceStructuredLogsServiceRequest request)
    {
        _logger.LogDebug("GetTraceStructuredLogs called. TraceId: {traceId}", request.TraceId);
        var resp = new GetTraceStructuredLogsServiceResponse();
        resp.LogResults = _mcpModel.GetTraceStructuredLogs(request.TraceId);
        _logger.LogDebug("Response json: {logs}", resp.LogResults);
        return Task.FromResult(resp);
    }

    public async Task<GetResourceGraphServiceResponse> GetResourceGraph(GetResourceGraphServiceRequest request)
    {
        _logger.LogDebug("GetResourceGraph called.");

        var cancellationTokenSource = new CancellationTokenSource();
        CancellationToken cancellationToken = cancellationTokenSource.Token;

        var resp = new GetResourceGraphServiceResponse();
        var task = _mcpModel.GetResourceGraph( cancellationToken);
        resp.ResourceGraph = await task.ConfigureAwait(false);
        _logger.LogDebug("Response json: {graph}", resp.ResourceGraph);
        return resp;
    }

}

public class DateTimeConverter
{
    public static Timestamp ConvertToTimestamp(DateTime dateTime)
    {
        return Timestamp.FromDateTime(dateTime.ToUniversalTime());
    }
}
