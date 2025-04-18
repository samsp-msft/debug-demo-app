// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.MCP;
using Grpc.Core;

namespace Aspire.Dashboard.MCP_gRPC;

public class McpGrpcService : DashboardMcp.DashboardMcpBase
{
    private readonly McpService _mcpService;

    public McpGrpcService(McpService mcpService)
    {
        _mcpService = mcpService;
    }

    public override Task<GetTracesServiceResponse> GetTraces(GetTracesServiceRequest request, ServerCallContext context)
    {
         return _mcpService.GetTraces(request);
    }

    public override Task<GetTraceDetailsServiceResponse> GetTraceDetails(GetTraceDetailsServiceRequest request, ServerCallContext context)
    {
        return _mcpService.GetTraceDetails(request);
    }

    public override Task<GetStructuredLogsServiceResponse> GetStructuredLogs(GetStructuredLogsServiceRequest request, ServerCallContext context)
    {
        return _mcpService.GetStructuredLogs(request);
    }

    public override Task<GetConsoleLogsServiceResponse> GetConsoleLogs(GetConsoleLogsServiceRequest request, ServerCallContext context)
    {
        return _mcpService.GetConsoleLogs(request);
    }

    public override Task<GetTraceStructuredLogsServiceResponse> GetTraceStructuredLogs(GetTraceStructuredLogsServiceRequest request, ServerCallContext context)
    {
        return _mcpService.GetTraceStructuredLogs(request);
    }

    public override Task<GetResourceGraphServiceResponse> GetResourceGraph(GetResourceGraphServiceRequest request, ServerCallContext context)
    {
        return _mcpService.GetResourceGraph(request);
    }
}
