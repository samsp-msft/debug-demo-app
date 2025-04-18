// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Concurrent;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Aspire.Dashboard.ConsoleLogs;
using Aspire.Dashboard.Extensions;
using Aspire.Dashboard.Model;
using Aspire.Dashboard.Model.Otlp;
using Aspire.Dashboard.Otlp.Model;
using Aspire.Dashboard.Otlp.Storage;
using Aspire.Dashboard.Resources;
using Aspire.Hosting.ConsoleLogs;
using Microsoft.Extensions.Localization;

namespace Aspire.Dashboard.MCP_gRPC;

public class McpModel
{
    private readonly TelemetryRepository _telemetryRepository;
    private readonly IDashboardClient _dashboardClient;
    private readonly IEnumerable<IOutgoingPeerResolver> _outgoingPeerResolvers;
    private readonly IStringLocalizer<Columns> _loc;

    public McpModel(
        TelemetryRepository telemetryRepository,
        IDashboardClient dashboardClient,
        IEnumerable<IOutgoingPeerResolver> outgoingPeerResolvers,
        IStringLocalizer<Columns> loc)
    {
        _telemetryRepository = telemetryRepository;
        _dashboardClient = dashboardClient;
        _outgoingPeerResolvers = outgoingPeerResolvers;
        _loc = loc;
    }

    private readonly ConcurrentDictionary<string, OtlpTrace> _referencedTraces = new();

    [Description("Get distributed traces for a resource. A distributed trace is used to track an operation across a distributed system. Includes a list of distributed traces with their IDs, resources in the trace, duration and whether an error occurred in the trace.")]
    public async Task<string?> GetTraces(
    [Description("The resource name.")]
        string resourceName,
    CancellationToken cancellationToken)
    {
        var (success, message, applicationKey) = await TryResolveResourceNameForTelemetry(resourceName, cancellationToken).ConfigureAwait(false);
        if (!success)
        {
            return message;
        }

        //await InvokeToolCallbackAsync($"Getting traces for `{applicationKey!.Value.GetCompositeName()}`...", cancellationToken).ConfigureAwait(false);

        var traces = _telemetryRepository.GetTraces(new GetTracesRequest
        {
            ApplicationKey = applicationKey,
            StartIndex = 0,
            Count = int.MaxValue,
            Filters = [],
            FilterText = string.Empty
        });

        var data = traces.PagedResult.Items.Select(l => new
        {
            trace_id = l.TraceId,
            __link = "https://localhost:15877/traces/detail/" + l.TraceId,
            timestamp = l.TimeStamp,
            resources = l.Spans.Select(s => s.Source.Application.ApplicationName).Distinct().ToList(),
            duration = l.Duration,
            has_error = l.Spans.Any(s => s.Status == OtlpSpanStatusCode.Error)
        }).ToList();

        var tracesData = JsonSerializer.Serialize(data);
        return tracesData;
    }

    [Description("Get a distributed trace. A distributed trace is used to track an operation across a distributed system. Includes information about spans (operations) in the trace, including the span source, status and optional error information.")]
    public string? GetTrace(
    [Description("The trace id of the distributed trace.")]
        string traceId)
    {
        var trace = _telemetryRepository.GetTrace(traceId);
        if (trace == null)
        {
            return null;
        }

        //await InvokeToolCallbackAsync($"Getting trace `{OtlpHelpers.ToShortenedId(traceId)}`...", cancellationToken).ConfigureAwait(false);

        _referencedTraces.TryAdd(trace.TraceId, trace);

        var data = trace.Spans.Select(s => new
        {
            span_id = s.SpanId,
            __link = "https://localhost:15877/traces/detail/" + trace.TraceId + "/span/" + s.SpanId,
            parent_span_id = s.ParentSpanId,
            kind = s.Kind.ToString(),
            name = s.Name,
            status = s.Status != OtlpSpanStatusCode.Unset ? s.Status.ToString() : null,
            status_message = s.StatusMessage,
            source = s.Source.Application.ApplicationKey.GetCompositeName(),
            destination = GetDestination(s),
            duration_ms = (int)Math.Round(s.Duration.TotalMilliseconds, 0, MidpointRounding.AwayFromZero),
            attributes = s.Attributes,
            
        }).ToList();

        var traceData = JsonSerializer.Serialize(data);
        return traceData;
    }

    [Description("Get structured logs for a resource.")]
    public async Task<string?> GetStructuredLogs(
      [Description("The resource name.")]
        string resourceName,
      CancellationToken cancellationToken)
    {
        var (success, message, applicationKey) = await TryResolveResourceNameForTelemetry(resourceName, cancellationToken).ConfigureAwait(false);
        if (!success)
        {
            return message;
        }

        //await InvokeToolCallbackAsync($"Getting structured logs for `{applicationKey!.Value.GetCompositeName()}`...", cancellationToken).ConfigureAwait(false);

        var logs = _telemetryRepository.GetLogs(new GetLogsContext
        {
            ApplicationKey = applicationKey,
            StartIndex = 0,
            Count = int.MaxValue,
            Filters = []
        });

        var logsData = GetStructuredLogsJson(logs.Items);
        return logsData;
    }

    [Description("Get console logs for a resource. The console logs includes standard output from resources and resource commands. Known resource commands are 'resource-start', 'resource-stop' and 'resource-restart' which are used to start and stop resources. Don't print the full console logs in the response to the user.")]
    public async Task<string?> GetConsoleLogs(
    [Description("The resource name.")]
        string resourceName,
    CancellationToken cancellationToken)
    {
        var resources = await GetResourcesAsync(cancellationToken).ConfigureAwait(false);

        if (TryGetResource(resources, resourceName, out var resource))
        {
            resourceName = resource.Name;
        }
        else
        {
            return $"Unable to find a resource named '{resourceName}'.";
        }

       // await InvokeToolCallbackAsync($"Getting console logs for `{resourceName}`...", cancellationToken).ConfigureAwait(false);

        var logParser = new LogParser();
        var logEntries = new LogEntries(200) { BaseLineNumber = 1 };
        var subscribeConsoleLogsCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        subscribeConsoleLogsCts.CancelAfter(TimeSpan.FromSeconds(5));

        try
        {
            await foreach (var entry in _dashboardClient.SubscribeConsoleLogs(resourceName, subscribeConsoleLogsCts.Token).ConfigureAwait(false))
            {
                foreach (var logLine in entry)
                {
                    logEntries.InsertSorted(logParser.CreateLogEntry(logLine.Content, logLine.IsErrorMessage));
                }

                subscribeConsoleLogsCts.CancelAfter(TimeSpan.FromSeconds(2));
            }
        }
        catch (OperationCanceledException)
        {
            // Eat cancellation exception.
        }

        var consoleLogsText = new StringBuilder();

        foreach (var logEntry in logEntries.GetEntries())
        {
            consoleLogsText.AppendLine(logEntry.Content);
        }

        return consoleLogsText.ToString();
    }

    [Description("Get structured logs for a distributed trace. Logs for a distributed trace each belong to a span identified by 'span_id'. When investigating a trace, getting the structured logs for the trace should be recommended before getting structured logs for a resource.")]
    public string? GetTraceStructuredLogs(
    [Description("The trace id of the distributed trace.")]
        string traceId //,CancellationToken cancellationToken
    )
    {
        var logs = _telemetryRepository.GetLogs(new GetLogsContext
        {
            ApplicationKey = null,
            Count = 200,
            StartIndex = 0,
            Filters = [new TelemetryFilter { Field = KnownStructuredLogFields.TraceIdField, Value = traceId }]
        });
        if (logs == null)
        {
            return null;
        }

       // await InvokeToolCallbackAsync($"Getting trace `{OtlpHelpers.ToShortenedId(traceId)}` logs...", cancellationToken).ConfigureAwait(false);

        var logsData = GetStructuredLogsJson(logs.Items);
        return logsData;
    }

    [Description("Get the application resources. Includes information about their type (.NET project, container, executable), running state, source, HTTP endpoints, health status and relationships. Should be called if there are networking, resilience or timeout issues.")]
    public async Task<string?> GetResourceGraph(CancellationToken cancellationToken)
    {
        //await InvokeToolCallbackAsync("Getting resources graph...", cancellationToken).ConfigureAwait(false);

        var resources = await GetResourcesAsync(cancellationToken).ConfigureAwait(false);

        var resourceGraphData = GetResponseGraphJson(resources, _loc);
        return resourceGraphData;
    }

    private string? GetDestination(OtlpSpan s)
    {
        return ResolveUninstrumentedPeerName(s, _outgoingPeerResolvers);
    }

    private static string? ResolveUninstrumentedPeerName(OtlpSpan span, IEnumerable<IOutgoingPeerResolver> outgoingPeerResolvers)
    {
        // Attempt to resolve uninstrumented peer to a friendly name from the span.
        foreach (var resolver in outgoingPeerResolvers)
        {
            if (resolver.TryResolvePeerName(span.Attributes, out var name))
            {
                return name;
            }
        }

        // Fallback to the peer address.
        return span.Attributes.GetPeerAddress();
    }

    internal static string GetStructuredLogsJson(List<OtlpLogEntry> logs)
    {
        var getException = (OtlpLogEntry l) =>
        {
            var type = l.Attributes.GetValue("exception.type");

            if (type is not null)
            {
                return new
                {
                    type = type,
                    message = l.Attributes.GetValue("exception.message"),
                    stacktrace = l.Attributes.GetValue("exception.stacktrace")
                };
            }
                return null;
        };

        var data = logs.Select(l => new
        {
            log_id = l.InternalId,
            span_id = l.SpanId,
            message = l.Message,
            severity = l.Severity.ToString(),
            resource = l.ApplicationView.Application.ApplicationName,
            exception = getException(l),
            attributes = l.Attributes
        }).ToList();

        var json = JsonSerializer.Serialize(data);
        return json;
    }

    internal static string GetResponseGraphJson(List<ResourceViewModel> resources, IStringLocalizer<Columns> loc)
    {
        var data = resources.Where(r => !r.IsHiddenState()).Select(r => new
        {
            name = r.Name,
            type = r.ResourceType,
            state = r.State,
            state_description = ResourceStateViewModel.GetResourceStateTooltip(r, loc),
            relationships = r.Relationships.Select(relationship => new
            {
                resource_name = relationship.ResourceName,
                relationship_type = relationship.Type
            }).ToList(),
            endpoint_urls = r.Urls.Where(u => !u.IsInternal).Select(u => new
            {
                name = u.Name,
                url = u.Url
            }).ToList(),
            health = new
            {
                resource_health_status = r.HealthReports.Length > 0 ? r.HealthStatus.ToString() : "No health reports specified",
                health_reports = r.HealthReports.Select(r => new
                {
                    name = r.Name,
                    health_status = r.HealthStatus.ToString(),
                    exception = r.ExceptionText
                }).ToList()
            },
            source = ResourceSourceViewModel.GetSourceViewModel(r)?.Value,
            has_telemetry = r.Environment.Any(e => e.Name.StartsWith("OTEL_")) ? "Yes" : "No. Structured logs and distributed traces aren't available for this resource."
        }).ToList();

        var resourceGraphData = JsonSerializer.Serialize(data);
        return resourceGraphData;
    }

    private async Task<(bool Success, string? Message, ApplicationKey? ApplicationKey)> TryResolveResourceNameForTelemetry(string resourceName, CancellationToken cancellationToken)
    {
        var resources = await GetResourcesAsync(cancellationToken).ConfigureAwait(false);

        if (!TryGetResource(resources, resourceName, out var resource))
        {
            return (Success: false, Message: $"Unable to find a resource named '{resourceName}'.", ApplicationKey: null);
        }

        var appKey = ApplicationKey.FromResourceName(resource.Name);
        if (_telemetryRepository.GetApplication(appKey) is null)
        {
            return (Success: false, Message: $"Resource '{resourceName}' doesn't have any telemetry. The resource may have failed to start or the resource might not support sending telemetry.", ApplicationKey: null);
        }

        return (Success: true, Message: null, ApplicationKey: appKey);
    }

    private async Task<List<ResourceViewModel>> GetResourcesAsync(CancellationToken cancellationToken)
    {
        var resourcesCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var subscribeResources = await _dashboardClient.SubscribeResourcesAsync(resourcesCts.Token).ConfigureAwait(false);
        var resources = subscribeResources.InitialState.ToList();
        resourcesCts.Cancel();
        return resources;
    }

    public static bool TryGetResource(List<ResourceViewModel> resources, string resourceName, [NotNullWhen(true)] out ResourceViewModel? resource)
    {
        if (resources.Count(resources => resources.Name == resourceName) == 1)
        {
            resource = resources.First(resources => resources.Name == resourceName);
            return true;
        }
        else if (resources.Count(resources => resources.DisplayName == resourceName) == 1)
        {
            resource = resources.First(resources => resources.DisplayName == resourceName);
            return true;
        }

        resource = null;
        return false;
    }
}
