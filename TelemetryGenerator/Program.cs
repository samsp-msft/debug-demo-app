// filepath: c:\Users\samsp\source\repos\debug-demo-app\TelemetryGenerator\Program.cs
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Exporter;

// Create a host to configure OpenTelemetry and the application
using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging((context, logging) =>
    {
        logging.ClearProviders();
        logging.AddConsole();

        // Get OTLP configuration from appsettings.json
        var config = context.Configuration;
        var serviceName = config.GetValue<string>("OpenTelemetry:ServiceName") ?? "TelemetryGenerator";
        var serviceVersion = config.GetValue<string>("OpenTelemetry:ServiceVersion") ?? "1.0.0";

        // Configure OpenTelemetry logging
        logging.AddOpenTelemetry(options =>
        {
            options
                .SetResourceBuilder(
                    ResourceBuilder.CreateDefault()
                        .AddService(serviceName, serviceVersion: serviceVersion));

        // Configure OTLP exporter from settings
        options.AddOtlpExporter(); 
        });
    })
    .ConfigureServices((context, services) =>
    {
        // Get OTLP configuration from appsettings.json
        var config = context.Configuration;
        var serviceName = config.GetValue<string>("OpenTelemetry:ServiceName") ?? "TelemetryGenerator";
        var serviceVersion = config.GetValue<string>("OpenTelemetry:ServiceVersion") ?? "1.0.0";

        // Configure OpenTelemetry tracing
        services.AddOpenTelemetry()
            .WithTracing(builder =>
            {
                builder
                    .SetResourceBuilder(
                        ResourceBuilder.CreateDefault()
                            .AddService(serviceName, serviceVersion: serviceVersion))
                    .AddSource("TelemetryGenerator");

                // Configure OTLP exporter from settings
                builder.AddOtlpExporter(); 
            });
    })
    .Build();

// Configure ActivitySource for generating traces
var activitySource = new ActivitySource("TelemetryGenerator");

// Get logger
var logger = host.Services.GetRequiredService<ILogger<Program>>();

// Start the application
Console.WriteLine("OpenTelemetry Test Generator Starting...");
Console.WriteLine("Press any key to generate telemetry. Press Ctrl + C to quit.");

var cts = new CancellationTokenSource();
var generator = new Thread(GenerateTracesLoop);
generator.Start(cts.Token);

host.Run(); 
cts.Cancel();


// Main application loop
void GenerateTracesLoop(object data)
{
    var token = data as CancellationToken? ?? CancellationToken.None;
    while (!token.IsCancellationRequested)
    {
        GenerateRandomTraces(activitySource, logger, 5);
        var delay = Random.Shared.Next(100, 3000); // Random delay between 0.1 and 3 seconds
        if (!token.IsCancellationRequested) Task.Delay(delay).Wait();
    }
}

void GenerateSimpleLog(ILogger logger)
{
    logger.LogTrace("This is a TRACE level log message");
    logger.LogDebug("This is a DEBUG level log message");
    logger.LogInformation("This is an INFO level log message");
    logger.LogWarning("This is a WARNING level log message");
    logger.LogError("This is an ERROR level log message");
    logger.LogCritical("This is a CRITICAL level log message");
}

// Generate structured logs with various properties
void GenerateStructuredLog(ILogger logger)
{
    var orderDetails = new
    {
        OrderId = Guid.NewGuid(),
        CustomerId = $"CUST-{Random.Shared.Next(1000, 9999)}",
        Items = Random.Shared.Next(1, 10),
        TotalAmount = Math.Round(Random.Shared.NextDouble() * 1000, 2),
        Timestamp = DateTimeOffset.UtcNow
    };

    logger.LogInformation("Order {OrderId} placed for customer {CustomerId} with {Items} items for ${TotalAmount}",
        orderDetails.OrderId, orderDetails.CustomerId, orderDetails.Items, orderDetails.TotalAmount);

    // Log with additional context as JSON
    logger.LogInformation("Order details: {OrderDetails}",
        JsonSerializer.Serialize(orderDetails));
}

// Generate a simple trace with a single span
async Task GenerateSimpleTrace(ActivitySource activitySource)
{
    using var activity = activitySource.StartActivity("SimpleOperation");
    activity?.SetTag("operation.type", "simple");
    activity?.SetTag("operation.id", Guid.NewGuid().ToString());

    // Simulate work
    await Task.Delay(Random.Shared.Next(100, 500));

    activity?.SetStatus(ActivityStatusCode.Ok);
}

// Generate a complex trace with multiple nested spans
async Task GenerateComplexTrace(ActivitySource activitySource, ILogger logger)
{
    using var rootActivity = activitySource.StartActivity("ComplexOperation");
    rootActivity?.SetTag("operation.type", "complex");
    rootActivity?.SetTag("operation.id", Guid.NewGuid().ToString());

    logger.LogInformation("Starting complex operation {OperationId}", rootActivity?.TraceId);

    // First child operation
    await ExecuteChildOperation(activitySource, "Database.Query", 300, 600);

    // Second set of child operations (parallel)
    var tasks = new List<Task>();
    for (int i = 0; i < 3; i++)
    {
        tasks.Add(ExecuteChildOperation(activitySource, $"API.Call.{i}", 200, 400));
    }
    await Task.WhenAll(tasks);

    // Final operation
    await ExecuteChildOperation(activitySource, "Results.Processing", 250, 500);

    logger.LogInformation("Completed complex operation {OperationId}", rootActivity?.TraceId);
    rootActivity?.SetStatus(ActivityStatusCode.Ok);
}

// Helper method to execute a child operation within a trace
async Task ExecuteChildOperation(ActivitySource activitySource, string name, int minDelay, int maxDelay)
{
    using var activity = activitySource.StartActivity(name);
    activity?.SetTag("operation.name", name);
    activity?.SetTag("started.at", DateTimeOffset.UtcNow);

    // Simulate work
    await Task.Delay(Random.Shared.Next(minDelay, maxDelay));

    // Randomly add events
    if (Random.Shared.Next(0, 2) == 1)
    {
        activity?.AddEvent(new ActivityEvent("operation.milestone",
            DateTimeOffset.UtcNow,
            new ActivityTagsCollection { { "milestone.name", "halfway" } }));
    }

    activity?.SetStatus(ActivityStatusCode.Ok);
}

// Generate multiple traces with random depths and spans
void GenerateRandomTraces(ActivitySource activitySource, ILogger logger, int count)
{
    logger.LogInformation("Generating {Count} random traces", count);

    for (int i = 0; i < count; i++)
    {
        int depth = Random.Shared.Next(1, 6); // Random depth between 1 and 5
        GenerateRandomDepthTrace(activitySource, logger, $"RandomTrace.{i}", depth);
    }
}

// Generate a trace with random depth
void GenerateRandomDepthTrace(ActivitySource activitySource, ILogger logger, string name, int maxDepth)
{
    using var rootActivity = activitySource.StartActivity(name);
    rootActivity?.SetTag("trace.depth", maxDepth);

    logger.LogInformation("Starting random trace {Name} with max depth {Depth}", name, maxDepth);

    var t = GenerateNestedSpans(activitySource, name, maxDepth, 1);

    logger.LogInformation("Completed random trace {Name}", name);
    rootActivity?.SetStatus(ActivityStatusCode.Ok);
}

// Recursively generate nested spans
async Task GenerateNestedSpans(ActivitySource activitySource, string baseName, int maxDepth, int currentDepth)
{
    if (currentDepth > maxDepth)
    {
        return;
    }

    // Generate 1-3 spans at this level
    int spansAtLevel = Random.Shared.Next(1, 4);

    for (int i = 0; i < spansAtLevel; i++)
    {
        string spanName = $"{baseName}.Level{currentDepth}.Span{i}";
        using var activity = activitySource.StartActivity(spanName);

        activity?.SetTag("depth", currentDepth);
        activity?.SetTag("index", i);

        // Add random tags
        if (Random.Shared.Next(0, 2) == 1)
        {
            activity?.SetTag("random.value", Random.Shared.NextDouble());
        }

        // Simulate work
        await Task.Delay(Random.Shared.Next(50, 200));

        // Recursively create child spans
        if (Random.Shared.Next(0, 10) < 7) // 70% chance to create children
        {
            await GenerateNestedSpans(activitySource, spanName, maxDepth, currentDepth + 1);
        }

        activity?.SetStatus(ActivityStatusCode.Ok);
    }
}
