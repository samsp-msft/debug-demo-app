// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Immutable;
using System.Runtime.InteropServices;
using Aspire.Dashboard.Model;
using Aspire.Dashboard.Otlp.Model;
using Aspire.Dashboard.Otlp.Model.MetricValues;
using Aspire.Dashboard.Otlp.Storage;
using Aspire.Dashboard.Resources;
using Microsoft.AspNetCore.Components;
using Microsoft.FluentUI.AspNetCore.Components;

namespace Aspire.Dashboard.Components;

public partial class McpMetricModel : IAsyncDisposable
{
    private readonly OtlpInstrumentData? _instrument;
    //private PeriodicTimer? _tickTimer;
    //private Task? _tickTask;
    private readonly InstrumentViewModel _instrumentViewModel = new InstrumentViewModel();

    public McpMetricModel(
        ApplicationKey applicationKey,
        string meterName,
        string instrumentName,
        TimeSpan duration,
        TelemetryRepository telemetryRepository,
        ILogger logger
        )
    {
        ApplicationKey = applicationKey;
        MeterName = meterName;
        InstrumentName = instrumentName;
        Duration = duration;

        TelemetryRepository = telemetryRepository;
        Logger = logger;

        _instrument = GetInstrument();
        DimensionFilters = ImmutableList.Create(CollectionsMarshal.AsSpan(CreateUpdatedFilters(true)));
        UpdateInstrumentDataAsync(_instrument);
    }

    public required ApplicationKey ApplicationKey { get; set; }

    public required string MeterName { get; set; }

    public required string InstrumentName { get; set; }

    public required TimeSpan Duration { get; set; }

    public required TelemetryRepository TelemetryRepository { get; init; }

    public required ILogger Logger { get; init; }

    public required ThemeManager ThemeManager { get; init; }

    public ImmutableList<DimensionFilterViewModel> DimensionFilters { get; set; } = [];

    private async Task UpdateInstrumentDataAsync(OtlpInstrumentData instrument)
    {
        var matchedDimensions = instrument.Dimensions.Where(MatchDimension).ToList();

        // Only update data in plotly
        await _instrumentViewModel.UpdateDataAsync(instrument.Summary, matchedDimensions);
    }

    private bool MatchDimension(DimensionScope dimension)
    {
        foreach (var dimensionFilter in DimensionFilters)
        {
            if (!MatchFilter(dimension.Attributes, dimensionFilter))
            {
                return false;
            }
        }
        return true;
    }

    private static bool MatchFilter(KeyValuePair<string, string>[] attributes, DimensionFilterViewModel filter)
    {
        // No filter selected.
        if (!filter.SelectedValues.Any())
        {
            return false;
        }

        var value = OtlpHelpers.GetValue(attributes, filter.Name);
        foreach (var item in filter.SelectedValues)
        {
            if (item.Value == value)
            {
                return true;
            }
        }

        return false;
    }

    private OtlpInstrumentData? GetInstrument()
    {
        var endDate = DateTime.UtcNow;
        // Get more data than is being displayed. Histogram graph uses some historical data to calculate bucket counts.
        // It's ok to get more data than is needed here. An additional date filter is applied when building chart values.
        var startDate = endDate.Subtract(Duration + TimeSpan.FromSeconds(30));

        var instrument = TelemetryRepository.GetInstrument(new GetInstrumentRequest
        {
            ApplicationKey = ApplicationKey,
            MeterName = MeterName,
            InstrumentName = InstrumentName,
            StartTime = startDate,
            EndTime = endDate,
        });

        if (instrument == null)
        {
            Logger.LogDebug(
                "Unable to find instrument. ApplicationKey: {ApplicationKey}, MeterName: {MeterName}, InstrumentName: {InstrumentName}",
                ApplicationKey,
                MeterName,
                InstrumentName);
        }

        return instrument;
    }

    private List<DimensionFilterViewModel> CreateUpdatedFilters(bool hasInstrumentChanged)
    {
        var filters = new List<DimensionFilterViewModel>();
        if (_instrument != null)
        {
            foreach (var item in _instrument.KnownAttributeValues.OrderBy(kvp => kvp.Key))
            {
                var dimensionModel = new DimensionFilterViewModel
                {
                    Name = item.Key
                };

                dimensionModel.Values.AddRange(item.Value.Select(v =>
                {
                    var text = v switch
                    {
                        null => Loc[nameof(ControlsStrings.LabelUnset)],
                        { Length: 0 } => Loc[nameof(ControlsStrings.LabelEmpty)],
                        _ => v
                    };
                    return new DimensionValueViewModel
                    {
                        Text = text,
                        Value = v
                    };
                }).OrderBy(v => v.Text));

                filters.Add(dimensionModel);
            }

            foreach (var item in filters)
            {
                item.SelectedValues.Clear();

                if (hasInstrumentChanged)
                {
                    // Select all by default.
                    foreach (var v in item.Values)
                    {
                        item.SelectedValues.Add(v);
                    }
                }
                else
                {
                    var existing = DimensionFilters.SingleOrDefault(m => m.Name == item.Name);
                    if (existing != null)
                    {
                        // Select previously selected.
                        // Automatically select new incoming values if existing values are all selected.
                        var newSelectedValues = (existing.AreAllValuesSelected ?? false)
                            ? item.Values
                            : item.Values.Where(newValue => existing.SelectedValues.Any(existingValue => existingValue.Value == newValue.Value));

                        foreach (var v in newSelectedValues)
                        {
                            item.SelectedValues.Add(v);
                        }
                    }
                    else
                    {
                        // New filter. Select all by default.
                        foreach (var v in item.Values)
                        {
                            item.SelectedValues.Add(v);
                        }
                    }
                }
            }
        }

        return filters;
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }
}
