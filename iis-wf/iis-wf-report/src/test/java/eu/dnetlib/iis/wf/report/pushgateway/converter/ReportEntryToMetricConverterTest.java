package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import io.prometheus.client.Gauge;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReportEntryToMetricConverterTest {

    @Test
    public void convertShouldReturnEmptyListOfGaugesOnEmptyReportEntries() {
        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(Collections.emptyList(), "path", Collections.emptyMap());

        // then
        assertTrue(gauges.isEmpty());
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesOnNonEmptyCounterReportEntries() {
        // given
        ReportEntry counterReportEntryWithoutLabel = mock(ReportEntry.class);
        when(counterReportEntryWithoutLabel.getType()).thenReturn(ReportEntryType.COUNTER);
        ReportEntry counterReportEntryWithLabel = mock(ReportEntry.class);
        when(counterReportEntryWithLabel.getType()).thenReturn(ReportEntryType.COUNTER);
        List<ReportEntry> reportEntries = Arrays.asList(counterReportEntryWithoutLabel, counterReportEntryWithLabel);
        LabelConf labelConf = mock(LabelConf.class);
        when(labelConf.getLabelName()).thenReturn("label_name");
        LabeledMetricConf labeledMetricConf = mock(LabeledMetricConf.class);
        when(labeledMetricConf.getMetricName()).thenReturn("metric_name_with_labels");
        when(labeledMetricConf.getLabelConfs()).thenReturn(Collections.singletonList(labelConf));
        Map<String, LabeledMetricConf> labeledMetricConfByPattern = Collections.singletonMap("key", labeledMetricConf);
        ExtractedMetric extractedMetricWithoutLabel = mock(ExtractedMetric.class);
        when(extractedMetricWithoutLabel.getMetricName()).thenReturn("metric_name_without_labels");
        when(extractedMetricWithoutLabel.getLabelValues()).thenReturn(Collections.emptyList());
        when(extractedMetricWithoutLabel.getValue()).thenReturn(1d);
        ExtractedMetric extractedMetricWithLabel = mock(ExtractedMetric.class);
        when(extractedMetricWithLabel.getMetricName()).thenReturn("metric_name_with_labels");
        when(extractedMetricWithLabel.getLabelNames()).thenReturn(Collections.singletonList("label_name"));
        when(extractedMetricWithLabel.getLabelValues()).thenReturn(Collections.singletonList("label_value"));
        when(extractedMetricWithLabel.getValue()).thenReturn(10d);
        CounterReportEntryMetricExtractor.Extractor counterMetricExtractor = mock(CounterReportEntryMetricExtractor.Extractor.class);
        when(counterMetricExtractor.extract(counterReportEntryWithoutLabel, labeledMetricConfByPattern)).thenReturn(extractedMetricWithoutLabel);
        when(counterMetricExtractor.extract(counterReportEntryWithLabel, labeledMetricConfByPattern)).thenReturn(extractedMetricWithLabel);
        Gauge gaugeWithoutLabels = mock(Gauge.class);
        Gauge gaugeWithLabel = mock(Gauge.class);
        GaugesBuilder.BuilderWithoutLabels builderWithoutLabels = mock(GaugesBuilder.BuilderWithoutLabels.class);
        when(builderWithoutLabels.build("metric_name_without_labels", "location:path", 1d))
                .thenReturn(gaugeWithoutLabels);
        GaugesBuilder.BuilderWithLabels builderWithLabels = mock(GaugesBuilder.BuilderWithLabels.class);
        when(builderWithLabels.build("metric_name_with_labels", "location:path", Collections.singletonList("label_name"),
                Collections.singletonList(Collections.singletonList("label_value")), Collections.singletonList(10d))).thenReturn(gaugeWithLabel);

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries,
                "path",
                labeledMetricConfByPattern,
                counterMetricExtractor,
                mock(DurationReportEntryMetricExtractor.Extractor.class),
                builderWithoutLabels,
                builderWithLabels
        );

        // then
        assertEquals(Arrays.asList(gaugeWithLabel, gaugeWithoutLabels), gauges);
    }

    @Test(expected = RuntimeException.class)
    public void convertShouldThrowOnLabelValueExtractionError() {
        // given
        ReportEntry counterReportEntryWithoutLabel = mock(ReportEntry.class);
        when(counterReportEntryWithoutLabel.getType()).thenReturn(ReportEntryType.COUNTER);
        List<ReportEntry> reportEntries = Collections.singletonList(counterReportEntryWithoutLabel);
        Map<String, LabeledMetricConf> labeledMetricConfByPattern = Collections.singletonMap("key", mock(LabeledMetricConf.class));
        ExtractedMetric extractedMetricWithoutLabel = mock(ExtractedMetric.class);
        when(extractedMetricWithoutLabel.getMetricName()).thenReturn("metric_name_without_labels");
        when(extractedMetricWithoutLabel.getLabelValues()).thenReturn(Collections.emptyList());
        when(extractedMetricWithoutLabel.getValue()).thenReturn(null);
        CounterReportEntryMetricExtractor.Extractor counterMetricExtractor = mock(CounterReportEntryMetricExtractor.Extractor.class);
        when(counterMetricExtractor.extract(counterReportEntryWithoutLabel, labeledMetricConfByPattern)).thenReturn(extractedMetricWithoutLabel);

        // when
        ReportEntryToMetricConverter.convert(reportEntries,
                "path",
                labeledMetricConfByPattern,
                counterMetricExtractor,
                mock(DurationReportEntryMetricExtractor.Extractor.class),
                mock(GaugesBuilder.BuilderWithoutLabels.class),
                mock(GaugesBuilder.BuilderWithLabels.class)
        );
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesOnNonEmptyDurationReportEntries() {
        // given
        ReportEntry durationReportEntry = mock(ReportEntry.class);
        when(durationReportEntry.getType()).thenReturn(ReportEntryType.DURATION);
        List<ReportEntry> reportEntries = Collections.singletonList(durationReportEntry);
        Map<String, LabeledMetricConf> labeledMetricConfByPattern = Collections.singletonMap("key", mock(LabeledMetricConf.class));
        ExtractedMetric extractedMetric = mock(ExtractedMetric.class);
        when(extractedMetric.getMetricName()).thenReturn("metric_name");
        when(extractedMetric.getValue()).thenReturn(1d);
        DurationReportEntryMetricExtractor.Extractor durationMetricExtractor = mock(DurationReportEntryMetricExtractor.Extractor.class);
        when(durationMetricExtractor.extract(durationReportEntry)).thenReturn(extractedMetric);
        Gauge gauge = mock(Gauge.class);
        GaugesBuilder.BuilderWithoutLabels builderWithoutLabels = mock(GaugesBuilder.BuilderWithoutLabels.class);
        when(builderWithoutLabels.build("metric_name", "location:path", 1d)).thenReturn(gauge);

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries,
                "path",
                labeledMetricConfByPattern,
                mock(CounterReportEntryMetricExtractor.Extractor.class),
                durationMetricExtractor,
                builderWithoutLabels,
                mock(GaugesBuilder.BuilderWithLabels.class)
        );

        // then
        assertEquals(Collections.singletonList(gauge), gauges);
    }

}