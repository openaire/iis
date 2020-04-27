package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.java.stream.ListUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReportEntryToMetricConverterTest {

    @Test
    public void convertShouldReturnEmptyListOfGaugesOnEmptyReportEntries() {
        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(Collections.emptyList(), "path", Collections.emptyMap());

        // then
        assertTrue(gauges.isEmpty());
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesWithoutLabelsOnNonEmptyCounterReportEntriesAndEmptyLabelsMap() {
        // given
        List<ReportEntry> reportEntries = Arrays.asList(
                ReportEntryFactory.createCounterReportEntry("a.b.c1", 100),
                ReportEntryFactory.createCounterReportEntry("a.b.c2", 200)
        );

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries, "path", Collections.emptyMap());

        // then
        assertEquals(2, gauges.size());
        List<Collector.MetricFamilySamples> samples = gauges.stream()
                .flatMap(x -> x.collect().stream())
                .collect(Collectors.toList());
        Map<String, Collector.MetricFamilySamples> samplesByMetricName = samples.stream()
                .map(sample -> new AbstractMap.SimpleEntry<>(sample.name, sample))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        assertForMetric(samplesByMetricName.get("a_b_c1"),
                "a_b_c1",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 100d)));
        assertForMetric(samplesByMetricName.get("a_b_c2"),
                "a_b_c2",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 200d)));
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesWithoutLabelsOnNonEmptyCounterReportEntriesAndNotMatchingLabelsMap() {
        // given
        List<ReportEntry> reportEntries = Arrays.asList(
                ReportEntryFactory.createCounterReportEntry("a.b.c1", 100),
                ReportEntryFactory.createCounterReportEntry("a.b.c2", 200)
        );
        Map<String, String[]> labelsByMetricName = Collections.singletonMap("x_y", new String[]{"label"});

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries, "path", labelsByMetricName);

        // then
        assertEquals(2, gauges.size());
        List<Collector.MetricFamilySamples> samples = gauges.stream()
                .flatMap(x -> x.collect().stream())
                .collect(Collectors.toList());
        Map<String, Collector.MetricFamilySamples> samplesByMetricName = samples.stream()
                .map(sample -> new AbstractMap.SimpleEntry<>(sample.name, sample))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        assertForMetric(samplesByMetricName.get("a_b_c1"),
                "a_b_c1",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 100d)));
        assertForMetric(samplesByMetricName.get("a_b_c2"),
                "a_b_c2",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 200d)));
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesWithLabelsOnNonEmptyCounterReportEntriesAndMatchingLabelsMap() {
        // given
        List<ReportEntry> reportEntries = Arrays.asList(
                ReportEntryFactory.createCounterReportEntry("a.b1.c1", 100),
                ReportEntryFactory.createCounterReportEntry("a.b1.c2", 200),
                ReportEntryFactory.createCounterReportEntry("a.b2.c1", 100),
                ReportEntryFactory.createCounterReportEntry("a.b2.c2", 200)
        );
        Map<String, String[]> labelsByMetricName = Stream.of(
                new AbstractMap.SimpleEntry<>("a_b1", new String[]{"label1"}),
                new AbstractMap.SimpleEntry<>("a_b2", new String[]{"label2"})
        )
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries, "path", labelsByMetricName);

        // then
        assertEquals(2, gauges.size());
        List<Collector.MetricFamilySamples> samples = gauges.stream()
                .flatMap(x -> x.collect().stream())
                .collect(Collectors.toList());
        Map<String, Collector.MetricFamilySamples> samplesByMetricName = samples.stream()
                .map(sample -> new AbstractMap.SimpleEntry<>(sample.name, sample))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        assertForMetric(samplesByMetricName.get("a_b1"),
                "a_b1",
                "location:path",
                Collections.singletonList("label1"),
                Arrays.asList(new LabelsAndValue(Collections.singletonList("c1"), 100d), new LabelsAndValue(Collections.singletonList("c2"), 200d)));
        assertForMetric(samplesByMetricName.get("a_b2"),
                "a_b2",
                "location:path",
                Collections.singletonList("label2"),
                Arrays.asList(new LabelsAndValue(Collections.singletonList("c1"), 100d), new LabelsAndValue(Collections.singletonList("c2"), 200d)));
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesWithoutLabelsOnNonEmptyDurationReportEntriesAndEmptyLabelsMap() {
        // given
        List<ReportEntry> reportEntries = Arrays.asList(
                ReportEntryFactory.createDurationReportEntry("a.b.c1", 100),
                ReportEntryFactory.createDurationReportEntry("a.b.c2", 200)
        );

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries, "path", Collections.emptyMap());

        // then
        assertEquals(2, gauges.size());
        List<Collector.MetricFamilySamples> samples = gauges.stream()
                .flatMap(x -> x.collect().stream())
                .collect(Collectors.toList());
        Map<String, Collector.MetricFamilySamples> samplesByMetricName = samples.stream()
                .map(sample -> new AbstractMap.SimpleEntry<>(sample.name, sample))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        assertForMetric(samplesByMetricName.get("a_b_c1_seconds"),
                "a_b_c1_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.100d)));
        assertForMetric(samplesByMetricName.get("a_b_c2_seconds"),
                "a_b_c2_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.200d)));
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesWithoutLabelsOnNonEmptyDurationReportEntriesAndNotMatchingLabelsMap() {
        // given
        List<ReportEntry> reportEntries = Arrays.asList(
                ReportEntryFactory.createDurationReportEntry("a.b.c1", 100),
                ReportEntryFactory.createDurationReportEntry("a.b.c2", 200)
        );
        Map<String, String[]> labelsByMetricName = Collections.singletonMap("x_y", new String[]{"label"});

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries, "path", labelsByMetricName);

        // then
        assertEquals(2, gauges.size());
        List<Collector.MetricFamilySamples> samples = gauges.stream()
                .flatMap(x -> x.collect().stream())
                .collect(Collectors.toList());
        Map<String, Collector.MetricFamilySamples> samplesByMetricName = samples.stream()
                .map(sample -> new AbstractMap.SimpleEntry<>(sample.name, sample))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        assertForMetric(samplesByMetricName.get("a_b_c1_seconds"),
                "a_b_c1_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.100d)));
        assertForMetric(samplesByMetricName.get("a_b_c2_seconds"),
                "a_b_c2_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.200d)));
    }

    @Test
    public void convertShouldReturnNonEmptyListOfGaugesWithoutLabelsOnNonEmptyDurationReportEntriesAndMatchingLabelsMap() {
        // given
        List<ReportEntry> reportEntries = Arrays.asList(
                ReportEntryFactory.createDurationReportEntry("a.b1.c1", 100),
                ReportEntryFactory.createDurationReportEntry("a.b1.c2", 200),
                ReportEntryFactory.createDurationReportEntry("a.b2.c1", 100),
                ReportEntryFactory.createDurationReportEntry("a.b2.c2", 200)
        );
        Map<String, String[]> labelsByMetricName = Stream.of(
                new AbstractMap.SimpleEntry<>("a_b1", new String[]{"label1"}),
                new AbstractMap.SimpleEntry<>("a_b2", new String[]{"label2"})
        )
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        // when
        List<Gauge> gauges = ReportEntryToMetricConverter.convert(reportEntries, "path", labelsByMetricName);

        // then
        assertEquals(4, gauges.size());
        List<Collector.MetricFamilySamples> samples = gauges.stream()
                .flatMap(x -> x.collect().stream())
                .collect(Collectors.toList());
        Map<String, Collector.MetricFamilySamples> samplesByMetricName = samples.stream()
                .map(sample -> new AbstractMap.SimpleEntry<>(sample.name, sample))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        assertForMetric(samplesByMetricName.get("a_b1_c1_seconds"),
                "a_b1_c1_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.100d)));
        assertForMetric(samplesByMetricName.get("a_b1_c2_seconds"),
                "a_b1_c2_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.200d)));
        assertForMetric(samplesByMetricName.get("a_b2_c1_seconds"),
                "a_b2_c1_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.100d)));
        assertForMetric(samplesByMetricName.get("a_b2_c2_seconds"),
                "a_b2_c2_seconds",
                "location:path",
                Collections.emptyList(),
                Collections.singletonList(new LabelsAndValue(Collections.emptyList(), 0.200d)));
    }

    private static class LabelsAndValue {
        private final List<String> labels;
        private final double value;

        private LabelsAndValue(List<String> labels, double value) {
            this.labels = labels;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LabelsAndValue that = (LabelsAndValue) o;
            return Double.compare(that.value, value) == 0 &&
                    Objects.equals(labels, that.labels);
        }

        @Override
        public int hashCode() {
            return Objects.hash(labels, value);
        }
    }

    private static void assertForMetric(Collector.MetricFamilySamples metric,
                                        String metricName,
                                        String help,
                                        List<String> labelNames,
                                        List<LabelsAndValue> labelsAndValues) {

        assertEquals(metricName, metric.name);
        assertEquals(help, metric.help);
        assertEquals(Collector.Type.GAUGE, metric.type);

        List<Collector.MetricFamilySamples.Sample> samples = metric.samples;
        assertEquals(labelsAndValues.size(), samples.size());
        ListUtils.zip(samples, labelsAndValues).forEach(pair -> {
            Collector.MetricFamilySamples.Sample sample = pair.getLeft();
            List<String> label = pair.getRight().labels;
            double value = pair.getRight().value;
            assertEquals(labelNames, sample.labelNames);
            assertEquals(label, sample.labelValues);
            assertEquals(value, sample.value, 1e-3);
        });
    }

}