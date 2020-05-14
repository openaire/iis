package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import io.prometheus.client.Gauge;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//TODO: fix description

/**
 * Converts report entries to prometheus gauges.
 * <p>
 * For report entries containing durations gauge's name is created from report key by replacing dots with
 * underscores and adding '_seconds' suffix.
 * <p>
 * <p>
 * For report entries containing counters gauge's name is created from report
 * key value by replacing dots with underscores additionally supporting labels given as a map from metric name to an
 * array of label names.
 */
public class ReportEntryToMetricConverter {

    public static final String REPORT_ENTRY_KEY_SEP = ".";
    public static final String METRIC_NAME_SEP = "_";
    public static final String LABEL_PATTERN_VALUE_TOKEN = "$";
    public static final String DURATION_METRIC_NAME_SUFFIX = "seconds";

    private static final String HELP_PREFIX = "location";

    public static List<Gauge> convert(List<ReportEntry> reportEntries,
                                      String path,
                                      Map<String, LabeledMetricConf> labeledMetricConfByPattern) {
        return convert(reportEntries,
                path,
                labeledMetricConfByPattern,
                CounterReportEntryMetricExtractor::extractMetricFromReportEntry,
                DurationReportEntryMetricExtractor::extractMetricFromReportEntry,
                GaugesBuilder::buildGaugeWithoutLabels,
                GaugesBuilder::buildGaugeWithLabels);
    }

    public static List<Gauge> convert(List<ReportEntry> reportEntries,
                                      String path,
                                      Map<String, LabeledMetricConf> labeledMetricConfByPattern,
                                      CounterReportEntryMetricExtractor.Extractor counterReportEntryMetricExtractor,
                                      DurationReportEntryMetricExtractor.Extractor durationReportEntryMetricExtractor,
                                      GaugesBuilder.BuilderWithoutLabels gaugesBuilderWithoutLabels,
                                      GaugesBuilder.BuilderWithLabels gaugesBuilderWithLabelsBuilder) {
        String help = help(path);
        Stream<Gauge> counters = convertCounterReportEntries(reportEntries.stream()
                        .filter(x -> ReportEntryType.COUNTER.equals(x.getType())), help, labeledMetricConfByPattern,
                counterReportEntryMetricExtractor, gaugesBuilderWithoutLabels, gaugesBuilderWithLabelsBuilder);
        Stream<Gauge> durations = convertDurationReportEntries(reportEntries.stream()
                        .filter(x -> ReportEntryType.DURATION.equals(x.getType())), help, durationReportEntryMetricExtractor,
                gaugesBuilderWithoutLabels);
        return Stream.concat(counters, durations).collect(Collectors.toList());
    }

    private static String help(String path) {
        return String.format("%s:%s", HELP_PREFIX, path);
    }

    private static Stream<Gauge> convertCounterReportEntries(Stream<ReportEntry> reportEntries,
                                                             String help,
                                                             Map<String, LabeledMetricConf> labeledMetricConfByPattern,
                                                             CounterReportEntryMetricExtractor.Extractor metricExtractor,
                                                             GaugesBuilder.BuilderWithoutLabels gaugesBuilderWithoutLabels,
                                                             GaugesBuilder.BuilderWithLabels gaugesBuilderWithLabels) {
        return reportEntries
                .map(x -> metricExtractor.extract(x, labeledMetricConfByPattern))
                .collect(Collectors.groupingBy(ExtractedMetric::getMetricName)).entrySet().stream()
                .flatMap(entry -> {
                    String metricName = entry.getKey();
                    List<ExtractedMetric> extractedMetrics = entry.getValue();
                    Map<String, List<String>> labelNamesByMetricName = labelNamesByMetricName(labeledMetricConfByPattern);
                    return Optional
                            .ofNullable(labelNamesByMetricName.get(metricName))
                            .map(labelNames -> Stream
                                    .of(gaugesBuilderWithLabels.build(metricName,
                                            help,
                                            labelNames,
                                            labelValues(extractedMetrics),
                                            values(extractedMetrics))))
                            .orElseGet(() -> Stream
                                    .of(gaugesBuilderWithoutLabels.build(metricName,
                                            help,
                                            values(extractedMetrics).stream().findFirst().orElseThrow(missingLabelValueException()))));
                });
    }

    private static Map<String, List<String>> labelNamesByMetricName(Map<String, LabeledMetricConf> labeledMetricConfByPattern) {
        return labeledMetricConfByPattern.values().stream()
                .map(x -> new AbstractMap.SimpleEntry<>(x.getMetricName(), labelNames(x.getLabelConfs())))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    private static List<String> labelNames(List<LabelConf> labelConfs) {
        return labelConfs.stream().map(LabelConf::getLabelName).collect(Collectors.toList());
    }

    private static List<List<String>> labelValues(List<ExtractedMetric> extractedMetrics) {
        return extractedMetrics.stream().map(ExtractedMetric::getLabelValues).collect(Collectors.toList());
    }

    private static List<Double> values(List<ExtractedMetric> extractedMetrics) {
        return extractedMetrics.stream().map(ExtractedMetric::getValue).collect(Collectors.toList());
    }

    private static Supplier<RuntimeException> missingLabelValueException() {
        return () -> new RuntimeException("missing label value");
    }

    private static Stream<Gauge> convertDurationReportEntries(Stream<ReportEntry> reportEntries,
                                                              String help,
                                                              DurationReportEntryMetricExtractor.Extractor metricExtractor,
                                                              GaugesBuilder.BuilderWithoutLabels gaugesWithoutLabelsBuilder) {
        return reportEntries
                .map(metricExtractor::extract)
                .map(extractedMetric -> gaugesWithoutLabelsBuilder.build(extractedMetric.getMetricName(), help, extractedMetric.getValue()));
    }

}
