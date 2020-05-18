package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import io.prometheus.client.Gauge;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Report entries to gauges conversion support.
 */
public class ReportEntryToMetricConverter {

    public static final String REPORT_ENTRY_KEY_SEP = ".";
    public static final String METRIC_NAME_SEP = "_";
    public static final String LABEL_PATTERN_VALUE_TOKEN = "$";
    public static final String DURATION_METRIC_NAME_SUFFIX = "seconds";

    private static final String HELP_PREFIX = "location";

    /**
     * Converts a list of report entries to a list of corresponding gauges using default extractors for counters, durations
     * and default builders for gauges with and without labels.
     *
     * @param reportEntries              List of report entries to convert.
     * @param path                       Path of the dir from which report entries are taken, to be used as gauge help string,
     * @param labeledMetricConfByPattern Map with keys as regexes matching report entries and values as labeled metric
     *                                   configurations.
     * @return List of gauges converted from report entries. List can contain counters and durations. For counter report
     * entries with keys matching keys of the labeled metric conf map, gauges with labels will be created. For other report
     * entries gauges without labels will be created.
     */
    public static List<Gauge> convert(List<ReportEntry> reportEntries,
                                      String path,
                                      Map<String, LabeledMetricConf> labeledMetricConfByPattern) {
        return convert(reportEntries,
                path,
                labeledMetricConfByPattern,
                CounterReportEntryMetricExtraction::extractMetricFromReportEntry,
                DurationReportEntryMetricExtraction::extractMetricFromReportEntry,
                GaugesCreation::buildGaugeWithoutLabels,
                GaugesCreation::buildGaugeWithLabels);
    }

    /**
     * Converts a list of report entries to a list of corresponding gauges using custom extractors for counters, durations
     * and custom builders for gauges with and without labels.
     */
    public static List<Gauge> convert(List<ReportEntry> reportEntries,
                                      String path,
                                      Map<String, LabeledMetricConf> labeledMetricConfByPattern,
                                      CounterReportEntryMetricExtraction.Extractor counterReportEntryMetricExtractor,
                                      DurationReportEntryMetricExtraction.Extractor durationReportEntryMetricExtractor,
                                      GaugesCreation.BuilderWithoutLabels gaugesBuilderWithoutLabels,
                                      GaugesCreation.BuilderWithLabels gaugesBuilderWithLabelsBuilder) {
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
                                                             CounterReportEntryMetricExtraction.Extractor metricExtractor,
                                                             GaugesCreation.BuilderWithoutLabels gaugesBuilderWithoutLabels,
                                                             GaugesCreation.BuilderWithLabels gaugesBuilderWithLabels) {
        Set<String> labeledMetricNames = labeledMetricConfByPattern.values().stream()
                .map(LabeledMetricConf::getMetricName)
                .collect(Collectors.toSet());
        return reportEntries
                .map(x -> metricExtractor.extract(x, labeledMetricConfByPattern))
                .collect(Collectors.groupingBy(ExtractedMetric::getMetricName)).entrySet().stream()
                .flatMap(entry -> {
                    String metricName = entry.getKey();
                    List<ExtractedMetric> extractedMetrics = entry.getValue();
                    if (isLabeledMetric(metricName, labeledMetricNames)) {
                        return Stream
                                .of(gaugesBuilderWithLabels.build(metricName,
                                        help,
                                        labelNames(extractedMetrics),
                                        labelValues(extractedMetrics),
                                        values(extractedMetrics)));
                    }
                    return Stream
                            .of(gaugesBuilderWithoutLabels.build(metricName,
                                    help,
                                    uniqueElementOrThrow(extractedMetrics, ExtractedMetric::getValue)));
                });
    }

    private static Boolean isLabeledMetric(String metricName, Set<String> labeledMetricNames) {
        return labeledMetricNames.contains(metricName);
    }

    private static List<String> labelNames(List<ExtractedMetric> extractedMetrics) {
        return uniqueElementOrThrow(extractedMetrics, ExtractedMetric::getLabelNames);
    }

    private static List<List<String>> labelValues(List<ExtractedMetric> extractedMetrics) {
        return extractedMetrics.stream().map(ExtractedMetric::getLabelValues).collect(Collectors.toList());
    }

    private static List<Double> values(List<ExtractedMetric> extractedMetrics) {
        return extractedMetrics.stream().map(ExtractedMetric::getValue).collect(Collectors.toList());
    }

    private static Stream<Gauge> convertDurationReportEntries(Stream<ReportEntry> reportEntries,
                                                              String help,
                                                              DurationReportEntryMetricExtraction.Extractor metricExtractor,
                                                              GaugesCreation.BuilderWithoutLabels gaugesWithoutLabelsBuilder) {
        return reportEntries
                .map(metricExtractor::extract)
                .map(extractedMetric -> gaugesWithoutLabelsBuilder.build(extractedMetric.getMetricName(), help, extractedMetric.getValue()));
    }

    public static <E, X> X uniqueElementOrThrow(List<E> list, Function<E, X> mapper) {
        List<X> collect = list.stream().map(mapper).distinct().collect(Collectors.toList());
        if (collect.size() != 1) {
            throw new RuntimeException(String.format("list size is not 1: size=%s", collect.size()));
        }
        return collect.get(0);
    }

}
