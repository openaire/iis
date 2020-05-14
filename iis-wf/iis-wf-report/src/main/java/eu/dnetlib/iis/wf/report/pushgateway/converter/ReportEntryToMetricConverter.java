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
                                    firstElementFromSingletonListOrThrow(extractedMetrics, ExtractedMetric::getValue)));
                });
    }

    private static Boolean isLabeledMetric(String metricName, Set<String> labeledMetricNames) {
        return labeledMetricNames.contains(metricName);
    }

    private static List<String> labelNames(List<ExtractedMetric> extractedMetrics) {
        return firstElementFromSingletonListOrThrow(extractedMetrics, ExtractedMetric::getLabelNames);
    }

    private static List<List<String>> labelValues(List<ExtractedMetric> extractedMetrics) {
        return extractedMetrics.stream().map(ExtractedMetric::getLabelValues).collect(Collectors.toList());
    }

    private static List<Double> values(List<ExtractedMetric> extractedMetrics) {
        return extractedMetrics.stream().map(ExtractedMetric::getValue).collect(Collectors.toList());
    }

    private static Stream<Gauge> convertDurationReportEntries(Stream<ReportEntry> reportEntries,
                                                              String help,
                                                              DurationReportEntryMetricExtractor.Extractor metricExtractor,
                                                              GaugesBuilder.BuilderWithoutLabels gaugesWithoutLabelsBuilder) {
        return reportEntries
                .map(metricExtractor::extract)
                .map(extractedMetric -> gaugesWithoutLabelsBuilder.build(extractedMetric.getMetricName(), help, extractedMetric.getValue()));
    }

    private static <E, X> X firstElementFromSingletonListOrThrow(List<E> list, Function<E, X> mapper) {
        List<X> collect = list.stream().map(mapper).distinct().collect(Collectors.toList());
        if (collect.size() != 1) {
            throw new RuntimeException(String.format("list size is not 1: size=%s", collect.size()));
        }
        return collect.get(0);
    }

}
