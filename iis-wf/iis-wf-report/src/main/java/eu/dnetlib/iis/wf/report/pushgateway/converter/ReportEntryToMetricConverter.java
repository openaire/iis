package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import io.prometheus.client.Gauge;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReportEntryToMetricConverter {

    private static final String HELP_PREFIX = "location";
    private static final String REPORT_ENTRY_KEY_SEP = ".";
    private static final String METRIC_NAME_SEP = "_";

    public static List<Gauge> convert(List<ReportEntry> reportEntries,
                                      String path,
                                      Map<String, String[]> labelNamesByMetricName) {
        String help = help(path);
        List<Gauge> durations = convertDurations(reportEntries.stream()
                .filter(x -> ReportEntryType.DURATION.equals(x.getType())), help);
        List<Gauge> counters = convertCounters(reportEntries.stream()
                .filter(x -> ReportEntryType.COUNTER.equals(x.getType())), help, labelNamesByMetricName);
        return Stream.concat(durations.stream(), counters.stream()).collect(Collectors.toList());
    }

    private static String help(String path) {
        return String.format("%s:%s", HELP_PREFIX, path);
    }

    private static List<Gauge> convertCounters(Stream<ReportEntry> reportEntries,
                                               String help,
                                               Map<String, String[]> labelNamesByMetricName) {
        return reportEntries
                .map(x -> metricNameAndLabelValuesAndValueForReportEntry(x, labelNamesByMetricName.keySet()))
                .collect(Collectors.groupingBy(x -> x.metricName)).entrySet().stream()
                .flatMap(entry -> {
                    String metricName = entry.getKey();
                    List<MetricNameAndLabelValuesAndValue> metricNameAndLabelValuesAndValueList = entry.getValue();
                    return Optional
                            .ofNullable(labelNamesByMetricName.get(metricName))
                            .map(labelNames -> Stream
                                    .of(buildGaugeWithLabels(metricName, help, labelNames, metricNameAndLabelValuesAndValueList)))
                            .orElseGet(() -> Stream
                                    .of(buildGaugeWithoutLabels(metricName, help, metricNameAndLabelValuesAndValueList.stream().map(x -> x.value).iterator().next())));
                })
                .collect(Collectors.toList());
    }

    private static class MetricNameAndLabelValuesAndValue {
        private final String metricName;
        private final String[] labelValues;
        private final double value;

        private MetricNameAndLabelValuesAndValue(String metricName,
                                                 String[] labelValues,
                                                 double value) {
            this.metricName = metricName;
            this.labelValues = labelValues;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricNameAndLabelValuesAndValue that = (MetricNameAndLabelValuesAndValue) o;
            return Double.compare(that.value, value) == 0 &&
                    Objects.equals(metricName, that.metricName) &&
                    Arrays.equals(labelValues, that.labelValues);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(metricName, value);
            result = 31 * result + Arrays.hashCode(labelValues);
            return result;
        }
    }

    private static MetricNameAndLabelValuesAndValue metricNameAndLabelValuesAndValueForReportEntry(ReportEntry reportEntry,
                                                                                                   Set<String> metricNames) {
        String rawMetricName = reportEntry.getKey().toString().replace(REPORT_ENTRY_KEY_SEP, METRIC_NAME_SEP);
        return metricNames.stream()
                .filter(rawMetricName::startsWith)
                .findAny()
                .map(metricName -> new MetricNameAndLabelValuesAndValue(metricName,
                        labelValuesFromRawMetricName(rawMetricName, metricName),
                        Double.parseDouble(reportEntry.getValue().toString()))
                )
                .orElseGet(() -> new MetricNameAndLabelValuesAndValue(rawMetricName,
                        new String[]{},
                        Double.parseDouble(reportEntry.getValue().toString())));
    }

    private static String[] labelValuesFromRawMetricName(String rawMetricName,
                                                         String metricName) {
        return rawMetricName.substring(metricName.length() + 1).split(METRIC_NAME_SEP);
    }

    private static List<Gauge> convertDurations(Stream<ReportEntry> reportEntries,
                                                String help) {
        return reportEntries
                .map(reportEntry -> {
                    String metricName = durationMetricName(reportEntry);
                    double value = millisecondsToSeconds(Double.parseDouble(reportEntry.getValue().toString()));
                    return buildGaugeWithoutLabels(metricName, help, value);
                })
                .collect(Collectors.toList());
    }

    private static String durationMetricName(ReportEntry reportEntry) {
        return String.format("%s_seconds",
                reportEntry.getKey().toString().replace(REPORT_ENTRY_KEY_SEP, METRIC_NAME_SEP));
    }

    private static Double millisecondsToSeconds(Double milliseconds) {
        return milliseconds / 1000;
    }

    private static Gauge buildGaugeWithLabels(String metricName,
                                              String help,
                                              String[] labelNames,
                                              List<MetricNameAndLabelValuesAndValue> metricNameAndLabelValuesAndValueList) {
        Gauge gauge = Gauge.build()
                .name(metricName)
                .help(help)
                .labelNames(labelNames)
                .create();
        metricNameAndLabelValuesAndValueList.forEach(x -> gauge.labels(x.labelValues).set(x.value));
        return gauge;
    }

    private static Gauge buildGaugeWithoutLabels(String metricName,
                                                 String help,
                                                 double value) {
        Gauge gauge = Gauge.build()
                .name(metricName)
                .help(help)
                .create();
        gauge.set(value);
        return gauge;
    }

}
