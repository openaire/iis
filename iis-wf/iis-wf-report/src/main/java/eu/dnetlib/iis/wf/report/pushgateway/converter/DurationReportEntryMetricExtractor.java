package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;

import java.util.Collections;

//TODO add description and logging
public class DurationReportEntryMetricExtractor {

    private DurationReportEntryMetricExtractor() {
    }

    @FunctionalInterface
    public interface Extractor {
        ExtractedMetric extract(ReportEntry reportEntry);
    }

    public static ExtractedMetric extractMetricFromReportEntry(ReportEntry reportEntry) {
        return extractMetricFromReportEntry(reportEntry, MetricNameCustomizer::dashRemover);
    }

    public static ExtractedMetric extractMetricFromReportEntry(ReportEntry reportEntry,
                                                               MetricNameCustomizer.Customizer metricNameCustomizer) {
        String metricName = metricNameCustomizer.customize(defaultMetricName(reportEntry));
        double value = millisecondsToSeconds(Double.parseDouble(reportEntry.getValue().toString()));
        return new ExtractedMetric(metricName, Collections.emptyList(), value);
    }

    private static String defaultMetricName(ReportEntry reportEntry) {
        return String.format("%s%s%s",
                reportEntry.getKey().toString()
                        .replace(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP, ReportEntryToMetricConverter.METRIC_NAME_SEP),
                ReportEntryToMetricConverter.METRIC_NAME_SEP,
                ReportEntryToMetricConverter.DURATION_METRIC_NAME_SUFFIX);
    }

    private static Double millisecondsToSeconds(Double milliseconds) {
        return milliseconds / 1000;
    }

}
