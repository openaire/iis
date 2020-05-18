package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;

import java.util.Collections;

/**
 * Support for extraction of metrics from duration report entries.
 */
public class DurationReportEntryMetricExtraction {

    private DurationReportEntryMetricExtraction() {
    }

    @FunctionalInterface
    public interface Extractor {
        ExtractedMetric extract(ReportEntry reportEntry);
    }

    /**
     * Extracts metric from report entry.
     *
     * @param reportEntry Report entry to extract metric from.
     * @return Extracted metric without labels. Metric name is constructed by replacing dots with underscores and adding
     * `_seconds` suffix. To comply with prometheus specs any dashes from metric name are also removed. Metric value is
     * given in seconds by converting report entry value given in milliseconds to seconds.
     */
    public static ExtractedMetric extractMetricFromReportEntry(ReportEntry reportEntry) {
        String metricName = defaultMetricName(reportEntry);
        double value = millisecondsToSeconds(Double.parseDouble(reportEntry.getValue().toString()));
        return new ExtractedMetric(metricName, Collections.emptyList(), Collections.emptyList(), value);
    }

    private static String defaultMetricName(ReportEntry reportEntry) {
        return String.format("%s%s%s",
                reportEntry.getKey().toString()
                        .replace(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP, ReportEntryToMetricConverter.METRIC_NAME_SEP)
                        .replace("-", ""),
                ReportEntryToMetricConverter.METRIC_NAME_SEP,
                ReportEntryToMetricConverter.DURATION_METRIC_NAME_SUFFIX);
    }

    private static Double millisecondsToSeconds(Double milliseconds) {
        return milliseconds / 1000;
    }

}
