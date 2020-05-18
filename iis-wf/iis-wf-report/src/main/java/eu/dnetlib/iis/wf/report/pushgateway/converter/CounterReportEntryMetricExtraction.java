package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static eu.dnetlib.iis.wf.report.pushgateway.converter.ReportEntryToMetricConverter.*;

/**
 * Support for extraction of metrics from counter report entries.
 */
public class CounterReportEntryMetricExtraction {

    private CounterReportEntryMetricExtraction() {
    }

    @FunctionalInterface
    public interface Extractor {
        ExtractedMetric extract(ReportEntry reportEntry, Map<String, LabeledMetricConf> labeledMetricConfByPattern);
    }

    /**
     * Extracts metric from report entry.
     *
     * @param reportEntry                Report entry to extract metric from.
     * @param labeledMetricConfByPattern Map with keys as regexes matching report entries and values as labeled metric
     *                                   configurations.
     * @return Extracted metric with or without labels. When report key matches a regex from labeled metric conf map a
     * labeled metric is extracted. Metric name, label names and values are taken from corresponding labeled metric conf
     * using default positional label values extractor. When report key does not match any regex from labeled metric conf
     * map a metric without labels is extracted. Metric name is constructed by replacing dots with underscores. To comply
     * with prometheus specs any dashes from metric name are also removed.
     */
    public static ExtractedMetric extractMetricFromReportEntry(ReportEntry reportEntry,
                                                               Map<String, LabeledMetricConf> labeledMetricConfByPattern) {
        return extractMetricFromReportEntry(reportEntry,
                labeledMetricConfByPattern,
                LabelValuesExtraction::extractLabelValuesByPosition);
    }

    /**
     * Extracts metric from report entry using a custom label values extractor.
     */
    public static ExtractedMetric extractMetricFromReportEntry(ReportEntry reportEntry,
                                                               Map<String, LabeledMetricConf> labeledMetricConfByPattern,
                                                               LabelValuesExtraction.Extractor labelValuesExtractor) {
        Double value = Double.parseDouble(reportEntry.getValue().toString());
        List<LabeledMetricConf> matchingLabeledMetricConfs = labeledMetricConfByPattern.entrySet().stream()
                .filter(entry -> keyMatchesPattern(reportEntry.getKey().toString(), entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        if (matchingLabeledMetricConfs.isEmpty()) {
            return new ExtractedMetric(defaultMetricName(reportEntry.getKey().toString()),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    value);
        }
        LabeledMetricConf labeledMetricConf = uniqueElementOrThrow(matchingLabeledMetricConfs, Function.identity());
        List<String> labelValues = labelValuesExtractor.extract(reportEntry.getKey().toString(), labeledMetricConf);
        return labeledMetricConf.toExtractedMetric(labelValues, value);
    }

    private static Boolean keyMatchesPattern(String key, String keyPattern) {
        return Pattern.compile(addStartEndAnchors(keyPattern)).matcher(key).matches();
    }

    private static String addStartEndAnchors(String pattern) {
        return String.format("^%s$", pattern);
    }

    private static String defaultMetricName(String key) {
        return key
                .replace(REPORT_ENTRY_KEY_SEP, METRIC_NAME_SEP)
                .replace("-", "");
    }

}
