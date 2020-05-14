package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CounterReportEntryMetricExtractor {

    private CounterReportEntryMetricExtractor() {
    }

    @FunctionalInterface
    public interface Extractor {
        ExtractedMetric extract(ReportEntry reportEntry, Map<String, LabeledMetricConf> labeledMetricConfByPattern);
    }

    public static ExtractedMetric extractMetricFromReportEntry(ReportEntry reportEntry,
                                                               Map<String, LabeledMetricConf> labeledMetricConfByPattern) {
        return extractMetricFromReportEntry(reportEntry,
                labeledMetricConfByPattern,
                MetricNameCustomizer::dashRemover,
                LabelValuesExtractor::extractLabelValuesByPosition);
    }

    public static ExtractedMetric extractMetricFromReportEntry(ReportEntry reportEntry,
                                                               Map<String, LabeledMetricConf> labeledMetricConfByPattern,
                                                               MetricNameCustomizer.Customizer metricNameCustomizer,
                                                               LabelValuesExtractor.Extractor labelValuesExtractor) {
        return labeledMetricConfByPattern.entrySet().stream()
                .filter(entry -> keyMatchesPattern(reportEntry.getKey().toString(), entry.getKey()))
                .findFirst()
                .map(entry -> {
                    LabeledMetricConf labeledMetricConf = entry.getValue();
                    return new ExtractedMetric(metricNameCustomizer.customize(labeledMetricConf.getMetricName()),
                            labeledMetricConf.getLabelConfs().stream().map(LabelConf::getLabelName).collect(Collectors.toList()),
                            labelValuesExtractor.extract(reportEntry.getKey().toString(), labeledMetricConf),
                            Double.parseDouble(reportEntry.getValue().toString()));
                })
                .orElseGet(() -> new ExtractedMetric(metricNameCustomizer.customize(defaultMetricName(reportEntry.getKey().toString())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Double.parseDouble(reportEntry.getValue().toString())));
    }

    private static Boolean keyMatchesPattern(String key, String keyPattern) {
        return Pattern.compile(addStartEndAnchors(keyPattern)).matcher(key).find();
    }

    private static String addStartEndAnchors(String pattern) {
        return String.format("^%s$", pattern);
    }

    private static String defaultMetricName(String key) {
        return key.replace(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP, ReportEntryToMetricConverter.METRIC_NAME_SEP);
    }

}
