package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.java.stream.ListUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LabelValuesExtractor {

    private LabelValuesExtractor() {
    }

    @FunctionalInterface
    public interface Extractor {
        List<String> extract(String key, LabeledMetricConf labeledMetricConf);
    }

    public static List<String> extractLabelValuesByPosition(String key,
                                                            LabeledMetricConf labeledMetricConf) {
        List<String> keyParts = keyParts(key);
        Map<Integer, String> keyPartsByPosition = keyPartsByPosition(keyParts);
        return labeledMetricConf.getLabelConfs().stream()
                .map(LabelConf::getPattern)
                .map(pattern -> replacePatternTokens(pattern, keyPartsByPosition))
                .collect(Collectors.toList());
    }

    private static List<String> keyParts(String key) {
        return Arrays.asList(key.split(Pattern.quote(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP)));
    }

    private static Map<Integer, String> keyPartsByPosition(List<String> keyParts) {
        return ListUtils.zipWithIndex(keyParts).stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    private static String replacePatternTokens(String pattern, Map<Integer, String> reducedKeyPartsByPosition) {
        String result = pattern;
        for (Map.Entry<Integer, String> entry : reducedKeyPartsByPosition.entrySet()) {
            result = result.replace(formatPatternToken(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private static String formatPatternToken(Integer position) {
        return String.format("%s%d", ReportEntryToMetricConverter.LABEL_PATTERN_VALUE_TOKEN, position);
    }

}
