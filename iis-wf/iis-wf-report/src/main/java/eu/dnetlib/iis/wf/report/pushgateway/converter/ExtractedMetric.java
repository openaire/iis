package eu.dnetlib.iis.wf.report.pushgateway.converter;

import java.util.List;
import java.util.Objects;

/**
 * Proxy class with values needed for gauge creation, with or without labels. For metrics without labels an empty lists
 * are used for label names and label values.
 */
public class ExtractedMetric {
    private final String metricName;
    private final List<String> labelNames;
    private final List<String> labelValues;
    private final Double value;

    public ExtractedMetric(String metricName,
                           List<String> labelNames,
                           List<String> labelValues,
                           Double value) {
        this.metricName = metricName;
        this.labelNames = labelNames;
        this.labelValues = labelValues;
        this.value = value;
    }

    public String getMetricName() {
        return metricName;
    }

    public List<String> getLabelNames() {
        return labelNames;
    }

    public List<String> getLabelValues() {
        return labelValues;
    }

    public Double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtractedMetric that = (ExtractedMetric) o;
        return Objects.equals(metricName, that.metricName) &&
                Objects.equals(labelNames, that.labelNames) &&
                Objects.equals(labelValues, that.labelValues) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricName, labelNames, labelValues, value);
    }

}
