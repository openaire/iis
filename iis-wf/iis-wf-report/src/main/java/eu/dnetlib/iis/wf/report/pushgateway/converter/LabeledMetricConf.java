package eu.dnetlib.iis.wf.report.pushgateway.converter;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Configuration of labeled metric. Labeled metric configuration is given by metric name and a list of label configurations.
 */
public class LabeledMetricConf {
    private String metricName;
    private List<LabelConf> labelConfs;

    public LabeledMetricConf() {
    }

    public LabeledMetricConf(String metricName, List<LabelConf> labelConfs) {
        this.metricName = metricName;
        this.labelConfs = labelConfs;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public List<LabelConf> getLabelConfs() {
        return labelConfs;
    }

    public void setLabelConfs(List<LabelConf> labelConfs) {
        this.labelConfs = labelConfs;
    }

    public ExtractedMetric toExtractedMetric(List<String> labelValues, Double value) {
        return new ExtractedMetric(metricName,
                labelConfs.stream().map(LabelConf::getLabelName).collect(Collectors.toList()),
                labelValues,
                value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LabeledMetricConf that = (LabeledMetricConf) o;
        return Objects.equals(metricName, that.metricName) &&
                Objects.equals(labelConfs, that.labelConfs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricName, labelConfs);
    }
}
