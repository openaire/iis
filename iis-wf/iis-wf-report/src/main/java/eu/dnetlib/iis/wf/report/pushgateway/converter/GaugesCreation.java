package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.java.stream.ListUtils;
import io.prometheus.client.Gauge;

import java.util.List;

/**
 * Support for creation of gauges.
 */
public class GaugesCreation {

    private GaugesCreation() {
    }

    @FunctionalInterface
    public interface BuilderWithLabels {
        Gauge build(String metricName,
                    String help,
                    List<String> labelNames,
                    List<List<String>> labelValues,
                    List<Double> values);
    }

    /**
     * Creates a gauge with labels.
     *
     * @param metricName  Name to be used for resulting gauge.
     * @param help        Gauge help string value.
     * @param labelNames  List of label names.
     * @param labelValues List of label values.
     * @param values      List of gauge values corresponding to label values.
     * @return Gauge with non-empty labels.
     */
    public static Gauge buildGaugeWithLabels(String metricName,
                                             String help,
                                             List<String> labelNames,
                                             List<List<String>> labelValues,
                                             List<Double> values) {
        Gauge gauge = Gauge.build()
                .name(metricName)
                .help(help)
                .labelNames(labelNames.toArray(new String[0]))
                .create();
        ListUtils.zip(labelValues, values).forEach(labelValuesAndValue ->
                gauge.labels(labelValuesAndValue.getLeft().toArray(new String[0])).set(labelValuesAndValue.getRight())
        );
        return gauge;
    }

    @FunctionalInterface
    public interface BuilderWithoutLabels {
        Gauge build(String metricName,
                    String help,
                    double value);
    }

    /**
     * Creates a gauge without labels.
     *
     * @param metricName Name to be used for resulting gauge.
     * @param help       Gauge help string value.
     * @param value      Value of the gauge.
     * @return Gauge with empty labels.
     */
    public static Gauge buildGaugeWithoutLabels(String metricName,
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
