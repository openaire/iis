package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.java.stream.ListUtils;
import io.prometheus.client.Gauge;

import java.util.List;

//TODO add description and logging
public class GaugesBuilder {

    private GaugesBuilder() {
    }

    @FunctionalInterface
    public interface BuilderWithLabels {
        Gauge build(String metricName,
                    String help,
                    List<String> labelNames,
                    List<List<String>> labelValues,
                    List<Double> values);
    }

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
