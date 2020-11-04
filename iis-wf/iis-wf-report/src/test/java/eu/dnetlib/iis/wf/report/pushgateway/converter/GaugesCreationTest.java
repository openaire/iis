package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.utils.ListUtils;
import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GaugesCreationTest {

    @Test
    public void shouldBuildGaugeWithLabels() {
        // given
        String metricName = "metric_name";
        String help = "help";
        List<String> labelNames = Arrays.asList("label_name_1", "label_name_2");
        List<List<String>> labelValues = Arrays.asList(
                Arrays.asList("label_value_a1", "label_value_a2"),
                Arrays.asList("label_value_b1", "label_value_b2")
        );
        List<Double> values = Arrays.asList(1d, 2d);

        // when
        Gauge gauge = GaugesCreation.buildGaugeWithLabels(metricName, help, labelNames, labelValues, values);

        // then
        assertForGauge(gauge, metricName, help, labelNames, labelValues, values);
    }

    @Test
    public void shouldBuildGaugeWithoutLabels() {
        // given
        String metricName = "metric_name";
        String help = "help";
        double value = 1d;

        // when
        Gauge gauge = GaugesCreation.buildGaugeWithoutLabels(metricName, help, value);

        // then
        assertForGauge(gauge, metricName, help, Collections.emptyList(), Collections.singletonList(Collections.emptyList()), Collections.singletonList(value));
    }

    private static void assertForGauge(Gauge gauge,
                                       String metricName,
                                       String help,
                                       List<String> labelNames,
                                       List<List<String>> labelValues,
                                       List<Double> values) {
        List<Collector.MetricFamilySamples> listOfMetricFamilySamples = gauge.collect();
        listOfMetricFamilySamples.forEach(metricFamilySamples -> {
            assertEquals(metricName, metricFamilySamples.name);
            assertEquals(help, metricFamilySamples.help);
            assertEquals(Collector.Type.GAUGE, metricFamilySamples.type);
            assertEquals(values.size(), metricFamilySamples.samples.size());
            ListUtils.zipWithIndex(metricFamilySamples.samples).forEach(pair -> {
                Integer idx = pair.getLeft();
                Collector.MetricFamilySamples.Sample sample = pair.getRight();
                assertEquals(labelNames, sample.labelNames);
                assertEquals(labelValues.get(idx), sample.labelValues);
                assertEquals(values.get(idx), sample.value, 1e-3);
            });
        });
    }

}