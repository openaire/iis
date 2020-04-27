package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class PushMetricsProcessLabelNamesByMetricNameProducerTest {

    @Test
    public void shouldProduceEmptyOnError() {
        // given
        PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new PushMetricsProcess.LabelNamesByMetricNameProducer();

        // when
        Optional<Map<String, String[]>> result = labelNamesByMetricNameProducer.create(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyMapOnEmptyParameters() {
        // given
        PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new PushMetricsProcess.LabelNamesByMetricNameProducer();

        // when
        Optional<Map<String, String[]>> result = labelNamesByMetricNameProducer.create(Collections.emptyMap());

        // then
        assertEquals(Optional.of(Collections.<String, String[]>emptyMap()), result);
    }

    @Test
    public void shouldProduceEmptyMapOnParametersWithoutLabeledMetrics() {
        // given
        PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new PushMetricsProcess.LabelNamesByMetricNameProducer();

        // when
        Optional<Map<String, String[]>> result = labelNamesByMetricNameProducer.create(Collections.singletonMap("notLabeledMetric", "label"));

        // then
        assertEquals(Optional.of(Collections.<String, String[]>emptyMap()), result);
    }

    @Test
    public void shouldProduceEmptyOnParametersWithLabeledMetricWithoutName() {
        // given
        PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new PushMetricsProcess.LabelNamesByMetricNameProducer();

        // when
        Optional<Map<String, String[]>> result = labelNamesByMetricNameProducer.create(Collections.singletonMap("labeledMetric", "label"));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnParametersWithLabeledMetricWithNullLabels() {
        // given
        PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new PushMetricsProcess.LabelNamesByMetricNameProducer();

        // when
        Optional<Map<String, String[]>> result = labelNamesByMetricNameProducer.create(Collections.singletonMap("labeledMetric.metricName", null));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnParametersWithLabeledMetricWithoutLabels() {
        // given
        PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new PushMetricsProcess.LabelNamesByMetricNameProducer();

        // when
        Optional<Map<String, String[]>> result = labelNamesByMetricNameProducer.create(Collections.singletonMap("labeledMetric.metricName", ""));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceNonEmpty() {
        // given
        PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new PushMetricsProcess.LabelNamesByMetricNameProducer();

        // when
        Optional<Map<String, String[]>> result = labelNamesByMetricNameProducer.create(Collections.singletonMap("labeledMetric.metricName", "label"));

        // then
        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        assertArrayEquals(new String[]{"label"}, result.get().get("metricName"));
    }

}
