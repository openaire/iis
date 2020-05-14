package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class PushMetricsProcessMetricPusherCreatorProducerTest {

    @Test
    public void shouldProduceEmptyOnError() {
        // given
        PushMetricsProcess.MetricPusherCreatorProducer metricPusherCreatorProducer = new PushMetricsProcess.MetricPusherCreatorProducer();

        // when
        Optional<MetricPusherCreator> result = metricPusherCreatorProducer.create(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    public static class MetricPusherCreatorImpl implements MetricPusherCreator {

        @Override
        public MetricPusher create(String address) {
            return null;
        }
    }

    @Test
    public void shouldProduceNonEmpty() {
        // given
        PushMetricsProcess.MetricPusherCreatorProducer metricPusherCreatorProducer = new PushMetricsProcess.MetricPusherCreatorProducer();

        // when
        Optional<MetricPusherCreator> result = metricPusherCreatorProducer.create(Collections.singletonMap("metricPusherCreatorClassName",
                String.format("%s$%s", this.getClass().getCanonicalName(), MetricPusherCreatorImpl.class.getSimpleName())));

        // then
        assertTrue(result.isPresent());
    }

}
