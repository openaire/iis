package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
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
