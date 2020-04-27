package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.wf.report.pushgateway.process.PushMetricsProcess;
import eu.dnetlib.iis.wf.report.pushgateway.pusher.MetricPusher;
import eu.dnetlib.iis.wf.report.pushgateway.pusher.MetricPusherCreator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PushMetricsProcessMetricPusherProducerTest {

    @Test
    public void shouldProduceEmptyWhenError() {
        // given
        PushMetricsProcess.MetricPusherProducer metricPusherProducer = new PushMetricsProcess.MetricPusherProducer();

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreator.create("the.address")).thenReturn(mock(MetricPusher.class));

        // when
        Optional<MetricPusher> result = metricPusherProducer.create(metricPusherCreator, null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyWhenMetricPusherAddressIsMissing() {
        // given
        PushMetricsProcess.MetricPusherProducer metricPusherProducer = new PushMetricsProcess.MetricPusherProducer();

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreator.create("the.address")).thenReturn(mock(MetricPusher.class));

        // when
        Optional<MetricPusher> result = metricPusherProducer.create(metricPusherCreator, Collections.emptyMap());

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyWhenMetricPusherAddressIsEmpty() {
        // given
        PushMetricsProcess.MetricPusherProducer metricPusherProducer = new PushMetricsProcess.MetricPusherProducer();

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreator.create("the.address")).thenReturn(mock(MetricPusher.class));

        // when
        Optional<MetricPusher> result = metricPusherProducer.create(metricPusherCreator, Collections.singletonMap("metricPusherAddress", ""));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceNonEmpty() {
        // given
        PushMetricsProcess.MetricPusherProducer metricPusherProducer = new PushMetricsProcess.MetricPusherProducer();

        MetricPusher metricPusher = mock(MetricPusher.class);
        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreator.create("the.address")).thenReturn(metricPusher);

        // when
        Optional<MetricPusher> result = metricPusherProducer.create(metricPusherCreator, Collections.singletonMap("metricPusherAddress", "the.address"));

        // then
        assertEquals(Optional.of(metricPusher), result);
    }

}
