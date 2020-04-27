package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(MockitoJUnitRunner.class)
public class PushMetricsProcessJobNameProducerTest {

    @Test
    public void shouldProduceEmptyOnError() {
        // given
        PushMetricsProcess.JobNameProducer jobNameProducer = new PushMetricsProcess.JobNameProducer();

        // when
        Optional<String> result = jobNameProducer.create(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnEmptyJobName() {
        // given
        PushMetricsProcess.JobNameProducer jobNameProducer = new PushMetricsProcess.JobNameProducer();

        // when
        Optional<String> result = jobNameProducer.create(() -> "");

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceNonEmpty() {
        // given
        PushMetricsProcess.JobNameProducer jobNameProducer = new PushMetricsProcess.JobNameProducer();

        // when
        Optional<String> result = jobNameProducer.create(Collections.singletonMap("jobName", "the.job.name"));

        // then
        assertEquals(Optional.of("the.job.name"), result);
    }

}
