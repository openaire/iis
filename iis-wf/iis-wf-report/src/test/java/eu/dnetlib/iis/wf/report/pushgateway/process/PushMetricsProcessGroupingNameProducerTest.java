package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(MockitoJUnitRunner.class)
public class PushMetricsProcessGroupingNameProducerTest {

    @Test
    public void shouldProduceEmptyOnError() {
        // given
        PushMetricsProcess.GroupingKeyProducer groupingKeyProducer = new PushMetricsProcess.GroupingKeyProducer();

        // when
        Optional<Map<String, String>> result = groupingKeyProducer.create(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnEmptyGroupingKeyMap() {
        // given
        PushMetricsProcess.GroupingKeyProducer groupingKeyProducer = new PushMetricsProcess.GroupingKeyProducer();

        // when
        Optional<Map<String, String>> result = groupingKeyProducer.create(Collections::emptyMap);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnGroupingKeyMapWithEmptyKey() {
        // given
        PushMetricsProcess.GroupingKeyProducer groupingKeyProducer = new PushMetricsProcess.GroupingKeyProducer();

        // when
        Optional<Map<String, String>> result = groupingKeyProducer.create(() -> Collections.singletonMap("", "value"));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnGroupingKeyMapWithNullValue() {
        // given
        PushMetricsProcess.GroupingKeyProducer groupingKeyProducer = new PushMetricsProcess.GroupingKeyProducer();

        // when
        Optional<Map<String, String>> result = groupingKeyProducer.create(() -> Collections.singletonMap("kay", null));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnGroupingKeyMapWithEmptyValue() {
        // given
        PushMetricsProcess.GroupingKeyProducer groupingKeyProducer = new PushMetricsProcess.GroupingKeyProducer();

        // when
        Optional<Map<String, String>> result = groupingKeyProducer.create(() -> Collections.singletonMap("kay", ""));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceNonEmpty() {
        // given
        PushMetricsProcess.GroupingKeyProducer groupingKeyProducer = new PushMetricsProcess.GroupingKeyProducer();

        // when
        Optional<Map<String, String>> result = groupingKeyProducer.create(() -> Collections.singletonMap("groupingKey", "value"));

        // then
        assertEquals(Optional.of(Collections.singletonMap("groupingKey", "value")), result);
    }

}
