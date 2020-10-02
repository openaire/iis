package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@IntegrationTest
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
