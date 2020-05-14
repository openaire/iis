package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.wf.report.pushgateway.converter.LabelConf;
import eu.dnetlib.iis.wf.report.pushgateway.converter.LabeledMetricConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class PushMetricsProcessLabeledMetricConfByPatternProducerTest {

    @Test
    public void shouldProduceEmptyOnError() {
        // given
        PushMetricsProcess.LabeledMetricConfByPatternProducer labeledMetricConfByPatternProducer = new PushMetricsProcess.LabeledMetricConfByPatternProducer();

        // when
        Optional<Map<String, LabeledMetricConf>> result = labeledMetricConfByPatternProducer.create(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnEmptyMap() {
        // given
        PushMetricsProcess.LabeledMetricConfByPatternProducer labeledMetricConfByPatternProducer = new PushMetricsProcess.LabeledMetricConfByPatternProducer();

        // when
        Optional<Map<String, LabeledMetricConf>> result = labeledMetricConfByPatternProducer.create(Collections::emptyMap);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnMapOfEmptyKey() {
        // given
        PushMetricsProcess.LabeledMetricConfByPatternProducer labeledMetricConfByPatternProducer = new PushMetricsProcess.LabeledMetricConfByPatternProducer();

        // when
        Optional<Map<String, LabeledMetricConf>> result = labeledMetricConfByPatternProducer.create(() -> Collections.singletonMap("", mock(LabeledMetricConf.class)));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceEmptyOnMapOfNullValue() {
        // given
        PushMetricsProcess.LabeledMetricConfByPatternProducer labeledMetricConfByPatternProducer = new PushMetricsProcess.LabeledMetricConfByPatternProducer();

        // when
        Optional<Map<String, LabeledMetricConf>> result = labeledMetricConfByPatternProducer.create(() -> Collections.singletonMap("pattern", null));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldProduceNonEmpty() {
        // given
        PushMetricsProcess.LabeledMetricConfByPatternProducer labeledMetricConfByPatternProducer = new PushMetricsProcess.LabeledMetricConfByPatternProducer();
        String labeledMetricsPropertiesPath = Objects.requireNonNull(this.getClass().getClassLoader()
                .getResource("eu/dnetlib/iis/wf/report/pushgateway/process/test/oozie_app/labeled_metrics_by_case.properties")).getFile();

        // when
        Optional<Map<String, LabeledMetricConf>> result = labeledMetricConfByPatternProducer
                .create(Collections.singletonMap("labeledMetricsPropertiesFile", String.format("file:%s", labeledMetricsPropertiesPath)));

        // then
        assertTrue(result.isPresent());
        assertEquals(Stream.of(
                new AbstractMap.SimpleImmutableEntry<>("a0\\.b0\\.c0\\.d0\\.\\S+",
                        new LabeledMetricConf("a0_b0_c0_d0", Collections.singletonList(new LabelConf("label_name_1", "$4")))),
                new AbstractMap.SimpleImmutableEntry<>("a1\\.b1\\.c1\\.\\S+\\.\\S+",
                        new LabeledMetricConf("a1_b1_c1", Arrays.asList(
                                new LabelConf("label_name_1", "$3"), new LabelConf("label_name_2", "$4")))),
                new AbstractMap.SimpleImmutableEntry<>("a2\\.b2\\.\\S+\\.\\S+\\.\\S+",
                        new LabeledMetricConf("a2_b2", Arrays.asList(
                                new LabelConf("label_name_1", "$2"), new LabelConf("label_name_2", "$3_$4")))),
                new AbstractMap.SimpleImmutableEntry<>("a3\\.b3\\.\\S+\\.\\S+\\.\\S+",
                        new LabeledMetricConf("a3_b3", Arrays.asList(
                                new LabelConf("label_name_1", "$2"), new LabelConf("label_name_2", "$4")))),
                new AbstractMap.SimpleImmutableEntry<>("a4\\.b4\\.c4\\.\\S+\\.\\S+",
                        new LabeledMetricConf("a4_b4_c4", Collections.singletonList(new LabelConf("label_name_1", "$4"))))
                ).collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue)),
                result.get());
    }

}
