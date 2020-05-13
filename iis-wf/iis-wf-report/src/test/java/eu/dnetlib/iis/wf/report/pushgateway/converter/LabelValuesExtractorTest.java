package eu.dnetlib.iis.wf.report.pushgateway.converter;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LabelValuesExtractorTest {

    @Test
    public void shouldExtractLabelValuesUsingPositionalPattern() {
        // given
        String key = "a.b.c.d";
        LabeledMetricConf labeledMetricConf = new LabeledMetricConf("a",
                Arrays.asList(new LabelConf("label_name", "$1"), new LabelConf("label_name", "$2_$3")));

        // when
        List<String> labelValues = LabelValuesExtractor.extractLabelValuesByPosition(key, labeledMetricConf);

        // then
        assertEquals(Arrays.asList("b", "c_d"), labelValues);
    }

}