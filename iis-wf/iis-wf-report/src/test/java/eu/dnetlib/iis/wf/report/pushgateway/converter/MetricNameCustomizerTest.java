package eu.dnetlib.iis.wf.report.pushgateway.converter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetricNameCustomizerTest {

    @Test
    public void shouldRemoveDashes() {
        // when
        String result = MetricNameCustomizer.dashRemover("a-b");

        // then
        assertEquals("ab", result);
    }
}