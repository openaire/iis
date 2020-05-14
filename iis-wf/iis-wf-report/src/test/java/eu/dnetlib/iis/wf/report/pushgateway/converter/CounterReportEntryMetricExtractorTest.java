package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class CounterReportEntryMetricExtractorTest {

    @Test
    public void shouldExtractMetricWithoutLabels() {
        // given
        ReportEntry reportEntry = new ReportEntry("a.b.c.d", ReportEntryType.COUNTER, "1");
        Map<String, LabeledMetricConf> labeledMetricConfByPattern = Collections.emptyMap();
        MetricNameCustomizer.Customizer metricCustomizer = mock(MetricNameCustomizer.Customizer.class);
        when(metricCustomizer.customize("a_b_c_d")).thenReturn("a_b_c_d");
        LabelValuesExtractor.Extractor labelValuesExtractor = mock(LabelValuesExtractor.Extractor.class);

        //when
        ExtractedMetric result = CounterReportEntryMetricExtractor
                .extractMetricFromReportEntry(reportEntry, labeledMetricConfByPattern, metricCustomizer, labelValuesExtractor);

        // then
        assertEquals("a_b_c_d", result.getMetricName());
        assertTrue(result.getLabelValues().isEmpty());
        assertEquals(1.0, result.getValue(), 1e-3);
        verify(labelValuesExtractor, never()).extract(anyString(), any(LabeledMetricConf.class));
    }

    @Test
    public void shouldExtractMetricWithLabels() {
        // given
        ReportEntry reportEntry = new ReportEntry("a.b.c.d", ReportEntryType.COUNTER, "1");
        LabeledMetricConf labeledMetricConf = mock(LabeledMetricConf.class);
        when(labeledMetricConf.getMetricName()).thenReturn("a_b_c");
        Map<String, LabeledMetricConf> labeledMetricConfByPattern = Collections.singletonMap("a\\.b\\.c\\.\\S+", labeledMetricConf);
        MetricNameCustomizer.Customizer metricCustomizer = mock(MetricNameCustomizer.Customizer.class);
        when(metricCustomizer.customize("a_b_c")).thenReturn("a_b_c");
        LabelValuesExtractor.Extractor labelValuesExtractor = mock(LabelValuesExtractor.Extractor.class);

        //when
        ExtractedMetric result = CounterReportEntryMetricExtractor
                .extractMetricFromReportEntry(reportEntry, labeledMetricConfByPattern, metricCustomizer, labelValuesExtractor);

        // then
        assertEquals("a_b_c", result.getMetricName());
        verify(labelValuesExtractor, times(1)).extract("a.b.c.d", labeledMetricConf);
        assertEquals(1.0, result.getValue(), 1e-3);
    }

}