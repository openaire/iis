package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DurationReportEntryMetricExtractorTest {

    @Test
    public void shouldExtractMetricFromReportKey() {
        // given
        ReportEntry reportEntry = new ReportEntry("a.b.c.d", ReportEntryType.COUNTER, "1000");
        MetricNameCustomizer.Customizer metricCustomizer = mock(MetricNameCustomizer.Customizer.class);
        when(metricCustomizer.customize("a_b_c_d_seconds")).thenReturn("a_b_c_d_seconds");

        //when
        ExtractedMetric result = DurationReportEntryMetricExtractor.extractMetricFromReportEntry(reportEntry, metricCustomizer);

        // then
        assertEquals("a_b_c_d_seconds", result.getMetricName());
        assertTrue(result.getLabelValues().isEmpty());
        assertEquals(1.0, result.getValue(), 1e-3);
    }

}