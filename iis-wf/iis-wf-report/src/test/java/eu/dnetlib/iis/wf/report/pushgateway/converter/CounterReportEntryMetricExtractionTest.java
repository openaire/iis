package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CounterReportEntryMetricExtractionTest {

    @Test
    public void shouldExtractMetricWithoutLabels() {
        // given
        ReportEntry reportEntry = new ReportEntry("a.b.c.d.e-f", ReportEntryType.COUNTER, "1");
        Map<String, LabeledMetricConf> labeledMetricConfByPattern = Collections.emptyMap();
        LabelValuesExtraction.Extractor labelValuesExtractor = mock(LabelValuesExtraction.Extractor.class);

        //when
        ExtractedMetric result = CounterReportEntryMetricExtraction
                .extractMetricFromReportEntry(reportEntry, labeledMetricConfByPattern, labelValuesExtractor);

        // then
        assertEquals("a_b_c_d_ef", result.getMetricName());
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
        when(labeledMetricConf.toExtractedMetric(Collections.singletonList("d"), 1d)).thenReturn(mock(ExtractedMetric.class));
        Map<String, LabeledMetricConf> labeledMetricConfByPattern = Collections.singletonMap("a\\.b\\.c\\.\\S+", labeledMetricConf);
        LabelValuesExtraction.Extractor labelValuesExtractor = mock(LabelValuesExtraction.Extractor.class);
        when(labelValuesExtractor.extract("a.b.c.d", labeledMetricConf)).thenReturn(Collections.singletonList("d"));

        //when
        ExtractedMetric result = CounterReportEntryMetricExtraction
                .extractMetricFromReportEntry(reportEntry, labeledMetricConfByPattern, labelValuesExtractor);

        // then
        assertNotNull(result);
    }

}