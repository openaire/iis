package eu.dnetlib.iis.wf.report.pushgateway.converter;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DurationReportEntryMetricExtractionTest {

    @Test
    public void shouldExtractMetricFromReportKey() {
        // given
        ReportEntry reportEntry = new ReportEntry("a.b.c.d.e-f", ReportEntryType.COUNTER, "1000");

        //when
        ExtractedMetric result = DurationReportEntryMetricExtraction.extractMetricFromReportEntry(reportEntry);

        // then
        assertEquals("a_b_c_d_ef_seconds", result.getMetricName());
        assertTrue(result.getLabelValues().isEmpty());
        assertEquals(1.0, result.getValue(), 1e-3);
    }

}