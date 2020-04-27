package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.report.pushgateway.process.PushMetricsProcess;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class PushMetricsProcessReportEntryReaderTest {

    @Test
    public void shouldReadEmptyWhenError() {
        // given
        PushMetricsProcess.ReportEntryReader reportEntryReader = new PushMetricsProcess.ReportEntryReader();

        // when
        Optional<List<ReportEntry>> result = reportEntryReader.read(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldReadEmptyWhenReportEntriesAreEmpty() {
        // given
        PushMetricsProcess.ReportEntryReader reportEntryReader = new PushMetricsProcess.ReportEntryReader();

        // when
        Optional<List<ReportEntry>> result = reportEntryReader.read(Collections::emptyList);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldReadNonEmpty() {
        // given
        PushMetricsProcess.ReportEntryReader reportEntryReader = new PushMetricsProcess.ReportEntryReader();
        ReportEntry reportEntry = mock(ReportEntry.class);

        // when
        Optional<List<ReportEntry>> result = reportEntryReader.read(() -> Collections.singletonList(reportEntry));

        // then
        assertEquals(Optional.of(Collections.singletonList(reportEntry)), result);
    }

}
