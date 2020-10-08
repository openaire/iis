package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class PushMetricsProcessReportEntryReaderTest {

    @Test
    public void shouldReadEmptyOnError() {
        // given
        PushMetricsProcess.ReportEntryReader reportEntryReader = new PushMetricsProcess.ReportEntryReader();

        // when
        Optional<List<ReportEntry>> result = reportEntryReader.read(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldReadEmptyOnEmptyReportEntries() {
        // given
        PushMetricsProcess.ReportEntryReader reportEntryReader = new PushMetricsProcess.ReportEntryReader();

        // when
        Optional<List<ReportEntry>> result = reportEntryReader.read(Collections::emptyList);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldReadEmptyOnReportEntriesWithNull() {
        // given
        PushMetricsProcess.ReportEntryReader reportEntryReader = new PushMetricsProcess.ReportEntryReader();

        // when
        Optional<List<ReportEntry>> result = reportEntryReader.read(() -> Collections.singletonList(null));

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
