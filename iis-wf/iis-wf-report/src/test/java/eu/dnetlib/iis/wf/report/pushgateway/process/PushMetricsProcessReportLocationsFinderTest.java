package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(MockitoExtension.class)
public class PushMetricsProcessReportLocationsFinderTest {

    @Test
    public void shouldFindEmptyOnError() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldFindEmptyOnEmptyReportLocations() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(Collections::emptyList);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldFindEmptyOnListOfEmptyReportLocations() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(() -> Collections.singletonList(""));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldFindEmptyOnListOfNull() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(() -> Collections.singletonList(null));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldFindNonEmpty() throws IOException {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();
        Path reportsDir = Files.createTempDirectory(this.getClass().getSimpleName());
        Path report = Files.createTempDirectory(reportsDir, "report");

        // when
        Optional<List<String>> result = reportLocationsFinder.find(Collections.singletonMap("reportsDirPath", reportsDir.toString()));

        // then
        assertEquals(Optional.of(Collections.singletonList(String.format("file:%s", report.toString()))), result);
    }

}
