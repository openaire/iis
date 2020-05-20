package eu.dnetlib.iis.wf.report.pushgateway.process;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(MockitoJUnitRunner.class)
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
