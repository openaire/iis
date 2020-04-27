package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.wf.report.pushgateway.process.PushMetricsProcess;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

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
    public void shouldFindEmptyOnEmptyParameters() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(Collections::emptyList);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldFindEmptyOnParametersWithoutReportLocations() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(Collections.singletonMap("not.reportLocation", "/path/to/location"));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldFindEmptyOnParametersWithEmptyReportLocations() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(Collections.singletonMap("reportLocation", ""));

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldFindReportLocations() {
        // given
        PushMetricsProcess.ReportLocationsFinder reportLocationsFinder = new PushMetricsProcess.ReportLocationsFinder();

        // when
        Optional<List<String>> result = reportLocationsFinder.find(Collections.singletonMap("reportLocation", "/path/to/report"));

        // then
        assertEquals(Optional.of(Collections.singletonList("/path/to/report")), result);
    }

}
