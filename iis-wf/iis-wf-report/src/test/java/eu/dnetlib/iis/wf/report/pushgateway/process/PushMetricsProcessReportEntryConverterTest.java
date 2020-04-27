package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.wf.report.pushgateway.process.PushMetricsProcess;
import io.prometheus.client.Gauge;
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
public class PushMetricsProcessReportEntryConverterTest {

    @Test
    public void shouldConvertToEmptyWhenError() {
        // given
        PushMetricsProcess.ReportEntryConverter reportEntryConverter = new PushMetricsProcess.ReportEntryConverter();

        // when
        Optional<List<Gauge>> result = reportEntryConverter.convert(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldConvertToEmptyWhenGaugesAreEmpty() {
        // given
        PushMetricsProcess.ReportEntryConverter reportEntryConverter = new PushMetricsProcess.ReportEntryConverter();

        // when
        Optional<List<Gauge>> result = reportEntryConverter.convert(Collections::emptyList);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldConvertToNonEmpty() {
        // given
        PushMetricsProcess.ReportEntryConverter reportEntryConverter = new PushMetricsProcess.ReportEntryConverter();
        Gauge gauge = mock(Gauge.class);

        // when
        Optional<List<Gauge>> result = reportEntryConverter.convert(() -> Collections.singletonList(gauge));

        // then
        assertEquals(Optional.of(Collections.singletonList(gauge)), result);
    }
}
