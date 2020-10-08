package eu.dnetlib.iis.wf.report.pushgateway.process;

import io.prometheus.client.Gauge;
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
public class PushMetricsProcessReportEntryConverterTest {

    @Test
    public void shouldConvertToEmptyOnError() {
        // given
        PushMetricsProcess.ReportEntryConverter reportEntryConverter = new PushMetricsProcess.ReportEntryConverter();

        // when
        Optional<List<Gauge>> result = reportEntryConverter.convert(() -> null);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldConvertToEmptyOnEmptyGauges() {
        // given
        PushMetricsProcess.ReportEntryConverter reportEntryConverter = new PushMetricsProcess.ReportEntryConverter();

        // when
        Optional<List<Gauge>> result = reportEntryConverter.convert(Collections::emptyList);

        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldConvertToEmptyOnGaugesWithNull() {
        // given
        PushMetricsProcess.ReportEntryConverter reportEntryConverter = new PushMetricsProcess.ReportEntryConverter();

        // when
        Optional<List<Gauge>> result = reportEntryConverter.convert(() -> Collections.singletonList(null));

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
