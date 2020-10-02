package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PushMetricsProcessTest {

    @Mock
    private PushMetricsProcess.MetricPusherCreatorProducer metricPusherCreatorProducer;

    @Mock
    private PushMetricsProcess.MetricPusherProducer metricPusherProducer;

    @Mock
    private PushMetricsProcess.FileSystemProducer fileSystemProducer;

    @Mock
    private PushMetricsProcess.ReportLocationsFinder reportLocationsFinder;

    @Mock
    private PushMetricsProcess.LabeledMetricConfByPatternProducer labeledMetricConfByPatternProducer;

    @Mock
    private PushMetricsProcess.ReportEntryReader reportEntryReader;

    @Mock
    private PushMetricsProcess.ReportEntryConverter reportEntryConverter;

    @Mock
    private PushMetricsProcess.GaugesRegistrar gaugesRegistrar;

    @Mock
    private PushMetricsProcess.GroupingKeyProducer groupingKeyProducer;

    @InjectMocks
    private PushMetricsProcess pushMetricsProcess = new PushMetricsProcess();

    @Test
    public void runShouldDoNothingWhenMetricPusherCreatorCreationFails() {
        // given
        PortBindings portBindings = mock(PortBindings.class);
        Configuration conf = mock(Configuration.class);
        Map<String, String> parameters = Collections.singletonMap("reportsDir", "/path/to/report");

        when(metricPusherCreatorProducer.create(anyMap())).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
    }

    @Test
    public void runShouldDoNothingWhenMetricPusherCreationFails() {
        // given
        PortBindings portBindings = mock(PortBindings.class);
        Configuration conf = mock(Configuration.class);
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        when(metricPusherProducer.create(any(MetricPusherCreator.class), anyMap())).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
    }

    @Test
    public void runShouldDoNothingWhenFileSystemCreationFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        when(fileSystemProducer.create(any(Configuration.class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(metricPusher, never()).pushSafe(any(), any(), any());
    }

    @Test
    public void runShouldDoNothingWhenReportLocationsFinderFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(anyMap())).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(metricPusher, never()).pushSafe(any(), any(), any());
    }

    @Test
    public void runShouldDoNothingWhenLabelNamesByMetricNameCreatorFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labeledMetricConfByPatternProducer.create(anyMap())).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labeledMetricConfByPatternProducer, times(1)).create(parameters);
        verify(metricPusher, never()).pushSafe(any(), any(), any());
    }

    @Test
    public void runShouldDoNothingWhenReportReadFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labeledMetricConfByPatternProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        when(reportEntryReader.read(any(FileSystem.class), any(Path.class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labeledMetricConfByPatternProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(metricPusher, never()).pushSafe(any(), any(), any());
    }

    @Test
    public void runShouldDoNothingWhenReportConversionFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labeledMetricConfByPatternProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        when(reportEntryConverter.convert(anyList(), any(String.class), anyMap())).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labeledMetricConfByPatternProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(metricPusher, never()).pushSafe(any(), any(), any());
    }

    @Test
    public void runShouldDoNothingWhenGaugesRegistrationFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labeledMetricConfByPatternProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        List<Gauge> gauges = Collections.singletonList(mock(Gauge.class));
        when(reportEntryConverter.convert(reportEntries, "/path/to/report", Collections.emptyMap())).thenReturn(Optional.of(gauges));
        when(gaugesRegistrar.register(anyList())).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labeledMetricConfByPatternProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(gaugesRegistrar, times(1)).register(gauges);
        verify(metricPusher, never()).pushSafe(any(), any(), any());
    }

    @Test
    public void runShouldDoNothingWhenJobNameCreationFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labeledMetricConfByPatternProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        List<Gauge> gauges = Collections.singletonList(mock(Gauge.class));
        when(reportEntryConverter.convert(reportEntries, "/path/to/report", Collections.emptyMap())).thenReturn(Optional.of(gauges));
        CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
        when(gaugesRegistrar.register(gauges)).thenReturn(Optional.of(collectorRegistry));
        when(groupingKeyProducer.create(anyMap())).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labeledMetricConfByPatternProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(gaugesRegistrar, times(1)).register(gauges);
        verify(groupingKeyProducer, times(1)).create(parameters);
        verify(metricPusher, never()).pushSafe(any(), any(), any());
    }

    @Test
    public void runShouldPushUsingMetricPusher() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("reportsDirPath", "/path/to/report");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labeledMetricConfByPatternProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        List<Gauge> gauges = Collections.singletonList(mock(Gauge.class));
        when(reportEntryConverter.convert(reportEntries, "/path/to/report", Collections.emptyMap())).thenReturn(Optional.of(gauges));
        CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
        when(gaugesRegistrar.register(gauges)).thenReturn(Optional.of(collectorRegistry));
        when(groupingKeyProducer.create(parameters)).thenReturn(Optional.of(Collections.singletonMap("grouping.key", "value")));

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labeledMetricConfByPatternProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(gaugesRegistrar, times(1)).register(gauges);
        verify(groupingKeyProducer, times(1)).create(parameters);
        verify(metricPusher, times(1)).pushSafe(collectorRegistry, "iis", Collections.singletonMap("grouping.key", "value"));
    }

}