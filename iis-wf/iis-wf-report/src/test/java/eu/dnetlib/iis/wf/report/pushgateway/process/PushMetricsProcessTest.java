package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.report.pushgateway.pusher.MetricPusher;
import eu.dnetlib.iis.wf.report.pushgateway.pusher.MetricPusherCreator;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
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
    private PushMetricsProcess.LabelNamesByMetricNameProducer labelNamesByMetricNameProducer;

    @Mock
    private PushMetricsProcess.ReportEntryReader reportEntryReader;

    @Mock
    private PushMetricsProcess.ReportEntryConverter reportEntryConverter;

    @Mock
    private PushMetricsProcess.GaugesRegistrar gaugesRegistrar;

    @Mock
    private PushMetricsProcess.JobNameProducer jobNameProducer;

    @InjectMocks
    private PushMetricsProcess pushMetricsProcess = new PushMetricsProcess();

    @Test
    public void runShouldDoNothingWhenMetricPusherCreatorCreationFails() {
        // given
        PortBindings portBindings = mock(PortBindings.class);
        Configuration conf = mock(Configuration.class);
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        when(metricPusherCreatorProducer.create(anyMapOf(String.class, String.class))).thenReturn(Optional.empty());

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
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        when(metricPusherProducer.create(any(MetricPusherCreator.class), anyMapOf(String.class, String.class))).thenReturn(Optional.empty());

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
        Map<String, String> parameters = Collections.singletonMap("key", "value");

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
        verify(metricPusher, never()).pushSafe(any(), any());
    }

    @Test
    public void runShouldDoNothingWhenReportLocationsFinderFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(anyMapOf(String.class, String.class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(metricPusher, never()).pushSafe(any(), any());
    }

    @Test
    public void runShouldDoNothingWhenLabelNamesByMetricNameCreatorFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labelNamesByMetricNameProducer.create(anyMapOf(String.class, String.class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labelNamesByMetricNameProducer, times(1)).create(parameters);
        verify(metricPusher, never()).pushSafe(any(), any());
    }

    @Test
    public void runShouldDoNothingWhenReportReadFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labelNamesByMetricNameProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        when(reportEntryReader.read(any(FileSystem.class), any(Path.class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labelNamesByMetricNameProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(metricPusher, never()).pushSafe(any(), any());
    }

    @Test
    public void runShouldDoNothingWhenReportConversionFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labelNamesByMetricNameProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        when(reportEntryConverter.convert(anyListOf(ReportEntry.class), any(String.class), anyMapOf(String.class, String[].class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labelNamesByMetricNameProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(metricPusher, never()).pushSafe(any(), any());
    }

    @Test
    public void runShouldDoNothingWhenGaugesRegistrationFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labelNamesByMetricNameProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        List<Gauge> gauges = Collections.singletonList(mock(Gauge.class));
        when(reportEntryConverter.convert(reportEntries, "/path/to/report", Collections.emptyMap())).thenReturn(Optional.of(gauges));
        when(gaugesRegistrar.register(anyListOf(Gauge.class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labelNamesByMetricNameProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(gaugesRegistrar, times(1)).register(gauges);
        verify(metricPusher, never()).pushSafe(any(), any());
    }

    @Test
    public void runShouldDoNothingWhenJobNameCreationFails() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labelNamesByMetricNameProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        List<Gauge> gauges = Collections.singletonList(mock(Gauge.class));
        when(reportEntryConverter.convert(reportEntries, "/path/to/report", Collections.emptyMap())).thenReturn(Optional.of(gauges));
        CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
        when(gaugesRegistrar.register(gauges)).thenReturn(Optional.of(collectorRegistry));
        when(jobNameProducer.create(anyMapOf(String.class, String.class))).thenReturn(Optional.empty());

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labelNamesByMetricNameProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(gaugesRegistrar, times(1)).register(gauges);
        verify(jobNameProducer, times(1)).create(parameters);
        verify(metricPusher, never()).pushSafe(any(), any());
    }

    @Test
    public void runShouldPushUsingMetricPusher() {
        // given
        PortBindings portBindings = new PortBindings(Collections.emptyMap(), Collections.emptyMap());
        Configuration conf = new Configuration();
        Map<String, String> parameters = Collections.singletonMap("key", "value");

        MetricPusherCreator metricPusherCreator = mock(MetricPusherCreator.class);
        when(metricPusherCreatorProducer.create(parameters)).thenReturn(Optional.of(metricPusherCreator));
        MetricPusher metricPusher = mock(MetricPusher.class);
        when(metricPusherProducer.create(metricPusherCreator, parameters)).thenReturn(Optional.of(metricPusher));
        FileSystem fs = mock(FileSystem.class);
        when(fileSystemProducer.create(conf)).thenReturn(Optional.of(fs));
        when(reportLocationsFinder.find(parameters)).thenReturn(Optional.of(Collections.singletonList("/path/to/report")));
        when(labelNamesByMetricNameProducer.create(parameters)).thenReturn(Optional.of(Collections.emptyMap()));
        List<ReportEntry> reportEntries = Collections.singletonList(mock(ReportEntry.class));
        when(reportEntryReader.read(fs, new Path("/path/to/report"))).thenReturn(Optional.of(reportEntries));
        List<Gauge> gauges = Collections.singletonList(mock(Gauge.class));
        when(reportEntryConverter.convert(reportEntries, "/path/to/report", Collections.emptyMap())).thenReturn(Optional.of(gauges));
        CollectorRegistry collectorRegistry = mock(CollectorRegistry.class);
        when(gaugesRegistrar.register(gauges)).thenReturn(Optional.of(collectorRegistry));
        when(jobNameProducer.create(parameters)).thenReturn(Optional.of("the.job.name"));

        // when
        pushMetricsProcess.run(portBindings, conf, parameters);

        // then
        verify(metricPusherCreatorProducer, times(1)).create(parameters);
        verify(metricPusherProducer, times(1)).create(metricPusherCreator, parameters);
        verify(fileSystemProducer, times(1)).create(conf);
        verify(reportLocationsFinder, times(1)).find(parameters);
        verify(labelNamesByMetricNameProducer, times(1)).create(parameters);
        verify(reportEntryReader, times(1)).read(fs, new Path("/path/to/report"));
        verify(reportEntryConverter, times(1)).convert(reportEntries, "/path/to/report", Collections.emptyMap());
        verify(gaugesRegistrar, times(1)).register(gauges);
        verify(jobNameProducer, times(1)).create(parameters);
        verify(metricPusher, times(1)).pushSafe(collectorRegistry, "the.job.name");
    }

}