package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.report.pushgateway.converter.ReportEntryToMetricConverter;
import io.prometheus.client.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FilePusherCreator implements MetricPusherCreator {
    private static final Logger logger = LoggerFactory.getLogger(FilePusherCreator.class);

    @Override
    public MetricPusher create(String address) {
        return (collectorRegistry, job, groupingKey) -> {
            List<ReportEntry> reportEntries = Collections.list(collectorRegistry.metricFamilySamples()).stream()
                    .flatMap(metricFamilySamples -> metricFamilySamples.samples.stream().map(FilePusherCreator::sampleToReportEntry))
                    .collect(Collectors.toList());
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                DataStore.create(reportEntries, new FileSystemPath(fs, new Path(address, job)), ReportEntry.SCHEMA$);
            } catch (IOException e) {
                logger.error("File pusher push failed.", e);
            }
        };
    }

    private static ReportEntry sampleToReportEntry(Collector.MetricFamilySamples.Sample sample) {
        if (isSampleDuration(sample)) {
            return ReportEntryFactory.createDurationReportEntry(keyValueForDuration(sample), (long) (1000 * sample.value));
        }
        return ReportEntryFactory.createCounterReportEntry(keyValueForCounter(sample), (long) sample.value);
    }

    private static boolean isSampleDuration(Collector.MetricFamilySamples.Sample sample) {
        return sample.name.contains(ReportEntryToMetricConverter.DURATION_METRIC_NAME_SUFFIX);
    }

    private static String keyValueForDuration(Collector.MetricFamilySamples.Sample sample) {
        String metricName = sample.name;
        String[] metricNameParts = metricName.split(ReportEntryToMetricConverter.METRIC_NAME_SEP);
        return Arrays.stream(metricNameParts).limit(metricNameParts.length - 1).collect(Collectors.joining(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP));
    }

    private static String keyValueForCounter(Collector.MetricFamilySamples.Sample sample) {
        String metricName = sample.name;
        String[] metricNameParts = metricName.split(ReportEntryToMetricConverter.METRIC_NAME_SEP);
        if (Objects.nonNull(sample.labelValues) && sample.labelValues.size() > 0) {
            return String.format("%s%s%s",
                    String.join(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP, metricNameParts),
                    ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP,
                    String.join(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP, sample.labelValues));
        }
        return String.join(ReportEntryToMetricConverter.REPORT_ENTRY_KEY_SEP, metricNameParts);
    }


}
