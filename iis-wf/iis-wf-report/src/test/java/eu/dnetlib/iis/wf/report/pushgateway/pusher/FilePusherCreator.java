package eu.dnetlib.iis.wf.report.pushgateway.pusher;

import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import io.prometheus.client.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FilePusherCreator implements MetricPusherCreator {
    private static final Logger logger = LoggerFactory.getLogger(FilePusherCreator.class);

    @Override
    public MetricPusher create(String address) {
        return (collectorRegistry, jobName) -> {
            List<ReportEntry> reportEntries = Collections.list(collectorRegistry.metricFamilySamples()).stream()
                    .flatMap(metricFamilySamples -> metricFamilySamples.samples.stream().map(FilePusherCreator::sampleToReportEntry))
                    .collect(Collectors.toList());

            try {
                FileSystem fs = FileSystem.get(new Configuration());
                DataStore.create(reportEntries, new FileSystemPath(fs, new Path(address, jobName)), ReportEntry.SCHEMA$);
            } catch (IOException e) {
                logger.error("File pusher push failed.");
                e.printStackTrace();
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
        return sample.name.contains("seconds");
    }

    private static String keyValueForDuration(Collector.MetricFamilySamples.Sample sample) {
        String rawMetricName = sample.name;
        return rawMetricName.substring(0, rawMetricName.indexOf("_seconds")).replace("_", ".");
    }

    private static String keyValueForCounter(Collector.MetricFamilySamples.Sample sample) {
        String rawMetricName = sample.name;
        if (Objects.nonNull(sample.labelValues) && sample.labelValues.size() > 0) {
            String labelValues = String.join("_", sample.labelValues);
            return String.format("%s_%s", rawMetricName, labelValues).replace("_", ".");
        }
        return rawMetricName.replace("_", ".");
    }


}
