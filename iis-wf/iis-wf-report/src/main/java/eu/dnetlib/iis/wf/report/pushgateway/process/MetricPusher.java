package eu.dnetlib.iis.wf.report.pushgateway.process;

import io.prometheus.client.CollectorRegistry;

import java.util.Map;

/**
 * Support for safe push to pushgateway of registered gauges using job name and a map of grouping keys. Any exceptions
 * while pushing will be silenced. For production code should push to a running instance of pushgateway, for testing
 * should save metrics to file for further inspection.
 */
public interface MetricPusher {
    void pushSafe(CollectorRegistry collectorRegistry, String job, Map<String, String> groupingKey);
}
