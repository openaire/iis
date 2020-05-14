package eu.dnetlib.iis.wf.report.pushgateway.process;

import io.prometheus.client.CollectorRegistry;

import java.util.Map;

public interface MetricPusher {
    void pushSafe(CollectorRegistry collectorRegistry, String job, Map<String, String> groupingKey);
}
