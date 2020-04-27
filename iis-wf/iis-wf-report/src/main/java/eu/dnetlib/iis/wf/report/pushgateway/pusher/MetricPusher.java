package eu.dnetlib.iis.wf.report.pushgateway.pusher;

import io.prometheus.client.CollectorRegistry;

public interface MetricPusher {
    void pushSafe(CollectorRegistry collectorRegistry, String jobName);
}
