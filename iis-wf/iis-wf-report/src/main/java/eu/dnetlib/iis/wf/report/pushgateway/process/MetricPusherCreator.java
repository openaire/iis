package eu.dnetlib.iis.wf.report.pushgateway.process;

/**
 * Creator of safe metric pushers. For production code should create a metric pusher pushing metrics to a running instance
 * of pushgateway. For testing should create a metric pusher saving metrics to a file.
 */
public interface MetricPusherCreator {
    MetricPusher create(String address);
}
