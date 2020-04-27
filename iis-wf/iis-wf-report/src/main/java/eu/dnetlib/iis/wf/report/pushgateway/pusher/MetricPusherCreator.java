package eu.dnetlib.iis.wf.report.pushgateway.pusher;

public interface MetricPusherCreator {
    MetricPusher create(String address);
}
