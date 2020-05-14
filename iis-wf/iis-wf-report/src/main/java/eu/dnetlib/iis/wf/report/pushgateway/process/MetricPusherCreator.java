package eu.dnetlib.iis.wf.report.pushgateway.process;

public interface MetricPusherCreator {
    MetricPusher create(String address);
}
