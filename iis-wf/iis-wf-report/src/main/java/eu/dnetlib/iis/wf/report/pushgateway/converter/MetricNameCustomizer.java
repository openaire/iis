package eu.dnetlib.iis.wf.report.pushgateway.converter;

//TODO add description and logging
public class MetricNameCustomizer {

    private MetricNameCustomizer() {
    }

    @FunctionalInterface
    public interface Customizer {
        String customize(String str);
    }

    public static String dashRemover(String rawMetricName) {
        return rawMetricName.replace("-", "");
    }

}
