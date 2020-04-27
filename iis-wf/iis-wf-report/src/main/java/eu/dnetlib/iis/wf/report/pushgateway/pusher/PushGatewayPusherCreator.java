package eu.dnetlib.iis.wf.report.pushgateway.pusher;

import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

public class PushGatewayPusherCreator implements MetricPusherCreator {
    private static final Logger logger = LoggerFactory.getLogger(PushGatewayPusherCreator.class);

    public MetricPusher create(String address) {
        Supplier<PushGateway> pushGatewaySupplier = () -> {
            try {
                return new PushGateway(address);
            } catch (Exception e) {
                logger.error("PushGateway creation with address '{}' failed.", address);
                e.printStackTrace();
                return null;
            }
        };
        return create(pushGatewaySupplier);
    }

    public MetricPusher create(Supplier<PushGateway> pushGatewaySupplier) {
        return (collectorRegistry, jobName) -> Optional
                .ofNullable(pushGatewaySupplier.get())
                .ifPresent(pushGateway -> {
                    try {
                        pushGateway.pushAdd(collectorRegistry, jobName);
                    } catch (IOException e) {
                        logger.error("PushGateway push failed with exception.");
                        e.printStackTrace();
                    }
                });
    }
}
