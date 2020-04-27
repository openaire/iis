package eu.dnetlib.iis.wf.report.pushgateway.pusher;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class PushGatewayPusherCreatorTest {

    @Test
    public void metricPusherShouldDoNothingOnNull() {
        PushGatewayPusherCreator pushGatewayPusherCreator = new PushGatewayPusherCreator();

        // when
        pushGatewayPusherCreator.create(() -> null).pushSafe(null, null);
    }

    @Test
    public void metricPusherShouldPushSafeUsingPushGateway() throws IOException {
        PushGatewayPusherCreator pushGatewayPusherCreator = new PushGatewayPusherCreator();
        PushGateway pushGateway = mock(PushGateway.class);

        // when
        pushGatewayPusherCreator.create(() -> pushGateway).pushSafe(mock(CollectorRegistry.class), "jobName");

        // then
        verify(pushGateway, times(1)).pushAdd(any(CollectorRegistry.class), eq("jobName"));
    }

}