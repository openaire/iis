package eu.dnetlib.iis.wf.report.pushgateway.process;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

public class PushGatewayPusherCreatorTest {

    @Test
    public void metricPusherShouldDoNothingOnNull() {
        PushGatewayPusherCreator pushGatewayPusherCreator = new PushGatewayPusherCreator();

        // when
        pushGatewayPusherCreator.create(() -> null).pushSafe(null, null, null);
    }

    @Test
    public void metricPusherShouldPushSafeUsingPushGateway() throws IOException {
        PushGatewayPusherCreator pushGatewayPusherCreator = new PushGatewayPusherCreator();
        PushGateway pushGateway = mock(PushGateway.class);
        Map<String, String> groupingKey = Collections.emptyMap();

        // when
        pushGatewayPusherCreator.create(() -> pushGateway).pushSafe(mock(CollectorRegistry.class), "job", groupingKey);

        // then
        verify(pushGateway, times(1)).push(any(CollectorRegistry.class), eq("job"), eq(groupingKey));
    }

}