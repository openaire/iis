package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

@IntegrationTest
public class PushMetricsProcessWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void shouldPushReports() {
        testWorkflow("eu/dnetlib/iis/wf/report/pushgateway/process/test");
    }
}
