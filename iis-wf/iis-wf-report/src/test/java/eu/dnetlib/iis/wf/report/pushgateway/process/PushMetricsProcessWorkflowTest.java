package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

public class PushMetricsProcessWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void shouldPushReports() {
        testWorkflow("eu/dnetlib/iis/wf/report/pushgateway/process/test");
    }
}
