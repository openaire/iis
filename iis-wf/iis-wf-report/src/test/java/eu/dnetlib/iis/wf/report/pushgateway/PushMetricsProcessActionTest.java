package eu.dnetlib.iis.wf.report.pushgateway;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class PushMetricsProcessActionTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void shouldPushReports() {
        testWorkflow("eu/dnetlib/iis/wf/report/pushgateway/process");
    }
}
