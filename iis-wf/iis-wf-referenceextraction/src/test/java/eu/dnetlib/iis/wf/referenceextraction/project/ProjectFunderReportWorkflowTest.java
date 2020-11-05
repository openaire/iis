package eu.dnetlib.iis.wf.referenceextraction.project;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
public class ProjectFunderReportWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testWorkflow() throws Exception {
        OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        wf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/referenceextraction/project/funder_report/test", wf);
    }

}
