package eu.dnetlib.iis.wf.top;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 * Main integration tests.
 * 
 * @author mhorst
 * 
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testIntegrationImportWorkflow() throws Exception {
		OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
		wfConf.setTimeoutInSeconds(720);
		testWorkflow("eu/dnetlib/iis/wf/top/integration/import", wfConf);
	}

	@Test
	public void testIntegrationMainChainWorkflow() throws Exception {
		// this is long running test, so we need to increate timeout
		OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
		wfConf.setTimeoutInSeconds(7200);
		testWorkflow("eu/dnetlib/iis/wf/top/integration/primary/processing", wfConf);
	}

}
