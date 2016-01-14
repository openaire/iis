package eu.dnetlib.iis.workflows.top;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;

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
		testWorkflow("eu/dnetlib/iis/workflows/top/integration/import", wfConf);
	}

	@Test
	public void testIntegrationMainChainWorkflow() throws Exception {
		// this is long running test, so we need to increate timeout
		OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
		wfConf.setTimeoutInSeconds(7200);
		testWorkflow("eu/dnetlib/iis/workflows/top/integration/primary/processing", wfConf);
	}

}
