package eu.dnetlib.iis.wf.primary.processing;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Main integration tests.
 * 
 * @author mhorst
 * 
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testIntegrationMainChainWorkflow() throws Exception {
		// this is long running test, so we need to increase timeout
		OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
		wfConf.setTimeoutInSeconds(7200);
		testWorkflow("eu/dnetlib/iis/wf/primary/processing/sampledataproducer", wfConf);
	}

}
