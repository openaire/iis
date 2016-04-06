package eu.dnetlib.iis.wf.importer.mapred;

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
	public void testImportWorkflow() throws Exception {
		OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
		wfConf.setTimeoutInSeconds(720);
		testWorkflow("eu/dnetlib/iis/wf/importer/mapred_import/sampledataproducer", wfConf);
	}

}
