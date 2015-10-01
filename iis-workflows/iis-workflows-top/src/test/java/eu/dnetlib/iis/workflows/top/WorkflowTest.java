package eu.dnetlib.iis.workflows.top;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;

/**
 * Main integration tests.
 * 
 * @author mhorst
 * 
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

//	@Test
//	disabled, introducing definition in deploy.info file causing jenkins job creation
	public void testIntegrationImportWorkflow() throws Exception {
		OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
		wfConf.setTimeoutInSeconds(720);
		runWorkflow("eu/dnetlib/iis/workflows/top/integration/import/oozie_app", wfConf);
	}

//	@Test
//	disabled, introducing definition in deploy.info file causing jenkins job creation
	public void testIntegrationMainChainWorkflow() throws Exception {
		// this is long running test, so we need to increate timeout
		OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
		wfConf.setTimeoutInSeconds(7200);
//		oozie.wf.validate.ForkJoin is required after introducing support for decision elements 
		Properties props = new Properties();
		props.put("oozie.wf.validate.ForkJoin", "false");
		wfConf.setJobProps(props);
		runWorkflow("eu/dnetlib/iis/workflows/top/integration/primary/processing/oozie_app", wfConf);
	}

}
