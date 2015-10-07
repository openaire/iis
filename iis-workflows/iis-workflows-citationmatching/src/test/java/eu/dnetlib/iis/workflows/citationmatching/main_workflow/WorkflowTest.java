package eu.dnetlib.iis.workflows.citationmatching.main_workflow;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;

/**
 *
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/citationmatching/main_workflow",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(3600));
	}
}
