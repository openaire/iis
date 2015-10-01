package eu.dnetlib.iis.workflows.citationmatching.main_workflow;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;

/**
 *
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	runWorkflow("eu/dnetlib/iis/workflows/citationmatching/main_workflow/oozie_app",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(3600));
	}
}
