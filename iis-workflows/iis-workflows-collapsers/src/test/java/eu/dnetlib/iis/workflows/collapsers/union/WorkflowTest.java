package eu.dnetlib.iis.workflows.collapsers.union;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testWorkflow2Inputs() throws Exception {
    	runWorkflow("eu/dnetlib/iis/workflows/collapsers/union/input_2/oozie_app");
    }

    @Test
	public void testWorkflow3Inputs() throws Exception {
    	runWorkflow("eu/dnetlib/iis/workflows/collapsers/union/input_3/oozie_app");
    }
    
}
