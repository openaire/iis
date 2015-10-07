package eu.dnetlib.iis.workflows.collapsers.union;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testWorkflow2Inputs() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/collapsers/union/input_2");
    }

    @Test
	public void testWorkflow3Inputs() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/collapsers/union/input_3");
    }
    
}
