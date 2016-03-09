package eu.dnetlib.iis.wf.collapsers.union;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testWorkflow2Inputs() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/collapsers/union/input_2");
    }

    @Test
	public void testWorkflow3Inputs() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/collapsers/union/input_3");
    }
    
}
