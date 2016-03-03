package eu.dnetlib.iis.workflows.collapsers.multiple_input;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testDefaultWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wc = new OozieWorkflowTestConfiguration();
        wc.setTimeoutInSeconds(720);
    	testWorkflow("eu/dnetlib/iis/workflows/collapsers/multiple_input_collapser/default", wc);
    }
   
    @Test
	public void testDocumentTextWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wc = new OozieWorkflowTestConfiguration();
        wc.setTimeoutInSeconds(720);
    	testWorkflow("eu/dnetlib/iis/workflows/collapsers/multiple_input_collapser/documenttext", wc);
    }

}
