package eu.dnetlib.iis.workflows.collapsers.multiple_input;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;
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
	public void testDefaultWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wc = new OozieWorkflowTestConfiguration();
        wc.setTimeoutInSeconds(720);
    	runWorkflow("eu/dnetlib/iis/workflows/collapsers/multiple_input_collapser/default/oozie_app", wc);
    }
   
    @Test
	public void testDocumentTextWorkflow() throws Exception {
    	OozieWorkflowTestConfiguration wc = new OozieWorkflowTestConfiguration();
        wc.setTimeoutInSeconds(720);
    	runWorkflow("eu/dnetlib/iis/workflows/collapsers/multiple_input_collapser/documenttext/oozie_app", wc);
    }

}
