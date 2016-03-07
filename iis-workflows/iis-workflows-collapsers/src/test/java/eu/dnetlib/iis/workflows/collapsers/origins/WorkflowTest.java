package eu.dnetlib.iis.workflows.collapsers.origins;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author Dominika Tkaczyk
 * @author Michal Oniszczuk
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDefaultWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/collapsers/origins_collapser/default");
    }

/*
    @Test
    public void testCitationWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/collapsers/collapser/citation");
    }
*/
    
    @Test
    public void testDocumentTextWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/collapsers/origins_collapser/documenttext");
    }

}
