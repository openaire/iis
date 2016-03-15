package eu.dnetlib.iis.wf.citationmatching.main_workflow;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 *
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class CitationMatchingWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/citationmatching/main_workflow",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(3600));
	}
}
