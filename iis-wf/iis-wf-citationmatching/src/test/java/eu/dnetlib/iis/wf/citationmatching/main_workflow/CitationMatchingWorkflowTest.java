package eu.dnetlib.iis.wf.citationmatching.main_workflow;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 *
 * @author mhorst
 *
 */
@IntegrationTest
public class CitationMatchingWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/citationmatching/main_workflow",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(3600));
	}
}
