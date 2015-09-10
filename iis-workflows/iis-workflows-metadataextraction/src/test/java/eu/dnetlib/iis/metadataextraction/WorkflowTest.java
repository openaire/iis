package eu.dnetlib.iis.metadataextraction;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.WorkflowConfiguration;

/**
 *
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	runWorkflow("eu/dnetlib/iis/metadataextraction/sampledataproducer/oozie_app",
    			new WorkflowConfiguration().setTimeoutInSeconds(720));
	}
}
