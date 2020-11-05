package eu.dnetlib.iis.wf.metadataextraction;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 *
 * @author mhorst
 *
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testMainWorkflow() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/metadataextraction/sampledataproducer",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
	}
}
