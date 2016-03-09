package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testIngestPmcMetadata() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/pmc/metadata/sampledataproducer",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
	}
}
