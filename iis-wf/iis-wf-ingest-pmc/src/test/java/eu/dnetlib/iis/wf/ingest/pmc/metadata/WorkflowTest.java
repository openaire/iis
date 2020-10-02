package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 *
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testIngestPmcMetadata() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/pmc/metadata/sampledataproducer",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
	}
}
