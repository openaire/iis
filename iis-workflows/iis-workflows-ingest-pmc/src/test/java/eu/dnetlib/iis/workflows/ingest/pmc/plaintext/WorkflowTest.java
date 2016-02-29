package eu.dnetlib.iis.workflows.ingest.pmc.plaintext;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;

/**
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testIngestPmcPlaintext() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/ingest/pmc/plaintext/sampledataproducer",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
	}
	
	@Test
	public void testIngestPmcPlaintextFromBrokenXML() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/ingest/pmc/plaintext/brokenxml",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
	}
	
	@Test
	public void testIngestPmcPlaintextFromNullText() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/ingest/pmc/plaintext/nulltext",
    			new OozieWorkflowTestConfiguration().setTimeoutInSeconds(720));
	}
}
