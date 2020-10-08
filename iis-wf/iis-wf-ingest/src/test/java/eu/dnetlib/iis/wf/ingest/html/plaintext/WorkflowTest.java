package eu.dnetlib.iis.wf.ingest.html.plaintext;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 *
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testIngestHtmlPlaintext() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/html/plaintext/sampledataproducer");
	}
	
	@Test
	public void testIngestHtmlPlaintextFromBrokenHTML() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/html/plaintext/brokenxml");
	}
}
