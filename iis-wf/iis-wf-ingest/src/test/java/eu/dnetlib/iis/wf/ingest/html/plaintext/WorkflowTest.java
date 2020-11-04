package eu.dnetlib.iis.wf.ingest.html.plaintext;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 *
 */
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
