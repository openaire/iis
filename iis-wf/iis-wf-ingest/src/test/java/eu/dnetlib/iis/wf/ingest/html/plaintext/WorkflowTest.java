package eu.dnetlib.iis.wf.ingest.html.plaintext;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
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
