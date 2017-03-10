package eu.dnetlib.iis.wf.ingest.webcrawl.fundings;

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
	public void testIngestWebcrawlFundings() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/webcrawl/fundings/sampledataproducer");
	}
	
	@Test
	public void testIngestWebcrawlFundingsFromBrokenXml() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/webcrawl/fundings/brokenxml");
	}

}
