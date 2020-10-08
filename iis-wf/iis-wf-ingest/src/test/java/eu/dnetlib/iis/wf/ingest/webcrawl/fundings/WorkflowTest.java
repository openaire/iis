package eu.dnetlib.iis.wf.ingest.webcrawl.fundings;

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
	public void testIngestWebcrawlFundings() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/webcrawl/fundings/sampledataproducer");
	}
	
	@Test
	public void testIngestWebcrawlFundingsFromBrokenXml() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/ingest/webcrawl/fundings/brokenxml");
	}

}
