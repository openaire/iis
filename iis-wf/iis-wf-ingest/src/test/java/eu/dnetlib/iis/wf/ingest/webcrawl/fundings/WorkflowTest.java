package eu.dnetlib.iis.wf.ingest.webcrawl.fundings;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 *
 */
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
