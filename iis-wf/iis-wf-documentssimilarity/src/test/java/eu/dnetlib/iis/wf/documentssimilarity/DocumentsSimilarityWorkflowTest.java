package eu.dnetlib.iis.wf.documentssimilarity;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * @author madryk
 */
@IntegrationTest
public class DocumentsSimilarityWorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void documentssimilarity_main_workflow() {
		
		OozieWorkflowTestConfiguration testConfig = new OozieWorkflowTestConfiguration();
		testConfig.setTimeoutInSeconds(3600);
		
		testWorkflow("eu/dnetlib/iis/wf/documentssimilarity/main_workflow", testConfig);
		
	}
}
