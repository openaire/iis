package eu.dnetlib.iis.workflows.sentimentanalysis;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
	public void testSentimentAnalysis() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/sentimentanalysis/sampledataproducer");
    }

}
