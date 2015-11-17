package eu.dnetlib.iis.workflows.transformers.spark.citationmatching.direct;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;

/**
 * 
 * @author madryk
 *
 */
@Category(IntegrationTest.class)
public class SparkCitationMatchingDirectTransformerWorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testTransformer() {
		
		testWorkflow("eu/dnetlib/iis/workflows/transformers/spark/citationmatching/direct/test");
	}
}
