package eu.dnetlib.iis.core.examples.spark;

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
public class SparkPipeClonerWorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void sparkPipeClonerTest() {
		
		testWorkflow("eu/dnetlib/iis/core/examples/spark/pipe_cloner");
		
	}
}
