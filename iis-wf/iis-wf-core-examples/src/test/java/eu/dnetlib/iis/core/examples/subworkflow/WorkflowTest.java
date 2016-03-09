package eu.dnetlib.iis.core.examples.subworkflow;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.WorkflowTestResult;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
 * 
 * @author Mateusz Kobos
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test 
	public void testBasic() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("cloner2/person");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/subworkflow/cloners", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner2/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(48),person);
	}
	
}
