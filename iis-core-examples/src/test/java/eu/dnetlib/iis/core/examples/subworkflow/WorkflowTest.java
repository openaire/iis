package eu.dnetlib.iis.core.examples.subworkflow;

import java.io.IOException;
import java.util.List;

import org.apache.oozie.client.OozieClientException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.RemoteOozieAppManager;
import eu.dnetlib.iis.core.TestsIOUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
 * 
 * @author Mateusz Kobos
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

	@Test 
	public void testBasic() throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/subworkflow/cloners/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner2/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(48),person);		
	}
	
}
