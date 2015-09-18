package eu.dnetlib.iis.core.examples.javamapreduce;

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
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.PersonAge;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.personwithdocuments.PersonWithDocuments;

/**
 * 
 * @author Mateusz Kobos
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

	@Test
	public void testClonerWithExplicitJSONSchema()
			throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_explicit_schema/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4),person);		
	}
	
	@Test
	public void testCloner() 
			throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4),person);		
	}

	@Test
	public void testReverseRelation() 
			throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/reverse_relation/oozie_app");
		
		List<PersonWithDocuments> person = 
			appManager.readDataStoreFromWorkingDir("mr_reverse_relation/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonWithDocumentsWithoutDocumentlessPersons(),
				person);
	}
	
	@Test
	public void testClonerWithoutReducer() 
			throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_without_reducer/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4),person);
	}

	@Test 
	public void testClonerMultipleOutput() 
			throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(12), person);		/*-?|2012-01-07 Cermine integration and MR multiple in-out|mafju|c4|?*/
		
		List<PersonAge> personAge = 
				appManager.readDataStoreFromWorkingDir("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(6), personAge);
	}
	
	@Test 
	public void testClonerMultipleOutputWithExplicitJSONSchema() 
			throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_with_explicit_schema/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(12), person);		/*-?|2012-01-07 Cermine integration and MR multiple in-out|mafju|c4|?*/
		
		List<PersonAge> personAge = 
				appManager.readDataStoreFromWorkingDir("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(6), personAge);
	}
	
	@Test 
	public void testClonerMultipleOutputWithoutReducerWithExplicitJSONSchema() 
			throws IOException, OozieClientException{
		
		RemoteOozieAppManager appManager =
			runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_without_reducer_with_explicit_schema/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4), person);		/*-?|2012-01-07 Cermine integration and MR multiple in-out|mafju|c4|?*/
		
		List<PersonAge> personAge = 
				appManager.readDataStoreFromWorkingDir("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(2), personAge);
	}
	
	@Test 
	public void testClonerMultipleOutputWithoutReducer() throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
			runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_without_reducer/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4), person);
		
		List<PersonAge> personAge = 
				appManager.readDataStoreFromWorkingDir("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(2), personAge);
	}
	
	@Test 
	public void testMultipleOuputsWithoutReducerWithEmptyInput() throws IOException, OozieClientException {
		runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_without_reducer_with_empty_input/oozie_app");
	}
	
	@Test 
	public void testMultipleOuputsWithEmptyInput() throws IOException, OozieClientException {
		runWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_with_empty_input/oozie_app");
	}

	
//	@Test 
//	public void testMultipleSameTypeInputAndOutput() /*-?|2012-01-07 Cermine integration and MR multiple in-out|mafju|c7|?*/
//			throws IOException, OozieClientException{
//		RemoteOozieAppManager appManager = 
//			new RemoteOozieAppManager(getFileSystem(), getFsTestCaseDir(),
//				"eu/dnetlib/iis/core/examples/javamapreduce/person_by_age_splitter/oozie_app");
//		
//		Properties jobProps = new Properties();
//		jobProps.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(
//				"eu/dnetlib/iis/core/examples/javamapreduce/person_by_age_splitter/job.properties"));
//		runWorkflow(appManager.getOozieAppPath(), appManager.getWorkingDir(), jobProps);
//		
//		List<Person> person = 
//			appManager.readDataStoreFromWorkingDir(
//					Person.class, "cloner/person");
//	
//		assertEqualSets(StandardDataStoreExamples.getPersonRepeated(4),person);
//		
//	}
//	
	
}
