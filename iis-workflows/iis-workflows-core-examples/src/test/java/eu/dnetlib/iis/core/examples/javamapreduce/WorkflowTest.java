package eu.dnetlib.iis.core.examples.javamapreduce;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.core.TestsIOUtils;
import eu.dnetlib.iis.core.WorkflowTestResult;
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
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testClonerWithExplicitJSONSchema() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("cloner/person");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_explicit_schema", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4),person);
	}
	
	@Test
	public void testCloner() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("cloner/person");
		
		WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4),person);		
	}

	@Test
	public void testReverseRelation() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("mr_reverse_relation/person");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/reverse_relation", conf);
		
		List<PersonWithDocuments> person = 
				workflowTestResult.getAvroDataStore("mr_reverse_relation/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonWithDocumentsWithoutDocumentlessPersons(),
				person);
	}
	
	@Test
	public void testClonerWithoutReducer() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("cloner/person");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_without_reducer", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4),person);
	}

	@Test 
	public void testClonerMultipleOutput() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("cloner/person");
		conf.addOutputAvroDataStoreToInclude("mr_cloner/age");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(12), person);		/*-?|2012-01-07 Cermine integration and MR multiple in-out|mafju|c4|?*/
		
		List<PersonAge> personAge = 
				workflowTestResult.getAvroDataStore("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(6), personAge);
	}
	
	@Test 
	public void testClonerMultipleOutputWithExplicitJSONSchema() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("cloner/person");
		conf.addOutputAvroDataStoreToInclude("mr_cloner/age");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_with_explicit_schema", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(12), person);		/*-?|2012-01-07 Cermine integration and MR multiple in-out|mafju|c4|?*/
		
		List<PersonAge> personAge = 
				workflowTestResult.getAvroDataStore("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(6), personAge);
	}
	
	@Test 
	public void testClonerMultipleOutputWithoutReducerWithExplicitJSONSchema() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("cloner/person");
		conf.addOutputAvroDataStoreToInclude("mr_cloner/age");
		
		WorkflowTestResult workflowTestResult = 
			testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_without_reducer_with_explicit_schema", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4), person);		/*-?|2012-01-07 Cermine integration and MR multiple in-out|mafju|c4|?*/
		
		List<PersonAge> personAge = 
				workflowTestResult.getAvroDataStore("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(2), personAge);
	}
	
	@Test 
	public void testClonerMultipleOutputWithoutReducer() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addOutputAvroDataStoreToInclude("cloner/person");
		conf.addOutputAvroDataStoreToInclude("mr_cloner/age");
		
		WorkflowTestResult workflowTestResult = 
			testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_without_reducer", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(4), person);
		
		List<PersonAge> personAge = 
				workflowTestResult.getAvroDataStore("mr_cloner/age");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAgeRepeated(2), personAge);
	}
	
	@Test 
	public void testMultipleOuputsWithoutReducerWithEmptyInput() {
		testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_without_reducer_with_empty_input");
	}
	
	@Test 
	public void testMultipleOuputsWithEmptyInput() {
		testWorkflow("eu/dnetlib/iis/core/examples/javamapreduce/cloner_with_multiple_output_with_empty_input");
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
