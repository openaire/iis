package eu.dnetlib.iis.core.examples.java;

import java.io.File;
import java.util.List;

import org.apache.oozie.client.WorkflowJob;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.core.TestsIOUtils;
import eu.dnetlib.iis.core.WorkflowTestResult;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.DocumentWithAuthors;
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
	public void testIfSimpleClonerJustWorks() {
		testWorkflow("eu/dnetlib/iis/core/examples/java/cloner");
	}
	
	@Test
	public void testSimpleLineByLineCopier() throws Exception{
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputFile("copier/doc_copy.csv");
		conf.addExpectedOutputFile("copier/person_copy.csv");
		
		WorkflowTestResult workflowTestResult = testWorkflow(
				"eu/dnetlib/iis/core/examples/java/line_by_line_copier",
				conf);
		
		final File actualDocument = workflowTestResult.getWorkflowOutputFile("copier/doc_copy.csv");
		final File actualPerson = workflowTestResult.getWorkflowOutputFile("copier/person_copy.csv");
		
		TestsIOUtils.assertContentsEqual(
				"eu/dnetlib/iis/core/examples/simple_csv_data/person.csv", 
				actualPerson);
		TestsIOUtils.assertContentsEqual(
				"eu/dnetlib/iis/core/examples/simple_csv_data/document.csv", 
				actualDocument);
	}
	
	@Test 
	public void testPassingParameter() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("cloner/person");
		
		WorkflowTestResult workflowTestResult = 
			testWorkflow("eu/dnetlib/iis/core/examples/java/cloner", conf);
		
		List<Person> person = workflowTestResult.getAvroDataStore("cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(3),	person);
	}
	
	@Test 
	public void testJsonBasedProducerAndConsumer() {
		testWorkflow("eu/dnetlib/iis/core/examples/java/json_based_producer_and_consumer");
	}
	
	@Test 
	public void testJsonBasedProducerAndConsumerFailing() {
		testWorkflow("eu/dnetlib/iis/core/examples/java/json_based_producer_and_consumer-failing",
				new OozieWorkflowTestConfiguration().setExpectedFinishStatus(WorkflowJob.Status.KILLED));
	}
	
	@Test
	public void testJoinTask() {
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("joiner/document_with_authors");
		conf.addExpectedOutputAvroDataStore("joiner/person_with_documents");
		conf.addExpectedOutputAvroDataStore("joiner/person_age");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/java/joiner", conf);
		
		List<DocumentWithAuthors> documentWithAuthors = 
				workflowTestResult.getAvroDataStore("joiner/document_with_authors");
		List<PersonWithDocuments> personWithDocuments = 
				workflowTestResult.getAvroDataStore("joiner/person_with_documents");
		List<PersonAge> personAge =
				workflowTestResult.getAvroDataStore("joiner/person_age");

		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getDocumentWithAuthors(),
				documentWithAuthors);
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonWithDocuments(),
				personWithDocuments);	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonAge(),
				personAge);	
	}
	
	@Test
	public void testIfEmptyDataStoreIsCreatedEvenWhenWorkflowNodeDoesNotExplicitlyCreateOutputDataStore() {
		testWorkflow("eu/dnetlib/iis/core/examples/java/no_output");
	}
	
	@Test
	public void testIfEmptyDataStoreIsCreatedEvenWhenWorkflowNodeCreatesEmptyOutputDirs() {
		testWorkflow("eu/dnetlib/iis/core/examples/java/no_output_empty_dirs");
	}
}
