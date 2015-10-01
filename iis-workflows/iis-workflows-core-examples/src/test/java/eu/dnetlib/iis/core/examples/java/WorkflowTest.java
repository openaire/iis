package eu.dnetlib.iis.core.examples.java;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.core.RemoteOozieAppManager;
import eu.dnetlib.iis.core.TestsIOUtils;
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
public class WorkflowTest extends AbstractWorkflowTestCase {

	@Test 
	public void testIfSimpleClonerJustWorks() throws IOException, OozieClientException{
		runWorkflow("eu/dnetlib/iis/core/examples/java/cloner/oozie_app");
	}
	
	@Test
	public void testSimpleLineByLineCopier() throws Exception{
//		notice: we have to skip reading job.properties file which by default 
//		is set explicitly to run only on Hadoop installed on localhost.
		RemoteOozieAppManager appManager = runWorkflow(
				"eu/dnetlib/iis/core/examples/java/line_by_line_copier/oozie_app", 
				new OozieWorkflowTestConfiguration(), true);
		
		File localDir = new File(getTestCaseDir());
		final File actualDocument = new File(localDir, "doc_copy.csv");
		final File actualPerson = new File(localDir, "person_copy.csv");
		@SuppressWarnings("serial")
		Map<String, File> hdfsFileNameToLocalFileNameMap = 
				new HashMap<String, File>(){{
					put("copier/doc_copy.csv", actualDocument);
					put("copier/person_copy.csv", actualPerson);
				}};			
		appManager.copyFilesFromWorkingDir(hdfsFileNameToLocalFileNameMap);
		
		TestsIOUtils.assertContentsEqual(
				"eu/dnetlib/iis/core/examples/simple_csv_data/person.csv", 
				actualPerson);
		TestsIOUtils.assertContentsEqual(
				"eu/dnetlib/iis/core/examples/simple_csv_data/document.csv", 
				actualDocument);
	}
	
	@Test 
	public void testPassingParameter() 
			throws IOException, OozieClientException{
		RemoteOozieAppManager appManager = 
			runWorkflow("eu/dnetlib/iis/core/examples/java/cloner/oozie_app");
		
		List<Person> person = appManager.readDataStoreFromWorkingDir(
				"cloner/person");
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(3),	person);		
	}
	
	@Test 
	public void testJsonBasedProducerAndConsumer() 
			throws IOException, OozieClientException{
		runWorkflow("eu/dnetlib/iis/core/examples/java/json_based_producer_and_consumer/oozie_app");
	}
	
	@Test 
	public void testJsonBasedProducerAndConsumerFailing() 
			throws IOException, OozieClientException{
		runWorkflow("eu/dnetlib/iis/core/examples/java/json_based_producer_and_consumer-failing/oozie_app",
				new OozieWorkflowTestConfiguration().setExpectedFinishStatus(WorkflowJob.Status.KILLED));	
	}
	
	@Test
	public void testJoinTask() throws Exception{
		RemoteOozieAppManager appManager = 
				runWorkflow("eu/dnetlib/iis/core/examples/java/joiner/oozie_app");
		
		List<DocumentWithAuthors> documentWithAuthors = 
			appManager.readDataStoreFromWorkingDir(
					"joiner/document_with_authors");
		List<PersonWithDocuments> personWithDocuments = 
			appManager.readDataStoreFromWorkingDir(
					"joiner/person_with_documents");
		List<PersonAge> personAge =
			appManager.readDataStoreFromWorkingDir(
					"joiner/person_age");

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
	public void testIfEmptyDataStoreIsCreatedEvenWhenWorkflowNodeDoesNotExplicitlyCreateOutputDataStore() throws IOException, OozieClientException{
		runWorkflow("eu/dnetlib/iis/core/examples/java/no_output/oozie_app");	
	}
	
	@Test
	public void testIfEmptyDataStoreIsCreatedEvenWhenWorkflowNodeCreatesEmptyOutputDirs() throws IOException, OozieClientException{
		runWorkflow("eu/dnetlib/iis/core/examples/java/no_output_empty_dirs/oozie_app");	
	}
}
