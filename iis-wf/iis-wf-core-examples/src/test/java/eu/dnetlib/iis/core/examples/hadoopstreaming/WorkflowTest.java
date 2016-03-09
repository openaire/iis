package eu.dnetlib.iis.core.examples.hadoopstreaming;

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
import eu.dnetlib.iis.core.examples.schemas.documenttext.DocumentText;

/**
 * 
 * @author Mateusz Kobos
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

	@Test
	public void testClonerWithoutReducer() throws Exception{
		testWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_without_reducer");
	}

	@Test
	public void testCloner() throws Exception{
		testWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner");
	}
	
	@Test
	public void testClonerWithoutReducerWithExplicitSchemaFile() throws Exception{
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("cloner/person");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_without_reducer_with_explicit_schema_file", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(6),person);
	}

	@Test
	public void testClonerWithoutReducerWithSubworkflow() throws Exception{
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("my_subworkflow/person");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_without_reducer_with_subworkflow", conf);
		
		List<Person> person = 
				workflowTestResult.getAvroDataStore("my_subworkflow/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(6),person);
	}
	
    @Test
	public void testWordCount() throws Exception{
    	testWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/wordcount");
	}

    @Test
	public void testWordCountWithSQLiteDBPlacedInDistribuedCache() throws Exception{
    	testWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/wordcount_with_distributed_cache");
	}

	@Test
	public void testClonerWithUnicodeEscapeCodes() throws Exception{
		OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
		conf.addExpectedOutputAvroDataStore("python_cloner/document_text");
		
		WorkflowTestResult workflowTestResult = 
				testWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_with_unicode_escape_codes", conf);
		
		List<DocumentText> documentText = 
				workflowTestResult.getAvroDataStore("python_cloner/document_text");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getDocumentTextRepeated(3), documentText);
	}
	
}