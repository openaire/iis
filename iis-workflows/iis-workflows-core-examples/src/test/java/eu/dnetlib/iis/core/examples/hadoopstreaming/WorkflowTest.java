package eu.dnetlib.iis.core.examples.hadoopstreaming;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import eu.dnetlib.iis.core.RemoteOozieAppManager;
import eu.dnetlib.iis.core.TestsIOUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import eu.dnetlib.iis.core.examples.schemas.documenttext.DocumentText;

/**
 * 
 * @author Mateusz Kobos
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

	@Test
	public void testClonerWithoutReducer() throws Exception{
		runWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_without_reducer/oozie_app");
	}

	@Test
	public void testCloner() throws Exception{
		runWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner/oozie_app");
	}
	
	@Test
	public void testClonerWithoutReducerWithExplicitSchemaFile() throws Exception{
		RemoteOozieAppManager appManager = 
			runWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_without_reducer_with_explicit_schema_file/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("cloner/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(6),person);
	}

	@Test
	public void testClonerWithoutReducerWithSubworkflow() throws Exception{
		RemoteOozieAppManager appManager = 
			runWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_without_reducer_with_subworkflow/oozie_app");
		
		List<Person> person = 
			appManager.readDataStoreFromWorkingDir("my_subworkflow/person");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getPersonRepeated(6),person);
	}
	
    @Test
	public void testWordCount() throws Exception{
		runWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/wordcount/oozie_app");
	}

    @Test
	public void testWordCountWithSQLiteDBPlacedInDistribuedCache() throws Exception{
		runWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/wordcount_with_distributed_cache/oozie_app");
	}

	@Test
	public void testClonerWithUnicodeEscapeCodes() throws Exception{
		RemoteOozieAppManager appManager = 
			runWorkflow("eu/dnetlib/iis/core/examples/hadoopstreaming/cloner_with_unicode_escape_codes/oozie_app");
		
		List<DocumentText> documentText = 
			appManager.readDataStoreFromWorkingDir("python_cloner/document_text");
	
		TestsIOUtils.assertEqualSets(
				StandardDataStoreExamples.getDocumentTextRepeated(3), documentText);
	}
	
}