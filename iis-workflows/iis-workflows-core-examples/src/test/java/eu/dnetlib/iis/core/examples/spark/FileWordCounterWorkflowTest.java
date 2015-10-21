package eu.dnetlib.iis.core.examples.spark;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.core.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.core.WorkflowTestResult;

/**
 * @author Łukasz Dumiszewski
 */
@Category(IntegrationTest.class)
public class FileWordCounterWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void fileWordCounter() throws Exception{
        
        
        // given
        
        OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
        
        conf.addExpectedOutputFile("output/part-00000");
        
        
        // execute
        
        WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/spark/file_word_counter", conf);
        
        
        // assert
        
        File outputFile = workflowTestResult.getWorkflowOutputFile("output/part-00000"); 
        
        String output = FileUtils.readFileToString(outputFile, "UTF-8");
        
        
        assertTrue(output.contains("(universe,2)"));
        
        assertTrue(output.contains("(dance,1)"));
        
    }
    
}

