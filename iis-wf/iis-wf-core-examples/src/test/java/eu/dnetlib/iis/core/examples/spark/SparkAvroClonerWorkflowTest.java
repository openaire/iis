package eu.dnetlib.iis.core.examples.spark;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.WorkflowTestResult;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;

/**
 * @author Łukasz Dumiszewski
 */
@Category(IntegrationTest.class)
public class SparkAvroClonerWorkflowTest extends AbstractOozieWorkflowTestCase {

        private static Logger log = LoggerFactory.getLogger(SparkAvroClonerWorkflowTest.class);
    
        @Test
        public void sparkAvroCloner() throws Exception{
            
            
            // given
            
            OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
            
            conf.addExpectedOutputAvroDataStore("simple_java_cloner/person/");
            
            
            // execute
            
            WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/spark/spark_cloner_node", conf);
            
            
            // assert
            
            List<Person> generatedRecords = workflowTestResult.getAvroDataStore("simple_java_cloner/person/");
            
            assertEquals(20, generatedRecords.size());

            log.info(generatedRecords.toString());
            
            assertEquals(4, generatedRecords.stream().filter(person -> person.getName().equals(new Utf8("Lisbeth Salander"))).count());
            
        }
        
}