package eu.dnetlib.iis.core.examples.spark;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.common.spark.test.SparkJob;
import eu.dnetlib.iis.common.spark.test.SparkJobBuilder;
import eu.dnetlib.iis.common.spark.test.SparkJobExecutor;
import eu.dnetlib.iis.core.common.AvroTestUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;


/**
 * @author Łukasz Dumiszewski
 */
@Category(IntegrationTest.class)
public class SparkSqlAvroClonerTest {

    private Logger log = LoggerFactory.getLogger(SparkSqlAvroClonerTest.class);
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void sparkAvroCloner() throws IOException {
        
        
        // given
        
        String inputDirPath = workingDir + "/spark_sql_avro_cloner/input";
        String outputDirPath = workingDir + "/spark_sql_avro_cloner/output";
        
        
        AvroTestUtils.createLocalAvroDataStore(StandardDataStoreExamples.getPerson(), inputDirPath);
        
        
        
        
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName("Spark SQL Avro Cloner")
        
                                           .setMainClass(SparkSqlAvroCloner.class)
                                           .addArg("-avroSchemaClass", Person.class.getName())
                                           .addArg("-inputAvroPath", inputDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .addArg("-numberOfCopies", ""+3)
                                           
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        
        List<Person> people = AvroTestUtils.readLocalAvroDataStore(outputDirPath);

        log.info(people.toString());
        
        assertEquals(15, people.size());
        
        assertEquals(3, people.stream().filter(p->p.getName().equals(new Utf8("Stieg Larsson"))).count());
        
    }

    


    
    
    
}
