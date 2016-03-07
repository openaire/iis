package eu.dnetlib.iis.core.examples.spark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.avro.Document;
import eu.dnetlib.iis.common.spark.pipe.SparkPipeMapReduce;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.WordCount;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;


/**
 * 
 * @author madryk
 *
 */
public class SparkPipeMapReduceTest {

    private Logger log = LoggerFactory.getLogger(getClass());

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    private String outputDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        inputDirPath = workingDir + "/input";
        outputDirPath = workingDir + "/output";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void mapReduceWordCount() throws IOException {
        
        // given
        
        AvroTestUtils.createLocalAvroDataStore(StandardDataStoreExamples.getDocument(), inputDirPath);
        
        
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Pipe WordCount")
                
                .setMainClass(SparkPipeMapReduce.class)
                .addArg("-inputAvroPath", inputDirPath)
                .addArg("-inputAvroSchemaClass", Document.class.getName())
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-outputAvroSchemaClass", WordCount.class.getName())
                
                .addArg("-mapperScript", "src/test/resources/eu/dnetlib/iis/core/examples/spark/pipe_cloner/oozie_app/scripts/wordcount_mapper.py")
                .addArg("-reducerScript", "src/test/resources/eu/dnetlib/iis/core/examples/spark/pipe_cloner/oozie_app/scripts/wordcount_reducer.py")
                
                .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        // assert
        
        List<WordCount> wordCounts = AvroTestUtils.readLocalAvroDataStore(outputDirPath);

        log.info(wordCounts.toString());
        
        assertThat(wordCounts, hasItem(new WordCount("basics", 2)));
        assertThat(wordCounts, hasItem(new WordCount("even", 1)));
        assertThat(wordCounts, hasItem(new WordCount("of", 2)));
        assertThat(wordCounts, hasSize(10));
    }
    
    
    @Test
    public void mapReduceCloner() throws IOException {
        
        // given
        
        AvroTestUtils.createLocalAvroDataStore(StandardDataStoreExamples.getPerson(), inputDirPath);
        
        
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Pipe Cloner")
                
                .setMainClass(SparkPipeMapReduce.class)
                .addArg("-inputAvroPath", inputDirPath)
                .addArg("-inputAvroSchemaClass", Person.class.getName())
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-outputAvroSchemaClass", Person.class.getName())
                
                .addArg("-mapperScript", "src/test/resources/eu/dnetlib/iis/core/examples/spark/pipe_cloner/oozie_app/scripts/cloner.py")
                .addArg("-mapperScriptArgs", "--copies 3")
                .addArg("-reducerScript", "src/test/resources/eu/dnetlib/iis/core/examples/spark/pipe_cloner/oozie_app/scripts/cloner.py")
                .addArg("-reducerScriptArgs", "--copies 2")
                
                .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        // assert
        
        List<Person> people = AvroTestUtils.readLocalAvroDataStore(outputDirPath);

        log.info(people.toString());
        
        assertEquals(5*6, people.size());
        assertEquals(6, people.stream().filter(p -> p.getId() == 1).count());
        assertEquals(6, people.stream().filter(p -> p.getId() == 20).count());
    }
    
}
