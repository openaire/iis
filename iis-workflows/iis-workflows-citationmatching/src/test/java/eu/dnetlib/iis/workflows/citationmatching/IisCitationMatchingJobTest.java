package eu.dnetlib.iis.workflows.citationmatching;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.core.common.AvroAssertTestUtil;
import eu.dnetlib.iis.core.common.AvroTestUtils;
import eu.dnetlib.iis.workflows.citationmatching.converter.DocumentAvroDatastoreProducer;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * @author madryk
 */
public class IisCitationMatchingJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        inputDirPath = workingDir + "/spark_citation_matching/input";
        outputDirPath = workingDir + "/spark_citation_matching/output";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingDirect() throws IOException {
        
        
        // given
        
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/main_workflow/data/citation.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                DocumentAvroDatastoreProducer.getDocumentMetadataList(),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingJob(inputDirPath, outputDirPath));
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, Citation.class);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildCitationMatchingJob(String inputDirPath, String outputDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Citation Matching")

                .setMainClass(IisCitationMatchingJob.class)
                .addArg("-fullDocumentPath", inputDirPath)
                .addArg("-outputDirPath", outputDirPath)
                
                .build();
        
        return sparkJob;
    }
    
}
