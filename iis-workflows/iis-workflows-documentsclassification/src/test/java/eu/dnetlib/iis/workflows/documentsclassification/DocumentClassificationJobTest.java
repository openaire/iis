package eu.dnetlib.iis.workflows.documentsclassification;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.spark.test.SparkJob;
import eu.dnetlib.iis.common.spark.test.SparkJobBuilder;
import eu.dnetlib.iis.common.spark.test.SparkJobExecutor;
import eu.dnetlib.iis.core.common.AvroAssertTestUtil;
import eu.dnetlib.iis.core.common.AvroTestUtils;
import eu.dnetlib.iis.core.common.JsonAvroTestUtils;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * @author Łukasz Dumiszewski
 */

public class DocumentClassificationJobTest {

    
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
    public void documentClassificationJob() throws IOException {
        
        
        // given
        
        String inputDirPath = workingDir + "/document_classification/input";
        String outputDirPath = workingDir + "/document_classification/output";
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/workflows/documentsclassification/data/input/spark_job_test_documents.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/workflows/documentsclassification/data/expected_output/spark_job_test_dc_metadata.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        
        
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(DocumentClassificationJob.class)
                                           .addArg("-inputAvroPath", inputDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJson(outputDirPath, jsonOutputFile, DocumentMetadata.class);
    }
        
}
