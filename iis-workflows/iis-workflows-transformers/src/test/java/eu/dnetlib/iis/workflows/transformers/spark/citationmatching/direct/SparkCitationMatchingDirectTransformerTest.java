package eu.dnetlib.iis.workflows.transformers.spark.citationmatching.direct;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.Files;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.spark.test.SparkJob;
import eu.dnetlib.iis.common.spark.test.SparkJobBuilder;
import eu.dnetlib.iis.common.spark.test.SparkJobExecutor;
import eu.dnetlib.iis.core.common.AvroAssertTestUtil;
import eu.dnetlib.iis.core.common.AvroTestUtils;
import eu.dnetlib.iis.core.common.JsonTestUtils;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * 
 * @author madryk
 *
 */
@Category(IntegrationTest.class)
public class SparkCitationMatchingDirectTransformerTest {
    
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
    public void citationMatchingDirectTransformer() throws IOException {
        
        
        // given
        
        String inputDirPath = workingDir + "/spark_citation_matching_direct_transformer/input";
        String outputDirPath = workingDir + "/spark_citation_matching_direct_transformer/output";
        
        String jsonInputFile = "src/test/resources/eu/dnetlib/iis/workflows/transformers/spark/citationmatching/direct/sampledataproducer/data/metadata.json";
        String jsonOutputFile = "src/test/resources/eu/dnetlib/iis/workflows/transformers/spark/citationmatching/direct/sampledataproducer/data/citation_metadata.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonTestUtils.readJsonDataStore(jsonInputFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputDirPath);
        
        
        
        
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName("Spark Avro Cloner")
        
                                           .setMainClass(SparkCitationMatchingDirectTransformer.class)
                                           .addArg("-inputAvroPath", inputDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJson(outputDirPath, jsonOutputFile, DocumentMetadata.class);
        
        
    }
}
