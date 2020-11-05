package eu.dnetlib.iis.wf.citationmatching.input;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;

@SlowTest
public class CitationMatchingInputTransformerJobTest {

    private static final String DATA_DIRECTORY_PATH = "src/test/resources/eu/dnetlib/iis/wf/citationmatching/data/input_transformer";
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputMetadataPath;
    
    private String inputMatchedCitationsPath;
    
    private String outputDirPath;
    
    
    @BeforeEach
    public void before() {
        
        inputMetadataPath = workingDir + "/spark_citation_matching_input_transformer/inputMetadata";
        inputMatchedCitationsPath = workingDir + "/spark_citation_matching_input_transformer/inputMatchedCitations";
        outputDirPath = workingDir + "/spark_citation_matching_input_transformer/output";
    }

    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingInputTransformer() throws IOException {
        
        
        // given
        
        String jsonInputMetadataFile = DATA_DIRECTORY_PATH + "/full_document.json";
        String jsonOutputFile = DATA_DIRECTORY_PATH + "/document.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputMetadataPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingInputTransformerJob(inputMetadataPath, WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE, outputDirPath));
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, DocumentMetadata.class);
        
    }
    
    @Test
    public void citationMatchingInputTransformerWithFiltering() throws IOException {
        
        
        // given
        
        String jsonInputMetadataFile = DATA_DIRECTORY_PATH + "/full_document.json";
        String jsonInputMatchedCitationsFile = DATA_DIRECTORY_PATH + "/matched_citations.json";
        String jsonOutputFile = DATA_DIRECTORY_PATH + "/document_filtered.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile, ExtractedDocumentMetadataMergedWithOriginal.class),
                inputMetadataPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputMatchedCitationsFile, Citation.class),
                inputMatchedCitationsPath);
        
        
        // execute
        
        executor.execute(buildCitationMatchingInputTransformerJob(inputMetadataPath, inputMatchedCitationsPath, outputDirPath));
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, DocumentMetadata.class);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildCitationMatchingInputTransformerJob(String inputMetadataDirPath, String inputMatchedCitationsDir, String outputDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Citation Matching - Input Transformer")

                .setMainClass(CitationMatchingInputTransformerJob.class)
                .addArg("-inputMetadata", inputMetadataDirPath)
                .addArg("-inputMatchedCitations", inputMatchedCitationsDir)
                .addArg("-output", outputDirPath)
                .addJobProperty("spark.driver.host", "localhost")
                
                .build();
        
        return sparkJob;
    }
    
}
