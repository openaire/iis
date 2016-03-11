package eu.dnetlib.iis.workflows.citationmatching.output;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * @author madryk
 */
public class CitationMatchingOutputTransformerJobTest {

    private static final String DATA_DIRECTORY_PATH = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/data/output_transformer";
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        inputDirPath = workingDir + "/spark_citation_matching_output_transformer/input";
        outputDirPath = workingDir + "/spark_citation_matching_output_transformer/output";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingOutputTransformer() throws IOException {
        
        
        // given
        
        String jsonInputFile = DATA_DIRECTORY_PATH + "/citation.json";
        String jsonOutputFile = DATA_DIRECTORY_PATH + "/commonCitation.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputFile, Citation.class),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingOutputTransformerJob(inputDirPath, outputDirPath));
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, eu.dnetlib.iis.common.citations.schemas.Citation.class);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildCitationMatchingOutputTransformerJob(String inputDirPath, String outputDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Citation Matching - Output Transformer")

                .setMainClass(CitationMatchingOutputTransformerJob.class)
                .addArg("-input", inputDirPath)
                .addArg("-output", outputDirPath)
                
                .build();
        
        return sparkJob;
    }
    
}
