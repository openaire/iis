package eu.dnetlib.iis.wf.citationmatching.output;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;

/**
 * @author madryk
 */
public class CitationMatchingOutputTransformerJobTest {

    private static final String DATA_DIRECTORY_PATH = "src/test/resources/eu/dnetlib/iis/wf/citationmatching/data/output_transformer";
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    
    @BeforeEach
    public void before() {
        
        inputDirPath = workingDir + "/spark_citation_matching_output_transformer/input";
        outputDirPath = workingDir + "/spark_citation_matching_output_transformer/output";
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
                .addJobProperty("spark.driver.host", "localhost")

                .build();
        
        return sparkJob;
    }
    
}
