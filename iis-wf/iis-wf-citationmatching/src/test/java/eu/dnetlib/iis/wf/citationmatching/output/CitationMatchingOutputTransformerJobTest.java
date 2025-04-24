package eu.dnetlib.iis.wf.citationmatching.output;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.lock.ZookeeperLockManagerFactory;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;

import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;

/**
 * @author madryk
 */
@SlowTest
public class CitationMatchingOutputTransformerJobTest {

    private static final String DATA_DIRECTORY_PATH = "src/test/resources/eu/dnetlib/iis/wf/citationmatching/data/output_transformer";
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputDocumentMetadataDirPath;
    
    private String inputMatchedCitationsDirPath;
    
    private String outputDirPath;
    
    private Path cacheRootDir;
    
    private static TestingServer zookeeperServer;

    @Mock
    private LockManager lockManager;
    
    @BeforeAll
    public static void beforeAll() throws Exception {
        zookeeperServer = new TestingServer(true);
    }
    
    @BeforeEach
    public void before() {
        inputDocumentMetadataDirPath = workingDir + "/spark_citation_matching_output_transformer/inputDocumentMetadata";
        inputMatchedCitationsDirPath = workingDir + "/spark_citation_matching_output_transformer/inputMatchedCitation";
        outputDirPath = workingDir + "/spark_citation_matching_output_transformer/output";
        cacheRootDir = new Path(workingDir + "/spark_citation_matching_output_transformer/cache/");
    }
    
    @AfterAll
    public static void afterAll() throws IOException {
        zookeeperServer.close();
    }

    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingOutputTransformer_ON_EMPTY_CACHE() throws IOException {
        
        
        // given
        
        String jsonMatchedCitationsInputFile = DATA_DIRECTORY_PATH + "/citation.json";
        String jsonDocumentMetadataInputFile = DATA_DIRECTORY_PATH + "/documentMetadata.json";
        String jsonOutputFile = DATA_DIRECTORY_PATH + "/outputCitation.json";
        String cacheOutputFile = DATA_DIRECTORY_PATH + "/cache1.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonMatchedCitationsInputFile, Citation.class),
                inputMatchedCitationsDirPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonDocumentMetadataInputFile, ExtractedDocumentMetadataMergedWithOriginal.class), 
                inputDocumentMetadataDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingOutputTransformerJob(inputDocumentMetadataDirPath, inputMatchedCitationsDirPath, outputDirPath));
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, eu.dnetlib.iis.common.citations.schemas.Citation.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(cacheRootDir.toString() + "/000001/data", cacheOutputFile, Citations.class);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildCitationMatchingOutputTransformerJob(String inputMetadataDirPath, 
            String inputMatchedCitationsDirPath, String outputDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Citation Matching - Output Transformer")

                .setMainClass(CitationMatchingOutputTransformerJob.class)
                .addArg("-inputMetadata", inputMetadataDirPath)
                .addArg("-inputMatchedCitations", inputMatchedCitationsDirPath)
                .addArg("-cacheRootDir", cacheRootDir.toString())
                .addArg("-cacheOlderThanXYears", "3")
                .addArg("-output", outputDirPath)
                .addArg("-numberOfEmittedFiles", "1")
                .addArg("-lockManagerFactoryClassName", ZookeeperLockManagerFactory.class.getName())
                .addJobProperty("spark.driver.host", "localhost")
                .addJobProperty(ZKFailoverController.ZK_QUORUM_KEY, "localhost:" + zookeeperServer.getPort())
                .build();
        
        return sparkJob;
    }
    
}
