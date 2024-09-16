package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlPreMatching;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.SoftwareHeritageOrigin;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * {@link SoftwareHeritageUrlsMatchingJob} test class. 
 * @author mhorst
 */
@SlowTest
public class SoftwareHeritageUrlsMatchingJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputDocumentToSoftwarePath;
    
    private String inputSoftwareHeritageOriginPath;
    
    private String outputPath;
    

    @BeforeEach
    public void beforeEach() {
        inputDocumentToSoftwarePath = workingDir + "/soft/inputDocumentToSoftware";
        inputSoftwareHeritageOriginPath = workingDir + "/soft/inputSoftwareHeritageOrigin";
        outputPath = workingDir + "/soft/output";
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void testJoiningSHwithMiningResults() throws IOException {
        
        // given
        String jsonInputDocumentToSoftwareFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/document_to_softwareurl_pre_matching.json");
        String jsonInputSoftwareHeritageOriginFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/sh_origins.json");
        
        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/document_to_softwareurl_post_matching.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputDocumentToSoftwareFile, DocumentToSoftwareUrlPreMatching.class), 
                inputDocumentToSoftwarePath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareHeritageOriginFile, SoftwareHeritageOrigin.class), 
                inputSoftwareHeritageOriginPath);
        
        // execute
        executor.execute(buildSoftwareHeritageUrlsMatchingJob(inputDocumentToSoftwarePath, inputSoftwareHeritageOriginPath, outputPath));
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithMeta.class);
    }
    
    @Test
    public void testJoiningUnmatchableSHwithMiningResults() throws IOException {
        
        // given
        String jsonInputDocumentToSoftwareFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/document_to_softwareurl_pre_matching.json");
        String jsonInputSoftwareHeritageOriginFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/sh_unmatchable_origins.json");
        
        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/softwareurl/data/document_to_softwareurl_post_matching_empty_sh.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputDocumentToSoftwareFile, DocumentToSoftwareUrlPreMatching.class), 
                inputDocumentToSoftwarePath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareHeritageOriginFile, SoftwareHeritageOrigin.class), 
                inputSoftwareHeritageOriginPath);
        
        // execute
        executor.execute(buildSoftwareHeritageUrlsMatchingJob(inputDocumentToSoftwarePath, inputSoftwareHeritageOriginPath, outputPath));
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputPath, jsonOutputFile, DocumentToSoftwareUrlWithMeta.class);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildSoftwareHeritageUrlsMatchingJob(String inputDocumentToSoftware, 
            String inputSoftwareHeritageOrigin, String outputPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName("SoftwareHeritage Urls Matching Job")
                .setMainClass(SoftwareHeritageUrlsMatchingJob.class)
                .addArg("-inputDocumentToSoftware", inputDocumentToSoftware)
                .addArg("-inputSoftwareHeritageOrigin", inputSoftwareHeritageOrigin)
                .addArg("-outputDocumentToSoftware", outputPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
        
        return sparkJob;
    }

}
