package eu.dnetlib.iis.wf.affmatching;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.MatchedAffiliation;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
* @author Łukasz Dumiszewski
*/

public class AffMatchingJobTest {
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputOrgDirPath;
    
    private String inputAffDirPath;
    
    private String outputDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
        inputOrgDirPath = workingDir + "/affiliation_matching/input/organizations";
        inputAffDirPath = workingDir + "/affiliation_matching/input/affiliations";
        outputDirPath = workingDir + "/affiliation_matching/output";
        
        
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void affiliationMatchingJob() throws IOException {
        
        
        // given
        
        String jsonInputOrgPath = "src/test/resources/data/input/organizations.json";
        String jsonInputAffPath = "src/test/resources/data/input/affiliations.json";
        String jsonOutputPath = "src/test/resources/data/expectedOutput/matchedAffiliations.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputOrgPath, Organization.class), inputOrgDirPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputAffPath, ExtractedDocumentMetadata.class), inputAffDirPath);
        
        // execute & assert
        
        executeJobAndAssert(jsonOutputPath);
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private void executeJobAndAssert(String jsonOutputPath) throws IOException {

        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(AffMatchingJob.class)
                                           .addArg("-inputAvroOrgPath", inputOrgDirPath)
                                           .addArg("-inputAvroAffPath", inputAffDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputPath, MatchedAffiliation.class);

    }



}
