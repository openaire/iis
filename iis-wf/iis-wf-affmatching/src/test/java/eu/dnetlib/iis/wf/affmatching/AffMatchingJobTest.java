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
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
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
    
    private String outputAffDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
        inputOrgDirPath = workingDir + "/affiliation_matching/input/organizations";
        inputAffDirPath = workingDir + "/affiliation_matching/input/affiliations";
        outputDirPath = workingDir + "/affiliation_matching/output";
        outputAffDirPath = workingDir + "/affiliation_matching/output/affiliations";
        
        
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
        String jsonOutputOrgFile = "src/test/resources/data/expectedOutput/affMatchOrganizations.json";
        String jsonOutputAffFile = "src/test/resources/data/expectedOutput/affMatchAffiliations.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputOrgPath, Organization.class), inputOrgDirPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputAffPath, ExtractedDocumentMetadata.class), inputAffDirPath);
        
        // execute & assert
        
        executeJobAndAssert(jsonOutputOrgFile, jsonOutputAffFile);
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private void executeJobAndAssert(String jsonOutputFile, String jsonOutputAffFile) throws IOException {

        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(AffMatchingJob.class)
                                           .addArg("-inputAvroOrgPath", inputOrgDirPath)
                                           .addArg("-inputAvroAffPath", inputAffDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .addArg("-outputAvroAffPath", outputAffDirPath)
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, AffMatchOrganization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputAffDirPath, jsonOutputAffFile, AffMatchAffiliation.class);
    }



}
