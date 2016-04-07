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
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
        inputDirPath = workingDir + "/affiliation_matching/input";
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
        String jsonOutputFile = "src/test/resources/data/expectedOutput/affMatchOrganizations.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputOrgPath, Organization.class), inputDirPath);
        
        
        // execute & assert
        
        executeJobAndAssert(jsonOutputFile);
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private void executeJobAndAssert(String jsonOutputFile) throws IOException {

        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(AffMatchingJob.class)
                                           .addArg("-inputAvroOrgPath", inputDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, AffMatchOrganization.class);
    }



}
