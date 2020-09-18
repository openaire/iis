package eu.dnetlib.iis.wf.affmatching;

import java.io.File;
import java.io.IOException;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganizationWithProvenance;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
* @author mhorst
*/

public class AffMatchingDedupJobTest {
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputADirPath;
    
    private String inputBDirPath;
    
    private String outputDirPath;
    
    private String outputReportPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
        inputADirPath = workingDir + "/affiliation_dedup/input/a";
        inputBDirPath = workingDir + "/affiliation_dedup/input/b";
        outputDirPath = workingDir + "/affiliation_dedup/output";
        outputReportPath = workingDir + "/affiliation_dedup/report";
        
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void affiliationDedupJob() throws IOException {
        
        
        // given

        String jsonInputAPath = StaticResourceProvider
                .getResourcePath("data/dedup/input/input1.json");
        String jsonInputBPath = StaticResourceProvider
                .getResourcePath("data/dedup/input/input2.json");

        String jsonOutputPath = StaticResourceProvider
                .getResourcePath("data/dedup/expectedOutput/matchedOrganizations.json");
        String jsonOutputReportPath = StaticResourceProvider
                .getResourcePath("data/dedup/expectedOutput/report.json");
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputAPath, MatchedOrganization.class), inputADirPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputBPath, MatchedOrganization.class), inputBDirPath);
        
        // execute & assert
        
        executeJobAndAssert(jsonOutputPath, jsonOutputReportPath);
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private void executeJobAndAssert(String jsonOutputPath, String jsonOutputReportPath) throws IOException {

        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(AffMatchingDedupJob.class)
                                           .addArg("-inputAPath", inputADirPath)
                                           .addArg("-inputBPath", inputBDirPath)
                                           .addArg("-inferenceProvenanceInputA", "affmatch")
                                           .addArg("-inferenceProvenanceInputB", "docmatch")
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .addArg("-outputAvroReportPath", outputReportPath)
                                           .addJobProperty("spark.driver.host", "localhost")
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputPath, MatchedOrganizationWithProvenance.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonOutputReportPath, ReportEntry.class);

    }



}
