package eu.dnetlib.iis.wf.affmatching;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganizationWithProvenance;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
* @author mhorst
*/

public class AffMatchingDedupJobTest {
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputADirPath;
    
    private String inputBDirPath;
    
    private String outputDirPath;
    
    private String outputReportPath;
    
    
    @BeforeEach
    public void before() {
        
        inputADirPath = workingDir + "/affiliation_dedup/input/a";
        inputBDirPath = workingDir + "/affiliation_dedup/input/b";
        outputDirPath = workingDir + "/affiliation_dedup/output";
        outputReportPath = workingDir + "/affiliation_dedup/report";
        
    }
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void affiliationDedupJob() throws IOException {
        
        
        // given

        String jsonInputAPath = ClassPathResourceProvider
                .getResourcePath("data/dedup/input/input1.json");
        String jsonInputBPath = ClassPathResourceProvider
                .getResourcePath("data/dedup/input/input2.json");

        String jsonOutputPath = ClassPathResourceProvider
                .getResourcePath("data/dedup/expectedOutput/matchedOrganizations.json");
        String jsonOutputReportPath = ClassPathResourceProvider
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
        assertEquals(1,
                HdfsUtils.countFiles(new Configuration(), outputReportPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonOutputReportPath, ReportEntry.class);

    }



}
