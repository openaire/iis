package eu.dnetlib.iis.wf.citationmatching;

import com.google.common.io.Files;
import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.wf.citationmatching.converter.DocumentAvroDatastoreProducer;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;

/**
 * @author madryk
 */
public class IisCitationMatchingJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    private String reportDirPath;
    
    
    @BeforeEach
    public void before() {
        
        workingDir = Files.createTempDir();
        inputDirPath = workingDir + "/spark_citation_matching/input";
        outputDirPath = workingDir + "/spark_citation_matching/output";
        reportDirPath = workingDir + "/spark_citation_matching_direct/report";
    }
    
    
    @AfterEach
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingDirect() throws IOException {
        
        
        // given

        String jsonOutputFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/main_workflow/data/citation.json");
        String jsonReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/main_workflow/data/report.json");
        
        AvroTestUtils.createLocalAvroDataStore(
                DocumentAvroDatastoreProducer.getDocumentMetadataList(),
                inputDirPath);
        
        
        
        // execute
        
        executor.execute(buildCitationMatchingJob(inputDirPath, outputDirPath, reportDirPath));
        
        
        
        // assert
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputFile, Citation.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(reportDirPath, jsonReportFile, ReportEntry.class);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private SparkJob buildCitationMatchingJob(String inputDirPath, String outputDirPath, String reportDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Citation Matching")

                .setMainClass(IisCitationMatchingJob.class)
                .addArg("-fullDocumentPath", inputDirPath)
                .addArg("-outputDirPath", outputDirPath)
                .addArg("-outputReportPath", reportDirPath)
                .addJobProperty("spark.driver.host", "localhost")
                
                .build();
        
        return sparkJob;
    }
    
}
