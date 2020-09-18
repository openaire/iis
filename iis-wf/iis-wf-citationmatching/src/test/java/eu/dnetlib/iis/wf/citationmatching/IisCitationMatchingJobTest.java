package eu.dnetlib.iis.wf.citationmatching;

import java.io.File;
import java.io.IOException;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.wf.citationmatching.converter.DocumentAvroDatastoreProducer;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * @author madryk
 */
public class IisCitationMatchingJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDirPath;
    
    private String outputDirPath;
    
    private String reportDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        inputDirPath = workingDir + "/spark_citation_matching/input";
        outputDirPath = workingDir + "/spark_citation_matching/output";
        reportDirPath = workingDir + "/spark_citation_matching_direct/report";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void citationMatchingDirect() throws IOException {
        
        
        // given

        String jsonOutputFile = StaticResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/citationmatching/main_workflow/data/citation.json");
        String jsonReportFile = StaticResourceProvider
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
