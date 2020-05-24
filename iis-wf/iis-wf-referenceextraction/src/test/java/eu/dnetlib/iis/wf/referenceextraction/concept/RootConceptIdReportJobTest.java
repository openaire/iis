package eu.dnetlib.iis.wf.referenceextraction.concept;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * 
 * @author mhorst
 *
 */
public class RootConceptIdReportJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputDocumentToConceptDirPath;
    
    private String outputReportDirPath;
    
    
    @Before
    public void before() {
        workingDir = Files.createTempDir();
        inputDocumentToConceptDirPath = workingDir + "/root_concept_report/input_document_to_concept";
        outputReportDirPath = workingDir + "/root_concept_report/output_report";
    }
    
    
    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void generateReport() throws IOException {
        
        // given
        String jsonInputDocumentToConceptFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/concept/root_conceptid_report/data/input_document_to_concept.json";
        String jsonOutputReportFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/concept/root_conceptid_report/data/output_report.json";

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputDocumentToConceptFile, DocumentToConceptId.class),
                inputDocumentToConceptDirPath);
        
        // execute
        executor.execute(buildRootConceptIdReportJob(inputDocumentToConceptDirPath, outputReportDirPath));
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDirPath, jsonOutputReportFile, ReportEntry.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    
    private SparkJob buildRootConceptIdReportJob(String inputDocumentToConceptDirPath, String outputReportDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Report Generator")

                .setMainClass(RootConceptIdReportJob.class)
                .addArg("-inputDocumentToConceptAvroPath", inputDocumentToConceptDirPath)
                .addArg("-outputReportPath", outputReportDirPath)
                .addArg("-reportKeyTemplate", "processing.referenceExtraction.concept.references.byrootid.#{rootConceptId}")
                .addJobProperty("spark.driver.host", "localhost")
                
                .build();
        
        return sparkJob;
    }
}
