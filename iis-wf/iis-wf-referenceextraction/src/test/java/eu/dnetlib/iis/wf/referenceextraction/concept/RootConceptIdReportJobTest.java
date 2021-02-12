package eu.dnetlib.iis.wf.referenceextraction.concept;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
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
 * 
 * @author mhorst
 *
 */
@SlowTest
public class RootConceptIdReportJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputDocumentToConceptDirPath;
    
    private String outputReportDirPath;
    
    
    @BeforeEach
    public void before() {
        inputDocumentToConceptDirPath = workingDir + "/root_concept_report/input_document_to_concept";
        outputReportDirPath = workingDir + "/root_concept_report/output_report";
    }
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void generateReport() throws IOException {
        
        // given
        String jsonInputDocumentToConceptFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/concept/root_conceptid_report/data/input_document_to_concept.json");
        String jsonOutputReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/concept/root_conceptid_report/data/output_report.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputDocumentToConceptFile, DocumentToConceptId.class),
                inputDocumentToConceptDirPath);
        
        // execute
        executor.execute(buildRootConceptIdReportJob(inputDocumentToConceptDirPath, outputReportDirPath));
        
        // assert
        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDirPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
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
