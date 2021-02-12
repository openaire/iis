package eu.dnetlib.iis.wf.referenceextraction.project;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
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
public class ProjectFunderReportJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public File workingDir;
    
    private String inputProjectDirPath;
    
    private String inputDocumentToProjectDirPath;
    
    private String outputReportDirPath;
    
    
    @BeforeEach
    public void before() {
        inputProjectDirPath = workingDir + "/spark_project_referenceextraction_report/input_project";
        inputDocumentToProjectDirPath = workingDir + "/spark_project_referenceextraction_report/input_document_to_project";
        outputReportDirPath = workingDir + "/spark_project_referenceextraction_report/output_report";
    }
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void generateReport() throws IOException {
        
        // given
        String jsonInputProjectFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/project/funder_report/data/input_project.json");
        String jsonInputDocumentToProjectFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/project/funder_report/data/input_document_to_project.json");
        String jsonOutputReportFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/referenceextraction/project/funder_report/data/output_report.json");
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputProjectFile, Project.class), 
                inputProjectDirPath);
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputDocumentToProjectFile, DocumentToProject.class),
                inputDocumentToProjectDirPath);
        
        // execute
        executor.execute(buildProjectFunderReportJob(inputProjectDirPath, inputDocumentToProjectDirPath, outputReportDirPath));
        
        // assert
        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDirPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDirPath, jsonOutputReportFile, ReportEntry.class);
    }
    
    //------------------------ PRIVATE --------------------------
    
    
    private SparkJob buildProjectFunderReportJob(String inputProjectDirPath, 
            String inputDocumentToProjectDirPath, String outputReportDirPath) {
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName("Spark Report Generator")

                .setMainClass(ProjectFunderReportJob.class)
                .addArg("-inputProjectAvroPath", inputProjectDirPath)
                .addArg("-inputDocumentToProjectAvroPath", inputDocumentToProjectDirPath)
                .addArg("-outputReportPath", outputReportDirPath)
                .addArg("-reportKeyTemplate", "processing.referenceExtraction.project.references.byfunder.#{funder}")
                .addArg("-reportKeyTotal", "processing.referenceExtraction.project.references.total")
                .addJobProperty("spark.driver.host", "localhost")
                
                .build();
        
        return sparkJob;
    }
}
