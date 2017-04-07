package eu.dnetlib.iis.wf.referenceextraction.project;

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
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * 
 * @author mhorst
 *
 */
public class ProjectFunderReportJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputProjectDirPath;
    
    private String inputDocumentToProjectDirPath;
    
    private String outputReportDirPath;
    
    
    @Before
    public void before() {
        workingDir = Files.createTempDir();
        inputProjectDirPath = workingDir + "/spark_project_referenceextraction_report/input_project";
        inputDocumentToProjectDirPath = workingDir + "/spark_project_referenceextraction_report/input_document_to_project";
        outputReportDirPath = workingDir + "/spark_project_referenceextraction_report/output_report";
    }
    
    
    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void generateReport() throws IOException {
        
        // given
        String jsonInputProjectFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/project/funder_report/data/input_project.json";
        String jsonInputDocumentToProjectFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/project/funder_report/data/input_document_to_project.json";
        String jsonOutputReportFile = "src/test/resources/eu/dnetlib/iis/wf/referenceextraction/project/funder_report/data/output_report.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputProjectFile, Project.class), 
                inputProjectDirPath);
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputDocumentToProjectFile, DocumentToProject.class),
                inputDocumentToProjectDirPath);
        
        // execute
        executor.execute(buildProjectFunderReportJob(inputProjectDirPath, inputDocumentToProjectDirPath, outputReportDirPath));
        
        // assert
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
                .addArg("-reportKeyTemplate", "processing.referenceExtraction.project.reference.byfunder.#{funder}")
                .addJobProperty("spark.driver.host", "localhost")
                
                .build();
        
        return sparkJob;
    }
}
