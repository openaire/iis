package eu.dnetlib.iis.wf.referenceextraction.project;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
    
    
    @BeforeEach
    public void before() throws IOException {
        workingDir = Files.createTempDirectory(ProjectFunderReportJobTest.class.getSimpleName()).toFile();
        inputProjectDirPath = workingDir + "/spark_project_referenceextraction_report/input_project";
        inputDocumentToProjectDirPath = workingDir + "/spark_project_referenceextraction_report/input_document_to_project";
        outputReportDirPath = workingDir + "/spark_project_referenceextraction_report/output_report";
    }
    
    
    @AfterEach
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
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
                HdfsUtils.countFiles(new Configuration(), outputReportDirPath, x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
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
