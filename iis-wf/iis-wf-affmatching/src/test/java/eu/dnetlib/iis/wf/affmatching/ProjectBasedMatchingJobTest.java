package eu.dnetlib.iis.wf.affmatching;

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
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
* @author mhorst
*/

public class ProjectBasedMatchingJobTest {
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputInferredDocProjDirPath;
    
    private String inputDocProjDirPath;
    
    private float inputDocProjConfidenceThreshold = 0.8f;
    
    private String projectFundingClassWhitelistRegex = "^EC.*";
    
    private String inputProjOrgDirPath;
    
    private String inputProjectDirPath;
    
    private String outputDirPath;
    
    private String outputReportPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();

        inputInferredDocProjDirPath = workingDir + "/projectbased_matching/input/doc_proj_inferred";
        inputDocProjDirPath = workingDir + "/projectbased_matching/input/doc_proj";
        inputProjOrgDirPath = workingDir + "/projectbased_matching/input/proj_org";
        inputProjectDirPath = workingDir + "/projectbased_matching/input/project";
        outputDirPath = workingDir + "/projectbased_matching/output";
        outputReportPath = workingDir + "/projectbased_matching/report";
        
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void projectBasedMatchingJob() throws IOException {
        
        
        // given

        String jsonInputInferredDocProjPath = "src/test/resources/data/projectbased/input/docProjInferred.json";
        String jsonInputDocProjPath = "src/test/resources/data/projectbased/input/docProj.json";
        String jsonInputProjOrgPath = "src/test/resources/data/projectbased/input/projOrg.json";
        String jsonInputProjectPath = "src/test/resources/data/projectbased/input/project.json";
        
        String jsonOutputPath = "src/test/resources/data/projectbased/expectedOutput/matchedOrganizations.json";
        String jsonOutputReportPath = "src/test/resources/data/projectbased/expectedOutput/report.json";
        
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputInferredDocProjPath, DocumentToProject.class), inputInferredDocProjDirPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputDocProjPath, eu.dnetlib.iis.importer.schemas.DocumentToProject.class), inputDocProjDirPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputProjOrgPath, ProjectToOrganization.class), inputProjOrgDirPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputProjectPath, Project.class), inputProjectDirPath);
        
        // execute & assert
        
        executeJobAndAssert(jsonOutputPath, jsonOutputReportPath);
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private void executeJobAndAssert(String jsonOutputPath, String jsonOutputReportPath) throws IOException {

        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName(getClass().getName())
        
                                           .setMainClass(ProjectBasedMatchingJob.class)
                                           .addArg("-inputAvroDocProjPath", inputDocProjDirPath)
                                           .addArg("-inputAvroInferredDocProjPath", inputInferredDocProjDirPath)
                                           .addArg("-inputDocProjConfidenceThreshold", String.valueOf(inputDocProjConfidenceThreshold))
                                           .addArg("-projectFundingClassWhitelistRegex", projectFundingClassWhitelistRegex)
                                           .addArg("-inputAvroProjOrgPath", inputProjOrgDirPath)
                                           .addArg("-inputAvroProjectPath", inputProjectDirPath)
                                           .addArg("-outputAvroPath", outputDirPath)
                                           .addArg("-outputAvroReportPath", outputReportPath)
                                           .addJobProperty("spark.driver.host", "localhost")
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDirPath, jsonOutputPath, MatchedOrganization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportPath, jsonOutputReportPath, ReportEntry.class);

    }



}
