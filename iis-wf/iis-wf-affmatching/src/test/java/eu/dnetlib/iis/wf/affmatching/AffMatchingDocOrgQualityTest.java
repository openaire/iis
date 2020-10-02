package eu.dnetlib.iis.wf.affmatching;

import com.google.common.io.Files;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.createLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.readLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.JsonAvroTestUtils.readMultipleJsonDataStores;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readMultipleJsons;
import static java.util.stream.Collectors.toList;

/**
 * Affiliation matching module test that measures quality of matching.<br/>
 * Tests in this class completely ignores {@link MatchedOrganization#getMatchStrength()}<br/>
 * <br/>
 * Quality of matching is described by three factors:<br/>
 * <ul>
 * <li>All matches - percentage of returned results to expected matches</li>
 * <li>Correct matches - percentage of returned matches that are also present in expected matches</li>
 * <li>False positives - percentage of returned matches that was matched, but incorrectly (are not present in expected matches)</li>
 * <ul><br/>
 * 
 * @author madryk
 */
@IntegrationTest
public class AffMatchingDocOrgQualityTest {

    private static final Logger logger = LoggerFactory.getLogger(AffMatchingDocOrgQualityTest.class);

    private final static String INPUT_DATA_DIR_PATH = "src/test/resources/experimentalData/input";
     
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputOrgDirPath;
    
    private String inputAffDirPath;
    
    private String inputDocProjDirPath;
    
    private String inputInferredDocProjDirPath;
    
    private float inputDocProjConfidenceThreshold = 0.8f;
    
    private String inputProjOrgDirPath;
    
    private String outputDirPath;
    
    private String outputReportPath;
    
    
    
    @BeforeEach
    public void before() {
        
        workingDir = Files.createTempDir();
        
        inputOrgDirPath = workingDir + "/affiliation_matching/input/organizations";
        inputAffDirPath = workingDir + "/affiliation_matching/input/affiliations";
        inputDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj";
        inputInferredDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj_inferred";
        inputProjOrgDirPath = workingDir + "/affiliation_matching/input/proj_org";
        outputDirPath = workingDir + "/affiliation_matching/output";
        outputReportPath = workingDir + "/affiliation_matching/report";
        
    }
    
    
    @AfterEach
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void affiliationMatchingJob_combined_data() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                of(INPUT_DATA_DIR_PATH + "/all_organizations.json"),
                of(INPUT_DATA_DIR_PATH + "/docs_with_aff_real_data.json"),
                of(INPUT_DATA_DIR_PATH + "/doc_project.json"),
                of(),
                of(INPUT_DATA_DIR_PATH + "/org_project.json"));
        
        
        // execute
        
        executeJob();
        
        
        // log

        logger.trace("ALL TEST DATA");
        
        readResultsAndPrintQualityRate(of(
                "src/test/resources/experimentalData/expectedOutput/matched_aff.json"));
    }
    
    
    
    //------------------------ PRIVATE --------------------------
    
    private void executeJob() {
        
        SparkJob sparkJob = SparkJobBuilder
                .create()
                
                .setAppName(getClass().getName())

                .setMainClass(AffMatchingJob.class)
                .addArg("-inputAvroOrgPath", inputOrgDirPath)
                .addArg("-inputAvroAffPath", inputAffDirPath)
                .addArg("-inputAvroDocProjPath", inputDocProjDirPath)
                .addArg("-inputAvroInferredDocProjPath", inputInferredDocProjDirPath)
                .addArg("-inputDocProjConfidenceThreshold", String.valueOf(inputDocProjConfidenceThreshold))
                .addArg("-numberOfEmittedFiles", "1")
                .addArg("-inputAvroProjOrgPath", inputProjOrgDirPath)
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-outputAvroReportPath", outputReportPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
        
        executor.execute(sparkJob);
    }
    
    private void readResultsAndPrintQualityRate(List<String> expectedResultsJsonPaths) throws IOException {
        
        List<MatchedOrganization> actualMatches = readLocalAvroDataStore(outputDirPath);
        List<MatchedOrganization> expectedMatches = readExpectedResultsFromJsons(expectedResultsJsonPaths);
        
        printQualityFactor("All matches", actualMatches.size(), expectedMatches.size());
        printCorrectMatchesFactor(expectedMatches, actualMatches);
        printFalsePositivesFactor(expectedMatches, actualMatches);

    }
    
    private void printCorrectMatchesFactor(List<MatchedOrganization> expectedMatches, List<MatchedOrganization> actualMatches) {
        
        List<MatchedOrganization> correctMatches = expectedMatches.stream()
                .filter(x -> containsMatchIgnoreMatchStrength(actualMatches, x))
                .collect(toList());
        
        printQualityFactor("Correct matches", correctMatches.size(), actualMatches.size());
        
    }
    
    private void printFalsePositivesFactor(List<MatchedOrganization> expectedMatches, List<MatchedOrganization> actualMatches) {
        
        List<MatchedOrganization> falsePositives = actualMatches.stream()
                .filter(x -> !containsMatchIgnoreMatchStrength(expectedMatches, x))
                .collect(toList());
        
        printQualityFactor("False positives", falsePositives.size(), actualMatches.size());
        
    }
    
    private boolean containsMatchIgnoreMatchStrength(List<MatchedOrganization> matches, MatchedOrganization match) {
        
        return matches.stream()
                .filter(x -> matchEqualsIgnoreMatchStrength(x, match))
                .findAny()
                .isPresent();
    }
    
    private boolean matchEqualsIgnoreMatchStrength(MatchedOrganization match1, MatchedOrganization match2) {
        
        String match1DocId = match1.getDocumentId().toString();
        String match1OrgId = match1.getOrganizationId().toString();
        
        String match2DocId = match2.getDocumentId().toString();
        String match2OrgId = match2.getOrganizationId().toString();
        
        if (!match1DocId.equals(match2DocId)) {
            return false;
        }
        return match1OrgId.equals(match2OrgId);
    }
    
    private void printQualityFactor(String factorName, int goodCount, int totalCount) {
        
        double factorPercentage = ((double)goodCount/totalCount)*100;
        
        String text = String.format("%-20s %5.2f%% (%d/%d)", factorName + ":", factorPercentage, goodCount, totalCount);
        logger.trace(text);
        
        
    }
    
    private void createInputDataFromJsonFiles(List<String> jsonInputOrgPaths, List<String> jsonInputAffPaths, List<String> jsonInputDocProjPaths, List<String> jsonInputInferredDocProjPaths, List<String> jsonInputProjOrgPaths) throws IOException {
        
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputOrgPaths, Organization.class), inputOrgDirPath, Organization.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputAffPaths, ExtractedDocumentMetadata.class), inputAffDirPath, ExtractedDocumentMetadata.class);
        
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputDocProjPaths, eu.dnetlib.iis.importer.schemas.DocumentToProject.class), inputDocProjDirPath, eu.dnetlib.iis.importer.schemas.DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputInferredDocProjPaths, DocumentToProject.class), inputInferredDocProjDirPath, DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputProjOrgPaths, ProjectToOrganization.class), inputProjOrgDirPath, ProjectToOrganization.class);
        
    }
    
    
    private List<MatchedOrganization> readExpectedResultsFromJsons(List<String> jsonOutputPaths) throws IOException {
        
        List<MatchedOrganization> expectedResults = newArrayList();
        List<SimpleAffMatchResult> simpleAffMatchResults = readMultipleJsons(jsonOutputPaths, SimpleAffMatchResult.class);
        
        for (SimpleAffMatchResult simpleResult : simpleAffMatchResults) {
            
            MatchedOrganization expectedResult = new MatchedOrganization(simpleResult.getDocumentId(), simpleResult.getOrganizationId(), 1f);
            
            
            if (!expectedResults.contains(expectedResult)) {
                expectedResults.add(expectedResult);
            }
        }
        
        return expectedResults;
    }

}
