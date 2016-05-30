package eu.dnetlib.iis.wf.affmatching;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.createLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.readLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.JsonAvroTestUtils.readMultipleJsonDataStores;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readMultipleJsons;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * Affiliation matching module test that measures quality of matching.<br/>
 * Tests in this class completely ignores {@link MatchedOrganization#getMatchStrength()}<br/>
 * <br/>
 * Quality of matching is described by two factors:<br/>
 * <ul>
 * <li>Correct matches - percentage of expected matches that are also present in returned results</li>
 * <li>False positives - percentage of returned matches that was matched, but incorrectly (are not present in expected matches)</li>
 * <ul><br/>
 * 
 * @author madryk
 */
public class AffMatchingQualityTest {
    
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String inputOrgDirPath;
    
    private String inputAffDirPath;
    
    private String inputDocProjDirPath;
    
    private float inputDocProjConfidenceThreshold = 0.8f;
    
    private String inputProjOrgDirPath;
    
    private String outputDirPath;
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        
        inputOrgDirPath = workingDir + "/affiliation_matching/input/organizations";
        inputAffDirPath = workingDir + "/affiliation_matching/input/affiliations";
        inputDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj";
        inputProjOrgDirPath = workingDir + "/affiliation_matching/input/proj_org";
        outputDirPath = workingDir + "/affiliation_matching/output";
        
        
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void affiliationMatchingJob_random_docs() throws IOException {
        
        
        // given
        
        createInputDataFromJsonFiles(
                of("src/test/resources/experimentalData/input/all_organizations.json"),
                of("src/test/resources/experimentalData/input/set1/docs_with_aff_real_data.json"),
                of(), of());
        
        
        // execute
        
        executeJob();
        
        
        // log
        
        System.out.println("\nRANDOM DOCUMENTS");
        
        readResultsAndPrintQualityRate(of("src/test/resources/experimentalData/expectedOutput/set1/matched_aff.json"));
    }
    
    @Test
    public void affiliationMatchingJob_docs_assigned_to_project() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                of("src/test/resources/experimentalData/input/all_organizations.json"),
                of("src/test/resources/experimentalData/input/set2/docs_with_aff_real_data.json"),
                of(), of());
        
        
        // execute
        
        executeJob();
        
        
        // log
        
        System.out.println("\nDOCUMENTS ASSIGNED TO PROJECT");
        
        readResultsAndPrintQualityRate(of("src/test/resources/experimentalData/expectedOutput/set2/matched_aff.json"));
    }
    
    @Test
    public void affiliationMatchingJob_combined_data() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                of(
                        "src/test/resources/experimentalData/input/all_organizations.json"),
                of(
                        "src/test/resources/experimentalData/input/set1/docs_with_aff_real_data.json",
                        "src/test/resources/experimentalData/input/set2/docs_with_aff_real_data.json"),
                of(), of());
        
        
        // execute
        
        executeJob();
        
        
        // log
        
        System.out.println("\nALL TEST DATA");
        
        readResultsAndPrintQualityRate(of(
                "src/test/resources/experimentalData/expectedOutput/set1/matched_aff.json",
                "src/test/resources/experimentalData/expectedOutput/set2/matched_aff.json"));
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
                .addArg("-inputDocProjConfidenceThreshold", String.valueOf(inputDocProjConfidenceThreshold))
                .addArg("-inputAvroProjOrgPath", inputProjOrgDirPath)
                .addArg("-outputAvroPath", outputDirPath)
                .build();
        
        executor.execute(sparkJob);
    }
    
    private void readResultsAndPrintQualityRate(List<String> expectedResultsJsonPaths) throws IOException {
        
        List<MatchedOrganization> actualMatches = readLocalAvroDataStore(outputDirPath);
        List<MatchedOrganization> expectedMatches = readExpectedResultsFromJsons(expectedResultsJsonPaths);
        
        printCorrectMatchesFactor(expectedMatches, actualMatches);
        printFalsePositivesFactor(expectedMatches, actualMatches);

    }
    
    private void printCorrectMatchesFactor(List<MatchedOrganization> expectedMatches, List<MatchedOrganization> actualMatches) {
        
        List<MatchedOrganization> correctMatches = expectedMatches.stream()
                .filter(x -> containsMatchIgnoreMatchStrength(actualMatches, x))
                .collect(toList());
        
        printQualityFactor("Correct matches", correctMatches.size(), expectedMatches.size());
        
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
        System.out.println(text);
        
        
    }
    
    private void createInputDataFromJsonFiles(List<String> jsonInputOrgPaths, List<String> jsonInputAffPaths, List<String> jsonInputDocProjPaths, List<String> jsonInputProjOrgPaths) throws IOException {

        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputOrgPaths, Organization.class), inputOrgDirPath, Organization.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputAffPaths, ExtractedDocumentMetadata.class), inputAffDirPath, ExtractedDocumentMetadata.class);
        
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputDocProjPaths, DocumentToProject.class), inputDocProjDirPath, DocumentToProject.class);
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
