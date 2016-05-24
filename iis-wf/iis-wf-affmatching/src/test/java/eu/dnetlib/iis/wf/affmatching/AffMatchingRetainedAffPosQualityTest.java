package eu.dnetlib.iis.wf.affmatching;

import static com.google.common.collect.ImmutableList.of;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.createLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.readLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.JsonAvroTestUtils.readMultipleJsonDataStores;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readJson;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readMultipleJsons;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createDocOrgRelationMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createFirstWordsHashBucketMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createMainSectionHashBucketMatcher;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Files;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.AffMatchResultWriter;
import eu.dnetlib.iis.wf.affmatching.write.SimpleAffMatchResultWriter;

/**
 * Affiliation matching module test that measures quality of matching.<br/>
 * Tests in this class use alternative {@link AffMatchResultWriter} which does
 * not loose information about matched affiliation position in document.<br/>
 * <br/>
 * Quality of matching is described by three factors:<br/>
 * <ul>
 * <li>Correct matches - percentage of correctly matched results among all expected matches</li>
 * <li>True positives - percentage of returned results that was matched correctly</li>
 * <li>False positives - percentage of returned results that was matched incorrectly (sums to 100% with true positives)</li>
 * <ul><br/>
 * 
 * @author madryk
 */
public class AffMatchingRetainedAffPosQualityTest {

    private final static boolean PRINT_NOT_MATCHED = false;
    private final static boolean PRINT_FALSE_POSITIVE_MATCHES = false;
    
    private AffMatchingService affMatchingService;
    
    private static JavaSparkContext sparkContext;
    
    
    private File workingDir;
    
    private String inputOrgDirPath;
    
    private String inputAffDirPath;
    
    private String inputDocProjDirPath;
    
    private float inputDocProjConfidenceThreshold = 0.8f;
    
    private String inputProjOrgDirPath;
    
    private String outputDirPath;
    
    
    @BeforeClass
    public static void classSetup() {
        
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(AffMatchingRetainedAffPosQualityTest.class.getName());
        
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        sparkContext = new JavaSparkContext(conf);
    }
    
    @Before
    public void setup() {
        
        workingDir = Files.createTempDir();
        
        inputOrgDirPath = workingDir + "/affiliation_matching/input/organizations";
        inputAffDirPath = workingDir + "/affiliation_matching/input/affiliations";
        inputDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj";
        inputProjOrgDirPath = workingDir + "/affiliation_matching/input/proj_org";
        outputDirPath = workingDir + "/affiliation_matching/output";
        
        affMatchingService = createAffMatchingService();
    }
    
    @After
    public void cleanup() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
    }
    
    @AfterClass
    public static void classCleanup() throws IOException {
        
        if (sparkContext != null) {
            sparkContext.close();
        }
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void matchAffiliations_random_docs() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                of("src/test/resources/experimentalData/input/all_organizations.json"),
                of("src/test/resources/experimentalData/input/set1/docs_with_aff_real_data.json"),
                of(), of());
        
        
        // execute
        
        affMatchingService.matchAffiliations(sparkContext, inputAffDirPath, inputOrgDirPath, outputDirPath);
        
        
        // print results quality
        
        System.out.println("\nRANDOM DOCUMENTS");
        
        readResultsAndPrintQualityRate(of("src/test/resources/experimentalData/expectedOutput/set1/matched_aff.json"));
    }
    
    
    @Test
    public void matchAffiliations_docs_assigned_to_project() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                of("src/test/resources/experimentalData/input/all_organizations.json"),
                of("src/test/resources/experimentalData/input/set2/docs_with_aff_real_data.json"),
                of(), of());
        
        
        // execute
        
        affMatchingService.matchAffiliations(sparkContext, inputAffDirPath, inputOrgDirPath, outputDirPath);
        
        
        // print results quality
        
        System.out.println("\nDOCUMENTS ASSIGNED TO PROJECT");
        
        readResultsAndPrintQualityRate(of("src/test/resources/experimentalData/expectedOutput/set2/matched_aff.json"));
    }
    
    
    @Test
    public void matchAffiliations_combined_data() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                of("src/test/resources/experimentalData/input/all_organizations.json"),
                of(
                        "src/test/resources/experimentalData/input/set1/docs_with_aff_real_data.json",
                        "src/test/resources/experimentalData/input/set2/docs_with_aff_real_data.json"),
                of(), of());
        
        
        // execute
        
        affMatchingService.matchAffiliations(sparkContext, inputAffDirPath, inputOrgDirPath, outputDirPath);
        
        
        // print results quality
        
        System.out.println("\nALL TEST DATA");
        
        readResultsAndPrintQualityRate(
                of(
                        "src/test/resources/experimentalData/expectedOutput/set1/matched_aff.json",
                        "src/test/resources/experimentalData/expectedOutput/set2/matched_aff.json"));
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void createInputDataFromJsonFiles(List<String> jsonInputOrgPaths, List<String> jsonInputAffPaths, List<String> jsonInputDocProjPaths, List<String> jsonInputProjOrgPaths) throws IOException {
        
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputOrgPaths, Organization.class), inputOrgDirPath, Organization.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputAffPaths, ExtractedDocumentMetadata.class), inputAffDirPath, ExtractedDocumentMetadata.class);
        
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputDocProjPaths, DocumentToProject.class), inputDocProjDirPath, DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputProjOrgPaths, ProjectToOrganization.class), inputProjOrgDirPath, ProjectToOrganization.class);
        
    }
    
    private void readResultsAndPrintQualityRate(List<String> expectedResultsJsonPaths) throws IOException {
        
        List<SimpleAffMatchResult> actualMatches = readJson(outputDirPath + "/part-00000", SimpleAffMatchResult.class);
        List<SimpleAffMatchResult> expectedMatches = readMultipleJsons(expectedResultsJsonPaths, SimpleAffMatchResult.class);
        
        
        printTruePositivesFactor(expectedMatches, actualMatches);
        printFalsePositivesFactor(expectedMatches, actualMatches);
        if (PRINT_FALSE_POSITIVE_MATCHES) {
            printFalsePositives(expectedMatches, actualMatches);
        }
        if (PRINT_NOT_MATCHED) {
            printNotMatched(expectedMatches, actualMatches);
        }
    }
    
    private void printTruePositivesFactor(List<SimpleAffMatchResult> expectedMatches, List<SimpleAffMatchResult> actualMatches) {
        
        List<SimpleAffMatchResult> truePositives = actualMatches.stream()
                .filter(x -> expectedMatches.contains(x))
                .collect(toList());
        
        printQualityFactor("Correct matches", truePositives.size(), expectedMatches.size());
        printQualityFactor("True positives", truePositives.size(), actualMatches.size());
        
    }
    
    private void printFalsePositivesFactor(List<SimpleAffMatchResult> expectedMatches, List<SimpleAffMatchResult> actualMatches) {
        
        List<SimpleAffMatchResult> falsePositives = actualMatches.stream()
                .filter(x -> !expectedMatches.contains(x))
                .collect(toList());
        
        printQualityFactor("False positives", falsePositives.size(), actualMatches.size());
        
    }
    
    private void printQualityFactor(String factorName, int goodCount, int totalCount) {
        
        double factorPercentage = ((double)goodCount/totalCount)*100;
        
        String text = String.format("%-20s %5.2f%% (%d/%d)", factorName + ":", factorPercentage, goodCount, totalCount);
        System.out.println(text);
        
        
    }
    
    private void printNotMatched(List<SimpleAffMatchResult> expectedMatches, List<SimpleAffMatchResult> actualMatches) throws IOException {
        List<ExtractedDocumentMetadata> docsAffiliations = readLocalAvroDataStore(inputAffDirPath);
        List<Organization> organizations = readLocalAvroDataStore(inputOrgDirPath);
        
        List<SimpleAffMatchResult> notMatched = expectedMatches.stream()
                .filter(x -> fetchMatchedOrganizationId(actualMatches, x.getDocumentId(), x.getAffiliationPosition()) == null)
                .collect(toList());
        
        System.out.println("\n\tnot matched");
        for (SimpleAffMatchResult match : notMatched) {
            
            Affiliation affiliation = fetchAffiliation(docsAffiliations, match.getDocumentId(), match.getAffiliationPosition());
            
            String expectedOrgId = fetchMatchedOrganizationId(expectedMatches, match.getDocumentId(), match.getAffiliationPosition());
            Organization expectedOrg = fetchOrganization(organizations, expectedOrgId);
            
            System.out.println("Affiliation:     " + affiliation);
            System.out.println("Should match to: " + expectedOrg);
            System.out.println();
        }
        
    }
    
    private void printFalsePositives(List<SimpleAffMatchResult> expectedMatches, List<SimpleAffMatchResult> actualMatches) throws IOException {
        List<ExtractedDocumentMetadata> docsAffiliations = readLocalAvroDataStore(inputAffDirPath);
        List<Organization> organizations = readLocalAvroDataStore(inputOrgDirPath);
        
        List<SimpleAffMatchResult> falsePositives = actualMatches.stream()
                .filter(x -> !expectedMatches.contains(x))
                .collect(toList());
        
        System.out.println("\n\tfalse positives");
        for (SimpleAffMatchResult falsePositive : falsePositives) {
            
            Affiliation affiliation = fetchAffiliation(docsAffiliations, falsePositive.getDocumentId(), falsePositive.getAffiliationPosition());
            
            String expectedOrgId = fetchMatchedOrganizationId(expectedMatches, falsePositive.getDocumentId(), falsePositive.getAffiliationPosition());
            Organization expectedOrg = expectedOrgId == null ? null : fetchOrganization(organizations, expectedOrgId);
            Organization actualOrg = fetchOrganization(organizations, falsePositive.getOrganizationId());
            
            
            System.out.println("Affiliation:     " + affiliation);
            System.out.println("Was matched to:  " + actualOrg);
            System.out.println("Should match to: " + expectedOrg);
            System.out.println();
        }
        
    }
    
    private String fetchMatchedOrganizationId(List<SimpleAffMatchResult> matches, String documentId, int pos) {
        return matches.stream()
                .filter(match -> StringUtils.equals(match.getDocumentId(), documentId) && match.getAffiliationPosition() == pos)
                .map(match -> match.getOrganizationId())
                .findFirst().orElse(null);
    }
    
    private Affiliation fetchAffiliation(List<ExtractedDocumentMetadata> docsWithAffs, String documentId, int affPosition) {
        ExtractedDocumentMetadata doc = docsWithAffs.stream().filter(x -> StringUtils.equals(x.getId(), documentId)).findFirst().get();
        return doc.getAffiliations().get(affPosition - 1);
        
    }
    
    private Organization fetchOrganization(List<Organization> organizations, String organizationId) {
        return organizations.stream().filter(x -> StringUtils.equals(x.getId().toString(), organizationId.toString())).findFirst().get();
        
    }
    
    private AffMatchingService createAffMatchingService() {
        
        AffMatchingService affMatchingService = new AffMatchingService();
        
        
        // readers
        
        affMatchingService.setAffiliationReader(new IisAffiliationReader());
        affMatchingService.setOrganizationReader(new IisOrganizationReader());
        
        
        // writer
        
        affMatchingService.setAffMatchResultWriter(new SimpleAffMatchResultWriter());
        
        
        // matchers
        
        AffOrgMatcher docOrgRelationMatcher = 
                createDocOrgRelationMatcher(sparkContext, inputDocProjDirPath, inputProjOrgDirPath, inputDocProjConfidenceThreshold);
        
        AffOrgMatcher mainSectionHashBucketMatcher = createMainSectionHashBucketMatcher();
        
        AffOrgMatcher firstWordsHashBucketMatcher = createFirstWordsHashBucketMatcher();
        
        
        
        affMatchingService.setAffOrgMatchers(of(docOrgRelationMatcher, mainSectionHashBucketMatcher, firstWordsHashBucketMatcher));
        
        return affMatchingService;
    }
}
