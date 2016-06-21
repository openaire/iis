package eu.dnetlib.iis.wf.affmatching;

import static com.google.common.collect.ImmutableList.of;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.createLocalAvroDataStore;
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
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
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
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.AffMatchResultWriter;
import eu.dnetlib.iis.wf.affmatching.write.SimpleAffMatchResultWriter;
import scala.Tuple2;

/**
 * Affiliation matching module test that measures quality of matching.<br/>
 * Tests in this class use alternative {@link AffMatchResultWriter} which does
 * not loose information about matched affiliation position in document.<br/>
 * <br/>
 * Quality of matching is described by four factors:<br/>
 * <ul>
 * <li>All matches - percentage of all actual matches to all expected matches</li>
 * <li>All distinct aff matches - percentage of actual matches with distinct affiliations to expected matches with distinct affiliations</li>
 * <li>True positives - percentage of returned results that was matched correctly</li>
 * <li>False positives - percentage of returned results that was matched incorrectly (sums to 100% with true positives)</li>
 * <ul><br/>
 * 
 * @author madryk
 */
public class AffMatchingAffOrgQualityTest {

    private final static boolean PRINT_NOT_MATCHED = true;
    private final static boolean PRINT_FALSE_POSITIVE_MATCHES = true;
    
    private final static String INPUT_DATA_DIR_PATH = "src/test/resources/experimentalData/input";
    
    private AffMatchingService affMatchingService;
    
    private static JavaSparkContext sparkContext;
    
    
    private File workingDir;
    
    private String inputOrgDirPath;
    
    private String inputAffDirPath;
    
    private String inputDocProjDirPath;
    
    private String inputInferredDocProjDirPath;
    
    private float inputDocProjConfidenceThreshold = 0.8f;
    
    private String inputProjOrgDirPath;
    
    private String outputDirPath;
    
    
    @BeforeClass
    public static void classSetup() {
        
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(AffMatchingAffOrgQualityTest.class.getName());
        
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
        inputInferredDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj_inferred";
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
    public void matchAffiliations_combined_data() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                of(INPUT_DATA_DIR_PATH + "/all_organizations.json"),
                of(INPUT_DATA_DIR_PATH + "/docs_with_aff_real_data.json"),
                of(INPUT_DATA_DIR_PATH + "/doc_project.json"),
                of(),
                of(INPUT_DATA_DIR_PATH + "/org_project.json"));
        
        
        // execute
        
        affMatchingService.matchAffiliations(sparkContext, inputAffDirPath, inputOrgDirPath, outputDirPath);
        
        
        // log
        
        System.out.println("\nALL TEST DATA");
        
        readResultsAndPrintQualityRate(
                of("src/test/resources/experimentalData/expectedOutput/matched_aff.json"));
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void createInputDataFromJsonFiles(List<String> jsonInputOrgPaths, List<String> jsonInputAffPaths, List<String> jsonInputDocProjPaths, List<String> jsonInputInferredDocProjPaths, List<String> jsonInputProjOrgPaths) throws IOException {
        
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputOrgPaths, Organization.class), inputOrgDirPath, Organization.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputAffPaths, ExtractedDocumentMetadata.class), inputAffDirPath, ExtractedDocumentMetadata.class);
        
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputDocProjPaths, eu.dnetlib.iis.importer.schemas.DocumentToProject.class), inputDocProjDirPath, eu.dnetlib.iis.importer.schemas.DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputInferredDocProjPaths, DocumentToProject.class), inputInferredDocProjDirPath, DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputProjOrgPaths, ProjectToOrganization.class), inputProjOrgDirPath, ProjectToOrganization.class);
        
    }
    
    private void readResultsAndPrintQualityRate(List<String> expectedResultsJsonPaths) throws IOException {
        
        List<SimpleAffMatchResult> actualMatches = readJson(outputDirPath + "/part-00000", SimpleAffMatchResult.class);
        List<SimpleAffMatchResult> expectedMatches = readMultipleJsons(expectedResultsJsonPaths, SimpleAffMatchResult.class);
        
        
        printTruePositivesFactor(expectedMatches, actualMatches);
        printFalsePositivesFactor(expectedMatches, actualMatches);
        if (PRINT_FALSE_POSITIVE_MATCHES) {
            AffMatchingResultPrinter.printFalsePositives(inputAffDirPath, inputOrgDirPath, expectedMatches, actualMatches);

        }
        if (PRINT_NOT_MATCHED) {
            AffMatchingResultPrinter.printNotMatched(inputAffDirPath, inputOrgDirPath, expectedMatches, actualMatches);
        }
    }
    
    private void printTruePositivesFactor(List<SimpleAffMatchResult> expectedMatches, List<SimpleAffMatchResult> actualMatches) {
        
        List<SimpleAffMatchResult> truePositives = actualMatches.stream()
                .filter(x -> expectedMatches.contains(x))
                .collect(toList());
        
        int distinctAffActualMatchesCount = actualMatches.stream()
                .collect(Collectors.groupingBy(x -> new Tuple2<>(x.getDocumentId(), x.getAffiliationPosition())))
                .size();
        
        int distinctAffExpectedMatchesCount = expectedMatches.stream()
                .collect(Collectors.groupingBy(x -> new Tuple2<>(x.getDocumentId(), x.getAffiliationPosition())))
                .size();
        
        printQualityFactor("All matches", actualMatches.size(), expectedMatches.size());
        printQualityFactor("All distinct aff matches", distinctAffActualMatchesCount, distinctAffExpectedMatchesCount);
        printQualityFactor("Correct matches", truePositives.size(), actualMatches.size());
        
    }
    
    private void printFalsePositivesFactor(List<SimpleAffMatchResult> expectedMatches, List<SimpleAffMatchResult> actualMatches) {
        
        List<SimpleAffMatchResult> falsePositives = actualMatches.stream()
                .filter(x -> !expectedMatches.contains(x))
                .collect(toList());
        
        printQualityFactor("False positives", falsePositives.size(), actualMatches.size());
        
    }
    
    private void printQualityFactor(String factorName, int goodCount, int totalCount) {
        
        double factorPercentage = ((double)goodCount/totalCount)*100;
        
        String text = String.format("%-30s %5.2f%% (%d/%d)", factorName + ":", factorPercentage, goodCount, totalCount);
        System.out.println(text);
        
        
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
                createDocOrgRelationMatcher(sparkContext, inputDocProjDirPath, inputInferredDocProjDirPath, inputProjOrgDirPath, inputDocProjConfidenceThreshold);
        
        AffOrgMatcher mainSectionHashBucketMatcher = createMainSectionHashBucketMatcher();
        
        AffOrgMatcher firstWordsHashBucketMatcher = createFirstWordsHashBucketMatcher();
        
        
        
        affMatchingService.setAffOrgMatchers(of(docOrgRelationMatcher, mainSectionHashBucketMatcher, firstWordsHashBucketMatcher));
        
        return affMatchingService;
    }
}
