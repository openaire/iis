package eu.dnetlib.iis.wf.affmatching.match.voter;

import static eu.dnetlib.iis.common.utils.AvroTestUtils.createLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.JsonAvroTestUtils.readMultipleJsonDataStores;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readJson;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readMultipleJsons;
import static eu.dnetlib.iis.wf.affmatching.AffMatchingResultPrinter.printFalsePositives;
import static eu.dnetlib.iis.wf.affmatching.AffMatchingResultPrinter.printNotMatched;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createDocOrgRelationMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createDocOrgRelationMatcherVoters;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createFirstWordsHashBucketMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createFirstWordsHashBucketMatcherVoters;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createMainSectionHashBucketMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcherFactory.createMainSectionHashBucketMatcherVoters;
import static java.lang.System.out;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.AffMatchingService;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchComputer;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.SimpleAffMatchResultWriter;

/**
 * An {@link AffOrgMatchVoter} match strength estimator. <br/> 
 * Each test method of this class gives the match strength of every voter connected to
 * a given {@link AffOrgMatcher}. Thus, the match strength of a voter is not absolute but relative
 * (to the given matcher). <br/>
 * The match strength depends on real data prepared by hand and is just a ratio of true positives (correct matches)
 * to all the matches guessed by the given matcher and voter.
 */
public class AffOrgMatchVoterStrengthEstimator {

    private final static boolean PRINT_NOT_MATCHED = false;
    
    private final static boolean PRINT_FALSE_POSITIVES = false;
    
    private final static boolean PRINT_NUMBER_DETAILS = true;
    
    
    
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
    public static void classSetup() throws IllegalAccessException, InstantiationException {
        
        SparkConf conf = new SparkConf();
    
        conf.setMaster("local");
        conf.setAppName(AffOrgMatchVoterStrengthEstimator.class.getName());
        
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        sparkContext = new JavaSparkContext(conf);
        
        
    }
    
    @Before
    public void setup() throws IOException {
        
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
    public void estimateVoterMatchStrengths_for_DocOrgRelationMatcher() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(
                ImmutableList.of("src/test/resources/experimentalData/input/all_organizations.json"),
                ImmutableList.of("src/test/resources/experimentalData/input/set4/docs_with_aff_real_data.json"),
                ImmutableList.of("src/test/resources/experimentalData/input/set4/doc_project.json"), 
                ImmutableList.of(),
                ImmutableList.of("src/test/resources/experimentalData/input/set4/org_project.json"));

        
        AffOrgMatcher affOrgMatcher = createDocOrgRelationMatcher(sparkContext, inputDocProjDirPath, inputInferredDocProjDirPath, inputProjOrgDirPath, inputDocProjConfidenceThreshold);
        
        List<AffOrgMatchVoter> voters = createDocOrgRelationMatcherVoters();
        
        // execute
        
        estimateVoterMatchStrengths(affOrgMatcher, "Doc-Org Relation Matcher", voters,
                                    ImmutableList.of("src/test/resources/experimentalData/expectedOutput/set4/matched_aff.json"));

        
    }
    
    
    
    @Test
    public void estimateVoterMatchStrengths_for_MainSectionHashBucketMatcher() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(ImmutableList.of("src/test/resources/experimentalData/input/all_organizations.json"),
                                     ImmutableList.of("src/test/resources/experimentalData/input/set1/docs_with_aff_real_data.json",
                                                      "src/test/resources/experimentalData/input/set2/docs_with_aff_real_data.json"),
                                     ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

        
        AffOrgMatcher affOrgMatcher = createMainSectionHashBucketMatcher();
        
        List<AffOrgMatchVoter> voters = createMainSectionHashBucketMatcherVoters();
        
        
        // execute
        
        estimateVoterMatchStrengths(affOrgMatcher, "Main Section Hash Bucket Matcher", voters,
                                    ImmutableList.of("src/test/resources/experimentalData/expectedOutput/set1/matched_aff.json",
                                                     "src/test/resources/experimentalData/expectedOutput/set2/matched_aff.json"));

        
    }
    
    
    @Test
    public void estimateVoterMatchStrengths_for_FirstWordsHashBucketMatcher() throws IOException {
        
        // given
        
        createInputDataFromJsonFiles(ImmutableList.of("src/test/resources/experimentalData/input/all_organizations.json"),
                                     ImmutableList.of("src/test/resources/experimentalData/input/set1/docs_with_aff_real_data.json",
                                                      "src/test/resources/experimentalData/input/set2/docs_with_aff_real_data.json"),
                                     ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

        AffOrgMatcher affOrgMatcher = createFirstWordsHashBucketMatcher();
        
        List<AffOrgMatchVoter> voters = createFirstWordsHashBucketMatcherVoters();
        
        
        // execute
        
        estimateVoterMatchStrengths(affOrgMatcher, "First Words Hash Bucket Matcher", voters, 
                                    ImmutableList.of("src/test/resources/experimentalData/expectedOutput/set1/matched_aff.json",
                                                     "src/test/resources/experimentalData/expectedOutput/set2/matched_aff.json"));
        
    }
    

    
    //------------------------ PRIVATE --------------------------

    
    private void estimateVoterMatchStrengths(AffOrgMatcher affOrgMatcher, String affOrgMatcherName, List<AffOrgMatchVoter> voters, List<String> matchedAffPaths) throws IOException {
        
        printMatcherHeader(affOrgMatcherName);
        
        
        // given
        
        affMatchingService.setAffOrgMatchers(ImmutableList.of(affOrgMatcher));
        
        AffOrgMatchComputer affOrgMatchComputer = new AffOrgMatchComputer();
        
        
        for (AffOrgMatchVoter voter : voters) {
            
            printVoterHeader(voter);
            
            affOrgMatchComputer.setAffOrgMatchVoters(Lists.newArrayList(voter));
            affOrgMatcher.setAffOrgMatchComputer(affOrgMatchComputer);
        
        
            
            // execute
            
            affMatchingService.matchAffiliations(sparkContext, inputAffDirPath, inputOrgDirPath, outputDirPath);
            
            
            // log
            
            calcAndPrintResult(matchedAffPaths);
            
            
            FileUtils.deleteDirectory(new File(outputDirPath));
        
        }
        
        out.println("\n\n");
        
    }

    
    private void printVoterHeader(AffOrgMatchVoter voter) {
        out.println("\n\n");
        out.println("---------------------------------- VOTER ----------------------------------------");
        out.println(voter.toString() + "\n");
    }

    
    private void printMatcherHeader(String affOrgMatcherName) {
        out.println("\n\n==================================================================================");
        out.println("========================= " + affOrgMatcherName + " ===========================");
        out.println("==================================================================================");
    }
    
    
    private void createInputDataFromJsonFiles(List<String> jsonInputOrgPaths, List<String> jsonInputAffPaths, List<String> jsonInputDocProjPaths, List<String> jsonInputInferredDocProjPaths, List<String> jsonInputProjOrgPaths) throws IOException {

        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputOrgPaths, Organization.class), inputOrgDirPath, Organization.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputAffPaths, ExtractedDocumentMetadata.class), inputAffDirPath, ExtractedDocumentMetadata.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputDocProjPaths, eu.dnetlib.iis.importer.schemas.DocumentToProject.class), inputDocProjDirPath, eu.dnetlib.iis.importer.schemas.DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputInferredDocProjPaths, DocumentToProject.class), inputInferredDocProjDirPath, DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputProjOrgPaths, ProjectToOrganization.class), inputProjOrgDirPath, ProjectToOrganization.class);
    
    }
    

    private void calcAndPrintResult(List<String> expectedResultsJsonPaths) throws IOException {
        
        List<SimpleAffMatchResult> actualMatches = readJson(outputDirPath + "/part-00000", SimpleAffMatchResult.class);
        List<SimpleAffMatchResult> expectedMatches = readMultipleJsons(expectedResultsJsonPaths, SimpleAffMatchResult.class);
        
        List<SimpleAffMatchResult> correctMatches = actualMatches.stream().filter(x -> expectedMatches.contains(x)).collect(toList());
        List<SimpleAffMatchResult> falsePositives = actualMatches.stream().filter(x -> !expectedMatches.contains(x)).collect(toList());
        
        calcAndPrintMatchStrength(actualMatches.size(), correctMatches.size());
        
        if (PRINT_NUMBER_DETAILS) {
            printNumberDetails(expectedMatches.size(), actualMatches.size(), correctMatches.size(), falsePositives.size());
        }
        
        out.println();
        
        if (PRINT_FALSE_POSITIVES) {
            printFalsePositives(inputAffDirPath, inputOrgDirPath, expectedMatches, actualMatches);
        }
        
        if (PRINT_NOT_MATCHED) {
            printNotMatched(inputAffDirPath, inputOrgDirPath, expectedMatches, actualMatches);
        }
    }

    
    private void calcAndPrintMatchStrength(int numberOfActualMatches, int numberOfCorrectMatches) {
        double matchStrength = ((double)numberOfCorrectMatches)/numberOfActualMatches;
        out.println("");
        out.printf("%s %1.3f", "MATCH STRENGTH: ", matchStrength);
    }


    private void printNumberDetails(int numberOfExpectedMatches, int numberOfActualMatches, int numberOfCorrectMatches, int numberOfFalsePositives) {
        out.print("  [");
        printQualityFactor("All matches", numberOfActualMatches, numberOfExpectedMatches);
        out.print(", ");
        printQualityFactor("Correct matches", numberOfCorrectMatches, numberOfActualMatches);
        out.print(", ");
        printQualityFactor("False positives", numberOfFalsePositives, numberOfActualMatches);
        out.print("]");
    }
    
    

    private void printQualityFactor(String factorName, int goodCount, int totalCount) {
        
        double factorPercentage = ((double)goodCount/totalCount)*100;
        
        String text = String.format("%s %3.2f%% (%d/%d)", factorName + ":", factorPercentage, goodCount, totalCount);
        
        System.out.print(text);
        
    }
    
    
    private AffMatchingService createAffMatchingService() {
        
        AffMatchingService affMatchingService = new AffMatchingService();
        
        
        // readers
        
        affMatchingService.setAffiliationReader(new IisAffiliationReader());
        affMatchingService.setOrganizationReader(new IisOrganizationReader());
        
        
        // writer
        
        affMatchingService.setAffMatchResultWriter(new SimpleAffMatchResultWriter());
        
        
        return affMatchingService;
    }
}
