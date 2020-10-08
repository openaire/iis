package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.AffMatchingService;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatchComputer;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.AffMatchOrganizationAltNameFiller;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.CsvOrganizationAltNamesDictionaryFactory;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.OrganizationAltNameConst;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.SimpleAffMatchResultWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.of;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.createLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.JsonAvroTestUtils.readMultipleJsonDataStores;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readJson;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readMultipleJsons;
import static eu.dnetlib.iis.wf.affmatching.AffMatchingResultPrinter.printFalsePositives;
import static eu.dnetlib.iis.wf.affmatching.AffMatchingResultPrinter.printNotMatched;
import static eu.dnetlib.iis.wf.affmatching.match.DocOrgRelationMatcherFactory.createDocOrgRelationMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.DocOrgRelationMatcherFactory.createDocOrgRelationMatcherVoters;
import static eu.dnetlib.iis.wf.affmatching.match.FirstWordsHashBucketMatcherFactory.createNameFirstWordsHashBucketMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.FirstWordsHashBucketMatcherFactory.createNameFirstWordsHashBucketMatcherVoters;
import static eu.dnetlib.iis.wf.affmatching.match.MainSectionHashBucketMatcherFactory.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * An {@link AffOrgMatchVoter} match strength estimator. <br/>
 * Each test method of this class gives the match strength of every voter connected to
 * a given {@link AffOrgMatcher}. Thus, the match strength of a voter is not absolute but relative
 * (to the given matcher). <br/>
 * The match strength depends on real data prepared by hand and is just a ratio of true positives (correct matches)
 * to all the matches guessed by the given matcher and voter.
 */
@IntegrationTest
public class AffOrgMatchVoterStrengthEstimatorAndTest {

    private static final Logger logger = LoggerFactory.getLogger(AffOrgMatchVoterStrengthEstimatorAndTest.class);

    private final static boolean PRINT_NOT_MATCHED = false;

    private final static boolean PRINT_FALSE_POSITIVES = true;

    private final static boolean PRINT_NUMBER_DETAILS = true;

    private final static String INPUT_DATA_DIR_PATH = "src/test/resources/experimentalData/input";

    private final static int VOTER_MATCH_STRENGTH_SCALE = 3; // number of decimal places

    private List<InvalidVoterStrength> invalidVoterStrengths = Lists.newArrayList();

    private AffMatchingService affMatchingService;

    private static JavaSparkContext sparkContext;

    @TempDir
    public File workingDir;

    private String inputOrgDirPath;

    private String inputAffDirPath;

    private String inputDocProjDirPath;

    private String inputInferredDocProjDirPath;

    private float inputDocProjConfidenceThreshold = 0.8f;

    private String inputProjOrgDirPath;

    private String outputDirPath;

    private String outputReportPath;

    @BeforeAll
    public static void classSetup() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(AffOrgMatchVoterStrengthEstimatorAndTest.class.getSimpleName());
        sparkContext = JavaSparkContextFactory.withConfAndKryo(conf);
    }

    @BeforeEach
    public void setup() throws IOException {
        inputOrgDirPath = workingDir + "/affiliation_matching/input/organizations";
        inputAffDirPath = workingDir + "/affiliation_matching/input/affiliations";
        inputDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj";
        inputInferredDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj_inferred";
        inputProjOrgDirPath = workingDir + "/affiliation_matching/input/proj_org";
        outputDirPath = workingDir + "/affiliation_matching/output";
        outputReportPath = workingDir + "/affiliation_matching/report";

        affMatchingService = createAffMatchingService();
    }

    @AfterAll
    public static void classCleanup() {
        if (sparkContext != null) {
            sparkContext.close();
        }
    }

    //------------------------ TESTS --------------------------

    @Test
    public void estimateAndCheckVoterStrengths() throws IOException {
        // execute
        estimateDocOrgRelationMatcherVoterStrengths();
        estimateNameMainSectionHashBucketMatcherVoterStrengths();
        estimateAlternativeNameMainSectionHashBucketMatcherVoterStrengths();
        estimateShortNameMainSectionHashBucketMatcherVoterStrengths();
        estimateFirstWordsHashBucketMatcherVoterStrengths();

        // assert
        if (CollectionUtils.isNotEmpty(invalidVoterStrengths)) {
            logger.trace("Invalid Voter Strengths. Change them manually to the calculated values (in the code):");
            invalidVoterStrengths.stream().map(InvalidVoterStrength::toString).forEach(logger::debug);
        }

        assertThat(invalidVoterStrengths, Matchers.emptyIterable());
    }

    //------------------------ PRIVATE --------------------------

    private void estimateDocOrgRelationMatcherVoterStrengths() throws IOException {
        // given
        createInputData();
        AffOrgMatcher affOrgMatcher = createDocOrgRelationMatcher(sparkContext, inputDocProjDirPath, inputInferredDocProjDirPath, inputProjOrgDirPath, inputDocProjConfidenceThreshold);
        List<AffOrgMatchVoter> voters = createDocOrgRelationMatcherVoters();

        // execute
        estimateVoterMatchStrengths(affOrgMatcher, "Doc-Org Relation Matcher", voters);
    }

    private void estimateNameMainSectionHashBucketMatcherVoterStrengths() throws IOException {
        // given
        createInputData();
        AffOrgMatcher affOrgMatcher = createNameMainSectionHashBucketMatcher();
        List<AffOrgMatchVoter> voters = createNameMainSectionHashBucketMatcherVoters();

        // execute
        estimateVoterMatchStrengths(affOrgMatcher, "Name Main Section Hash Bucket Matcher", voters);
    }

    private void estimateAlternativeNameMainSectionHashBucketMatcherVoterStrengths() throws IOException {
        // given
        createInputData();
        AffOrgMatcher affOrgMatcher = createAlternativeNameMainSectionHashBucketMatcher();
        List<AffOrgMatchVoter> voters = createAlternativeNameMainSectionHashBucketMatcherVoters();

        // execute
        estimateVoterMatchStrengths(affOrgMatcher, "Alternative Name Main Section Hash Bucket Matcher", voters);
    }

    private void estimateShortNameMainSectionHashBucketMatcherVoterStrengths() throws IOException {
        // given
        createInputData();
        AffOrgMatcher affOrgMatcher = createShortNameMainSectionHashBucketMatcher();
        List<AffOrgMatchVoter> voters = createShortNameMainSectionHashBucketMatcherVoters();

        // execute
        estimateVoterMatchStrengths(affOrgMatcher, "Short Name Main Section Hash Bucket Matcher", voters);
    }

    private void estimateFirstWordsHashBucketMatcherVoterStrengths() throws IOException {
        // given
        createInputData();
        AffOrgMatcher affOrgMatcher = createNameFirstWordsHashBucketMatcher();
        List<AffOrgMatchVoter> voters = createNameFirstWordsHashBucketMatcherVoters();

        // execute
        estimateVoterMatchStrengths(affOrgMatcher, "First Words Hash Bucket Matcher", voters);
    }

    private void estimateVoterMatchStrengths(AffOrgMatcher affOrgMatcher, String affOrgMatcherName, List<AffOrgMatchVoter> voters) throws IOException {
        printMatcherHeader(affOrgMatcherName);
        List<String> matchedAffPaths = ImmutableList.of(
                ClassPathResourceProvider.getResourcePath("experimentalData/expectedOutput/matched_aff.json"));

        // given
        affMatchingService.setAffOrgMatchers(ImmutableList.of(affOrgMatcher));
        AffOrgMatchComputer affOrgMatchComputer = new AffOrgMatchComputer();
        for (AffOrgMatchVoter voter : voters) {
            printVoterHeader(voter);
            affOrgMatchComputer.setAffOrgMatchVoters(Lists.newArrayList(voter));
            affOrgMatcher.setAffOrgMatchComputer(affOrgMatchComputer);

            // execute
            affMatchingService.matchAffiliations(sparkContext, inputAffDirPath, inputOrgDirPath, outputDirPath, outputReportPath);

            // log
            float calculatedVoterStrength = calcAndPrintResult(matchedAffPaths);
            checkIfVoterStrengthSetCorrectly(affOrgMatcherName, voter, calculatedVoterStrength);
            FileUtils.deleteDirectory(new File(outputDirPath));
        }

        FileUtils.deleteDirectory(workingDir);
    }

    private void checkIfVoterStrengthSetCorrectly(String affOrgMatcherName, AffOrgMatchVoter voter, float calculatedVoterStrength) {
        double voterStrengthEpsilon = 5/Math.pow(10, 1+VOTER_MATCH_STRENGTH_SCALE);

        if ((calculatedVoterStrength <= voter.getMatchStrength() - voterStrengthEpsilon) ||
            (calculatedVoterStrength >= voter.getMatchStrength() + voterStrengthEpsilon)) {

                String voterName = affOrgMatcherName + ":" + voter.toString();
                invalidVoterStrengths.add(new InvalidVoterStrength(voterName, calculatedVoterStrength, voter.getMatchStrength()));
        }
    }

    private void printVoterHeader(AffOrgMatchVoter voter) {
        logger.trace("---------------------------------- VOTER ----------------------------------------");
        logger.trace(voter.toString());
    }

    private void printMatcherHeader(String affOrgMatcherName) {
        logger.trace("==================================================================================");
        logger.trace("========================= " + affOrgMatcherName + " ===========================");
        logger.trace("==================================================================================");
    }

    private void createInputData() throws IOException {
        createInputDataFromJsonFiles(
                of(INPUT_DATA_DIR_PATH + "/all_organizations.json"),
                of(INPUT_DATA_DIR_PATH + "/docs_with_aff_real_data.json"),
                of(INPUT_DATA_DIR_PATH + "/doc_project.json"),
                of(),
                of(INPUT_DATA_DIR_PATH + "/org_project.json"));
    }

    private void createInputDataFromJsonFiles(List<String> jsonInputOrgPaths, List<String> jsonInputAffPaths, List<String> jsonInputDocProjPaths, List<String> jsonInputInferredDocProjPaths, List<String> jsonInputProjOrgPaths) throws IOException {
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputOrgPaths, Organization.class), inputOrgDirPath, Organization.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputAffPaths, ExtractedDocumentMetadata.class), inputAffDirPath, ExtractedDocumentMetadata.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputDocProjPaths, eu.dnetlib.iis.importer.schemas.DocumentToProject.class), inputDocProjDirPath, eu.dnetlib.iis.importer.schemas.DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputInferredDocProjPaths, DocumentToProject.class), inputInferredDocProjDirPath, DocumentToProject.class);
        createLocalAvroDataStore(readMultipleJsonDataStores(jsonInputProjOrgPaths, ProjectToOrganization.class), inputProjOrgDirPath, ProjectToOrganization.class);
    }

    private float calcAndPrintResult(List<String> expectedResultsJsonPaths) throws IOException {
        List<SimpleAffMatchResult> actualMatches = readJson(outputDirPath + "/part-00000", SimpleAffMatchResult.class);
        List<SimpleAffMatchResult> expectedMatches = readMultipleJsons(expectedResultsJsonPaths, SimpleAffMatchResult.class);

        List<SimpleAffMatchResult> correctMatches = actualMatches.stream().filter(expectedMatches::contains).collect(toList());
        List<SimpleAffMatchResult> falsePositives = actualMatches.stream().filter(x -> !expectedMatches.contains(x)).collect(toList());

        float matchStrength = calcMatchStrength(actualMatches.size(), correctMatches.size());

        printMatchStrength(matchStrength);

        if (PRINT_NUMBER_DETAILS) {
            printNumberDetails(expectedMatches.size(), actualMatches.size(), correctMatches.size(), falsePositives.size());
        }

        if (PRINT_FALSE_POSITIVES) {
            printFalsePositives(inputAffDirPath, inputOrgDirPath, expectedMatches, actualMatches);
        }

        if (PRINT_NOT_MATCHED) {
            printNotMatched(inputAffDirPath, inputOrgDirPath, expectedMatches, actualMatches);
        }

        return matchStrength;
    }

    private float calcMatchStrength(int numberOfActualMatches, int numberOfCorrectMatches) {
        return ((float)numberOfCorrectMatches)/numberOfActualMatches;
    }

    private void printMatchStrength(float matchStrength) {
        logger.trace(String.format("%s %1." + VOTER_MATCH_STRENGTH_SCALE + "f", "MATCH STRENGTH: ", matchStrength));
    }

    private void printNumberDetails(int numberOfExpectedMatches, int numberOfActualMatches, int numberOfCorrectMatches, int numberOfFalsePositives) {
        logger.trace("[{}, {}, {}]",
                qualityFactor("All matches", numberOfActualMatches, numberOfExpectedMatches),
                qualityFactor("Correct matches", numberOfCorrectMatches, numberOfActualMatches),
                qualityFactor("False positives", numberOfFalsePositives, numberOfActualMatches));
    }

    private String qualityFactor(String factorName, int goodCount, int totalCount) {
        double factorPercentage = ((double)goodCount/totalCount)*100;

        return String.format("%s %3.2f%% (%d/%d)", factorName + ":", factorPercentage, goodCount, totalCount);
    }

    private AffMatchingService createAffMatchingService() throws IOException {
        AffMatchingService affMatchingService = new AffMatchingService();

        // readers
        affMatchingService.setAffiliationReader(new IisAffiliationReader());
        affMatchingService.setOrganizationReader(new IisOrganizationReader());

        // writer
        affMatchingService.setAffMatchResultWriter(new SimpleAffMatchResultWriter());

        AffMatchOrganizationAltNameFiller altNameFiller = createAffMatchOrganizationAltNameFiller();
        affMatchingService.setAffMatchOrganizationAltNameFiller(altNameFiller);

        return affMatchingService;
    }

    private AffMatchOrganizationAltNameFiller createAffMatchOrganizationAltNameFiller() throws IOException {
        AffMatchOrganizationAltNameFiller altNameFiller = new AffMatchOrganizationAltNameFiller();

        List<Set<String>> alternativeNamesDictionary = new CsvOrganizationAltNamesDictionaryFactory()
                .createAlternativeNamesDictionary(OrganizationAltNameConst.CLASSPATH_ALTERNATIVE_NAMES_CSV_FILES);
        altNameFiller.setAlternativeNamesDictionary(alternativeNamesDictionary);

        return altNameFiller;
    }

    //------------------------ INNER CLASSES --------------------------

    private static class InvalidVoterStrength {

        private String voterName;

        private float calculatedStrength;

        private float setStrength;

        //------------------------ CONSTRUCTORS --------------------------

        public InvalidVoterStrength(String voterName, float calculatedStrength, float setStrength) {
            super();
            this.voterName = voterName;
            this.calculatedStrength = calculatedStrength;
            this.setStrength = setStrength;
        }

        //------------------------ toString --------------------------

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("voterName", voterName)
                                        .add("calculatedStrength", String.format("%1." + VOTER_MATCH_STRENGTH_SCALE + "f", calculatedStrength))
                                        .add("setStrength", String.format("%1." + VOTER_MATCH_STRENGTH_SCALE + "f", setStrength))
                                        .toString();
        }
    }
}
