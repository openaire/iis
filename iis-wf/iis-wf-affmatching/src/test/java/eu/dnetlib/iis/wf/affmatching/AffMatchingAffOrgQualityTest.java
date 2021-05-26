package eu.dnetlib.iis.wf.affmatching;

import eu.dnetlib.iis.common.spark.TestWithSharedSparkContext;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.match.AffOrgMatcher;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.AffMatchOrganizationAltNameFiller;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.CsvOrganizationAltNamesDictionaryFactory;
import eu.dnetlib.iis.wf.affmatching.orgalternativenames.OrganizationAltNameConst;
import eu.dnetlib.iis.wf.affmatching.read.AffiliationConverter;
import eu.dnetlib.iis.wf.affmatching.read.IisAffiliationReader;
import eu.dnetlib.iis.wf.affmatching.read.IisOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.write.AffMatchResultWriter;
import eu.dnetlib.iis.wf.affmatching.write.SimpleAffMatchResultWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.of;
import static eu.dnetlib.iis.common.utils.AvroTestUtils.createLocalAvroDataStore;
import static eu.dnetlib.iis.common.utils.JsonAvroTestUtils.readMultipleJsonDataStores;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readJson;
import static eu.dnetlib.iis.common.utils.JsonTestUtils.readMultipleJsons;
import static eu.dnetlib.iis.wf.affmatching.match.DocOrgRelationMatcherFactory.createDocOrgRelationMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.FirstWordsHashBucketMatcherFactory.createNameFirstWordsHashBucketMatcher;
import static eu.dnetlib.iis.wf.affmatching.match.MainSectionHashBucketMatcherFactory.*;
import static java.util.stream.Collectors.toList;

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
public class AffMatchingAffOrgQualityTest extends TestWithSharedSparkContext {

    private static final Logger logger = LoggerFactory.getLogger(AffMatchingAffOrgQualityTest.class);

    private final static boolean PRINT_NOT_MATCHED = true;
    private final static boolean PRINT_FALSE_POSITIVE_MATCHES = true;

    private final static String INPUT_DATA_DIR_PATH = "src/test/resources/experimentalData/input";

    private AffMatchingService affMatchingService;

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

    @BeforeEach
    public void setup() throws IOException {
        super.beforeEach();

        inputOrgDirPath = workingDir + "/affiliation_matching/input/organizations";
        inputAffDirPath = workingDir + "/affiliation_matching/input/affiliations";
        inputDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj";
        inputInferredDocProjDirPath = workingDir + "/affiliation_matching/input/doc_proj_inferred";
        inputProjOrgDirPath = workingDir + "/affiliation_matching/input/proj_org";
        outputDirPath = workingDir + "/affiliation_matching/output";
        outputReportPath = workingDir + "/affiliation_matching/report";

        affMatchingService = createAffMatchingService();
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
        affMatchingService.matchAffiliations(jsc(), inputAffDirPath, inputOrgDirPath, outputDirPath, outputReportPath);

        // log
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
                .filter(expectedMatches::contains)
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
        double factorPercentage = ((double) goodCount / totalCount) * 100;

        String text = String.format("%-30s %5.2f%% (%d/%d)", factorName + ":", factorPercentage, goodCount, totalCount);
        logger.trace(text);
    }

    private AffMatchingService createAffMatchingService() throws IOException {
        AffMatchingService affMatchingService = new AffMatchingService();

        // readers
        IisAffiliationReader affiliationReader = new IisAffiliationReader();
        AffiliationConverter affiliationConverter = new AffiliationConverter();
        affiliationConverter.setDocumentAcceptor((position, extractedDocumentMetadata) -> true);
        affiliationReader.setAffiliationConverter(affiliationConverter);
        affMatchingService.setAffiliationReader(affiliationReader);
        affMatchingService.setOrganizationReader(new IisOrganizationReader());

        // writer
        affMatchingService.setAffMatchResultWriter(new SimpleAffMatchResultWriter());

        // matchers
        AffOrgMatcher docOrgRelationMatcher =
                createDocOrgRelationMatcher(jsc(), inputDocProjDirPath, inputInferredDocProjDirPath, inputProjOrgDirPath, inputDocProjConfidenceThreshold);

        AffOrgMatcher nameMainSectionHashBucketMatcher = createNameMainSectionHashBucketMatcher();

        AffOrgMatcher shortNameMainSectionHashBucketMatcher = createShortNameMainSectionHashBucketMatcher();

        AffOrgMatcher alternativeNameMainSectionHashBucketMatcher = createAlternativeNameMainSectionHashBucketMatcher();

        AffOrgMatcher firstWordsNameHashBucketMatcher = createNameFirstWordsHashBucketMatcher();

        affMatchingService.setAffOrgMatchers(of(docOrgRelationMatcher, nameMainSectionHashBucketMatcher, shortNameMainSectionHashBucketMatcher, alternativeNameMainSectionHashBucketMatcher, firstWordsNameHashBucketMatcher));

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
}
