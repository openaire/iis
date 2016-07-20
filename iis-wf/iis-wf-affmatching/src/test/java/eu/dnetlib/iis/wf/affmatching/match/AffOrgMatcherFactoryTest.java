package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.reflect.Whitebox.getInternalState;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffiliationOrgNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.BucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.DocOrgRelationAffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher.FallbackSectionPickStrategy;
import eu.dnetlib.iis.wf.affmatching.bucket.OrganizationNameBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.StringPartFirstLettersHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationCombiner;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationFetcher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentProjectFetcher;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentProjectMerger;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisDocumentProjectReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisInferredDocumentProjectReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.IisProjectOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.ProjectOrganizationReader;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CompositeMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeLooseMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.FittingAffOrgSectionWordsMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.FittingAffOrgWordsMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.FittingOrgWordsMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameStrictWithCharFilteringMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameLevenshteinMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedShortNameStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;

/**
 * @author madryk
 */
public class AffOrgMatcherFactoryTest {

    private static final double PRECISION = 1e10-6;
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createDocOrgRelationMatcher() {
        
        // given
        
        JavaSparkContext sparkContext = Mockito.mock(JavaSparkContext.class);
        String inputAvroDocProjPath = "/path/docproj/";
        String inputAvroInferredDocProjPath = "/path/docproj/inferred/";
        String inputAvroProjOrgPath = "/path/projorg/";
        Float inputDocProjConfidenceThreshold = 0.4f;
        
        
        // execute
        
        AffOrgMatcher matcher = AffOrgMatcherFactory.createDocOrgRelationMatcher(sparkContext, inputAvroDocProjPath, inputAvroInferredDocProjPath, inputAvroProjOrgPath, inputDocProjConfidenceThreshold);
        
        
        // assert
        
        AffOrgJoiner joiner = getInternalState(matcher, AffOrgJoiner.class);
        assertTrue(joiner instanceof DocOrgRelationAffOrgJoiner);
        
        DocumentOrganizationFetcher docOrgFetcher = getInternalState(joiner, DocumentOrganizationFetcher.class);
        
        assertTrue(getInternalState(docOrgFetcher, ProjectOrganizationReader.class) instanceof IisProjectOrganizationReader);
        assertNotNull(getInternalState(docOrgFetcher, DocumentOrganizationCombiner.class));
        
        float actualDocProjConfidenceLevelThreshold = getInternalState(docOrgFetcher, "docProjConfidenceLevelThreshold");
        assertEquals(inputDocProjConfidenceThreshold, actualDocProjConfidenceLevelThreshold, PRECISION);
        assertEquals(sparkContext, getInternalState(docOrgFetcher, JavaSparkContext.class));
        assertEquals(inputAvroProjOrgPath, getInternalState(docOrgFetcher, "projOrgPath"));
        
        
        DocumentProjectFetcher docProjFetcher = getInternalState(docOrgFetcher, DocumentProjectFetcher.class);
        
        assertTrue(getInternalState(docProjFetcher, "firstDocumentProjectReader") instanceof IisDocumentProjectReader);
        assertTrue(getInternalState(docProjFetcher, "secondDocumentProjectReader") instanceof IisInferredDocumentProjectReader);
        
        assertEquals(inputAvroDocProjPath, getInternalState(docProjFetcher, "firstDocProjPath"));
        assertEquals(inputAvroInferredDocProjPath, getInternalState(docProjFetcher, "secondDocProjPath"));
        
        assertNotNull(getInternalState(docProjFetcher, DocumentProjectMerger.class));
        
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        assertVotersCount(computer, 7);
        
        AffOrgMatchVoter voter0 = getVoter(computer, 0);
        assertCompositeVoter(voter0, CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voter0, 1), ImmutableList.of(',', ';'));
        
        AffOrgMatchVoter voter1 = getVoter(computer, 1);
        assertCompositeVoter(voter1, CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voter1, 1), ImmutableList.of(',', ';'));
        
        AffOrgMatchVoter voter2 = getVoter(computer, 2);
        assertCompositeVoter(voter2, CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        
        AffOrgMatchVoter voter3 = getVoter(computer, 3);
        assertCompositeVoter(voter3, CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voter3, 1), 0.9f);
        
        AffOrgMatchVoter voter4 = getVoter(computer, 4);
        assertCompositeVoter(voter4, CountryCodeLooseMatchVoter.class, SectionedShortNameStrictMatchVoter.class);
        
        AffOrgMatchVoter voter5 = getVoter(computer, 5);
        assertFittingOrgWordsMatchVoter(voter5, ImmutableList.of(',', ';'), 0.7f, 0.9f, 2);
        
        AffOrgMatchVoter voter6 = getVoter(computer, 6);
        assertFittingAffOrgSectionWordsMatchVoter(voter6, ImmutableList.of(',', ';'), 0.8f, 0.85f, 1);
    }
    
    @Test
    public void createDocOrgRelationMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createDocOrgRelationMatcherVoters();
        
        // assert
        
        assertEquals(7, voters.size());
        
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'));
        
        assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'));
        
        assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f);
        
        assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, SectionedShortNameStrictMatchVoter.class);
        
        assertFittingOrgWordsMatchVoter(voters.get(5), ImmutableList.of(',', ';'), 0.7f, 0.9f, 2);
        
        assertFittingAffOrgSectionWordsMatchVoter(voters.get(6), ImmutableList.of(',', ';'), 0.8f, 0.85f, 1);
    }
    
    @Test
    public void createMainSectionHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = AffOrgMatcherFactory.createMainSectionHashBucketMatcher();
        
        
        // assert
        
        AffOrgJoiner joiner = getInternalState(matcher, AffOrgJoiner.class);
        assertTrue(joiner instanceof AffOrgHashBucketJoiner);
        
        
        BucketHasher<AffMatchAffiliation> affHasher = getInternalState(joiner, "affiliationBucketHasher");
        assertTrue(affHasher instanceof AffiliationOrgNameBucketHasher);
        
        assertInternalMainSectionBucketHasher(affHasher, OrgSectionType.UNIVERSITY, FallbackSectionPickStrategy.LAST_SECTION);
        
        
        BucketHasher<AffMatchOrganization> orgHasher = getInternalState(joiner, "organizationBucketHasher");
        assertTrue(orgHasher instanceof OrganizationNameBucketHasher);
        
        assertInternalMainSectionBucketHasher(orgHasher, OrgSectionType.UNIVERSITY, FallbackSectionPickStrategy.FIRST_SECTION);
        
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        assertVotersCount(computer, 5);
        
        AffOrgMatchVoter voter0 = getVoter(computer, 0);
        assertCompositeVoter(voter0, CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voter0, 1), ImmutableList.of(',', ';'));
        
        AffOrgMatchVoter voter1 = getVoter(computer, 1);
        assertCompositeVoter(voter1, CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voter1, 1), ImmutableList.of(',', ';'));
        
        AffOrgMatchVoter voter2 = getVoter(computer, 2);
        assertCompositeVoter(voter2, CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        
        AffOrgMatchVoter voter3 = getVoter(computer, 3);
        assertCompositeVoter(voter3, CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voter3, 1), 0.9f);
        
        AffOrgMatchVoter voter4 = getVoter(computer, 4);
        assertCompositeVoter(voter4, CountryCodeLooseMatchVoter.class, SectionedShortNameStrictMatchVoter.class);
        
    }
    
    @Test
    public void createMainSectionHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createMainSectionHashBucketMatcherVoters();
        
        // assert
        
        assertEquals(5, voters.size());
        
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'));
        
        assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'));
        
        assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f);
        
        assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, SectionedShortNameStrictMatchVoter.class);
        
    }
    
    @Test
    public void createFirstWordsHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = AffOrgMatcherFactory.createFirstWordsHashBucketMatcher();
        
        
        // assert
        
        AffOrgJoiner joiner = getInternalState(matcher, AffOrgJoiner.class);
        assertTrue(joiner instanceof AffOrgHashBucketJoiner);
        
        
        BucketHasher<AffMatchAffiliation> affHasher = getInternalState(joiner, "affiliationBucketHasher");
        assertTrue(affHasher instanceof AffiliationOrgNameBucketHasher);
        
        assertInternalStringPartFirstLettersHasher(affHasher, 2, 2);
        
        
        BucketHasher<AffMatchOrganization> orgHasher = getInternalState(joiner, "organizationBucketHasher");
        assertTrue(orgHasher instanceof OrganizationNameBucketHasher);
        
        assertInternalStringPartFirstLettersHasher(affHasher, 2, 2);
        
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        assertVotersCount(computer, 6);
        
        AffOrgMatchVoter voter0 = getVoter(computer, 0);
        assertCompositeVoter(voter0, CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voter0, 1), ImmutableList.of(',', ';'));
        
        AffOrgMatchVoter voter1 = getVoter(computer, 1);
        assertCompositeVoter(voter1, CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voter1, 1), ImmutableList.of(',', ';'));
        
        AffOrgMatchVoter voter2 = getVoter(computer, 2);
        assertCompositeVoter(voter2, CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        
        AffOrgMatchVoter voter3 = getVoter(computer, 3);
        assertCompositeVoter(voter3, CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voter3, 1), 0.9f);
        
        AffOrgMatchVoter voter4 = getVoter(computer, 4);
        assertCompositeVoter(voter4, CountryCodeLooseMatchVoter.class, SectionedShortNameStrictMatchVoter.class);
        
        AffOrgMatchVoter voter5 = getVoter(computer, 5);
        assertCompositeVoter(voter5, CountryCodeLooseMatchVoter.class, FittingOrgWordsMatchVoter.class, FittingAffOrgWordsMatchVoter.class);
        assertFittingOrgWordsMatchVoter(getInternalVoter(voter5, 1), ImmutableList.of(',', ';'), 0.7f, 0.9f, 2);
        assertFittingAffOrgWordsMatchVoter(getInternalVoter(voter5, 2), ImmutableList.of(',', ';'), 0.8f, 0.9f, 2);
        
    }
    
    @Test
    public void createFirstWordsHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createFirstWordsHashBucketMatcherVoters();
        
        
        // assert
        
        assertEquals(6, voters.size());
        
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'));
        
        assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'));
        
        assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f);
        
        assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, SectionedShortNameStrictMatchVoter.class);
        
        assertCompositeVoter(voters.get(5), CountryCodeLooseMatchVoter.class, FittingOrgWordsMatchVoter.class, FittingAffOrgWordsMatchVoter.class);
        assertFittingOrgWordsMatchVoter(getInternalVoter(voters.get(5), 1), ImmutableList.of(',', ';'), 0.7f, 0.9f, 2);
        assertFittingAffOrgWordsMatchVoter(getInternalVoter(voters.get(5), 2), ImmutableList.of(',', ';'), 0.8f, 0.9f, 2);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertCompositeVoter(AffOrgMatchVoter voter, Class<?>... internalVoterClasses) {
        
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, internalVoterClasses.length);
        
        for (int i = 0; i < internalVoterClasses.length; i++) {
            assertTrue(getInternalVoter(voter, i).getClass().isAssignableFrom(internalVoterClasses[i]));
        }
        
    }
    
    private void assertNameStrictWithCharFilteringMatchVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter) {
        
        assertTrue(voter instanceof NameStrictWithCharFilteringMatchVoter);
        
        assertEquals(expectedCharsToFilter, getInternalState(voter, "charsToFilter"));
    }
    
    private void assertSectionedNameLevenshteinMatchVoter(AffOrgMatchVoter voter, float expectedMinSimilarity) {
        
        assertTrue(voter instanceof SectionedNameLevenshteinMatchVoter);
        
        assertEquals(expectedMinSimilarity, getInternalState(voter, "minSimilarity"), PRECISION);
    }
    
    private void assertFittingOrgWordsMatchVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, double expectedMinFittingOrgWordsRatio,
            double expectedMinFittingWordsSimilarity, int expectedWordToRemoveMaxLength) {
        
        assertTrue(voter instanceof FittingOrgWordsMatchVoter);
        
        List<Character> charsToFilter = getInternalState(voter, "charsToFilter");
        assertEquals(expectedCharsToFilter, charsToFilter);
        
        double minFittingOrgWordsRatio = getInternalState(voter, "minFittingOrgWordsRatio");
        assertEquals(expectedMinFittingOrgWordsRatio, minFittingOrgWordsRatio, PRECISION);
        
        double minFittingWordSimilarity = getInternalState(voter, "minFittingWordSimilarity");
        assertEquals(expectedMinFittingWordsSimilarity, minFittingWordSimilarity, PRECISION);
        
        int wordToRemoveMaxLength = getInternalState(voter, "wordToRemoveMaxLength");
        assertEquals(expectedWordToRemoveMaxLength, wordToRemoveMaxLength);
    }
    
    private void assertFittingAffOrgSectionWordsMatchVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, double expectedMinFittingOrgWordsRatio,
            double expectedMinFittingWordsSimilarity, int expectedWordToRemoveMaxLength) {
        
        assertTrue(voter instanceof FittingAffOrgSectionWordsMatchVoter);
        
        List<Character> charsToFilter = getInternalState(voter, "charsToFilter");
        assertEquals(expectedCharsToFilter, charsToFilter);
        
        double minFittingOrgWordsRatio = getInternalState(voter, "minFittingOrgWordsRatio");
        assertEquals(expectedMinFittingOrgWordsRatio, minFittingOrgWordsRatio, PRECISION);
        
        double minFittingWordSimilarity = getInternalState(voter, "minFittingWordSimilarity");
        assertEquals(expectedMinFittingWordsSimilarity, minFittingWordSimilarity, PRECISION);
        
        int wordToRemoveMaxLength = getInternalState(voter, "wordToRemoveMaxLength");
        assertEquals(expectedWordToRemoveMaxLength, wordToRemoveMaxLength);
        
    }
    
    private void assertFittingAffOrgWordsMatchVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, double expectedMinFittingOrgWordsRatio,
            double expectedMinFittingWordsSimilarity, int expectedWordToRemoveMaxLength) {
        
        assertTrue(voter instanceof FittingAffOrgWordsMatchVoter);
        
        List<Character> charsToFilter = getInternalState(voter, "charsToFilter");
        assertEquals(expectedCharsToFilter, charsToFilter);
        
        double minFittingOrgWordsRatio = getInternalState(voter, "minFittingOrgWordsRatio");
        assertEquals(expectedMinFittingOrgWordsRatio, minFittingOrgWordsRatio, PRECISION);
        
        double minFittingWordSimilarity = getInternalState(voter, "minFittingWordSimilarity");
        assertEquals(expectedMinFittingWordsSimilarity, minFittingWordSimilarity, PRECISION);
        
        int wordToRemoveMaxLength = getInternalState(voter, "wordToRemoveMaxLength");
        assertEquals(expectedWordToRemoveMaxLength, wordToRemoveMaxLength);
    }
    
    private void assertVotersCount(AffOrgMatchComputer computer, int expectedCount) {
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertEquals(expectedCount, voters.size());
    }
    
    private AffOrgMatchVoter getVoter(AffOrgMatchComputer computer, int position) {
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        return voters.get(position);
    }
    
    private void assertInternalMainSectionBucketHasher(BucketHasher<?> hasher, OrgSectionType mainSectionType, FallbackSectionPickStrategy fallbackSectionStrategy) {
        BucketHasher<String> stringHasher = getInternalState(hasher, "stringHasher");
        assertTrue(stringHasher instanceof MainSectionBucketHasher);
        
        assertEquals(mainSectionType, getInternalState(stringHasher, "mainSectionType"));
        assertEquals(fallbackSectionStrategy, getInternalState(stringHasher, "fallbackSectionPickStrategy"));
    }
    
    private void assertInternalStringPartFirstLettersHasher(BucketHasher<?> hasher, int expectedNumberOfParts, int expectedNumberOfLettersPerPart) {
        BucketHasher<String> stringHasher = getInternalState(hasher, "stringHasher");
        assertTrue(stringHasher instanceof StringPartFirstLettersHasher);
        
        int numberOfParts = getInternalState(stringHasher, "numberOfParts");
        assertEquals(expectedNumberOfParts, numberOfParts);
        
        int numberOfLettersPerPart = getInternalState(stringHasher, "numberOfLettersPerPart");
        assertEquals(expectedNumberOfLettersPerPart, numberOfLettersPerPart);
    }
    
    private void assertInternalVotersCount(AffOrgMatchVoter voter, int expectedCount) {
        
        List<AffOrgMatchVoter> internalVoters = getInternalState(voter, "voters");
        
        assertEquals(expectedCount, internalVoters.size());
    }
    
    private AffOrgMatchVoter getInternalVoter(AffOrgMatchVoter voter, int position) {
        
        List<AffOrgMatchVoter> internalVoters = getInternalState(voter, "voters");
        
        return internalVoters.get(position);
    }
}
