package eu.dnetlib.iis.wf.affmatching.match;

import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_AFF_WORDS;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_ORG_WORDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.reflect.Whitebox.getInternalState;

import java.util.List;
import java.util.function.Function;

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
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonAffSectionWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation;
import eu.dnetlib.iis.wf.affmatching.match.voter.CompositeMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeLooseMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CountryCodeStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgAlternativeNamesFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.GetOrgShortNameFunction;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameStrictWithCharFilteringMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameLevenshteinMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameStrictMatchVoter;
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
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertDocOrgRelationMatcherVoters(voters);
    }
    
    
    @Test
    public void createDocOrgRelationMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createDocOrgRelationMatcherVoters();
        
        // assert
        
        assertDocOrgRelationMatcherVoters(voters);
    }

 
    
    @Test
    public void createNameMainSectionHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = AffOrgMatcherFactory.createNameMainSectionHashBucketMatcher();
        
        
        // assert
        
        assertMainSectionHashBucketMatcher(matcher, GetOrgNameFunction.class);
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertNameMainSectionHashBucketMatcherVoters(voters);
        
    }


    @Test
    public void createNameMainSectionHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createNameMainSectionHashBucketMatcherVoters();
        
        // assert
        
        assertNameMainSectionHashBucketMatcherVoters(voters);
        
        
    }

    
    @Test
    public void createAlternativeNameMainSectionHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = AffOrgMatcherFactory.createAlternativeNameMainSectionHashBucketMatcher();
        
        
        // assert
        
        assertMainSectionHashBucketMatcher(matcher, GetOrgAlternativeNamesFunction.class);
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertAlternativeNameMainSectionHashBucketMatcherVoters(voters);
        
    }


    @Test
    public void createAlternativeNameMainSectionHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createAlternativeNameMainSectionHashBucketMatcherVoters();
        
        // assert
        
        assertAlternativeNameMainSectionHashBucketMatcherVoters(voters);
        
        
    }

    
    @Test
    public void createShortNameMainSectionHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = AffOrgMatcherFactory.createShortNameMainSectionHashBucketMatcher();
        
        
        // assert
        
        assertMainSectionHashBucketMatcher(matcher, GetOrgShortNameFunction.class);
        
        
        AffOrgMatchComputer computer = getInternalState(matcher, AffOrgMatchComputer.class);
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertShortNameMainSectionHashBucketMatcherVoters(voters);
        
    }


    @Test
    public void createShortNameMainSectionHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createShortNameMainSectionHashBucketMatcherVoters();
        
        // assert
        
        assertShortNameMainSectionHashBucketMatcherVoters(voters);
        
        
    }
    
    
    
    @Test
    public void createNameFirstWordsHashBucketMatcher() {
        
        // execute
        
        AffOrgMatcher matcher = AffOrgMatcherFactory.createNameFirstWordsHashBucketMatcher();
        
        
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
        
        List<AffOrgMatchVoter> voters = getInternalState(computer, "affOrgMatchVoters");
        
        assertNameFirstWordsHashBucketMatcherVoters(voters);
        
    }
    
    @Test
    public void createNameFirstWordsHashBucketMatcherVoters() {
        
        // execute
        
        List<AffOrgMatchVoter> voters = AffOrgMatcherFactory.createNameFirstWordsHashBucketMatcherVoters();
        
        
        // assert
        
        assertNameFirstWordsHashBucketMatcherVoters(voters);
    }


    
    //------------------------ PRIVATE --------------------------
    
    private void assertCompositeVoter(AffOrgMatchVoter voter, Class<?>... internalVoterClasses) {
        
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, internalVoterClasses.length);
        
        for (int i = 0; i < internalVoterClasses.length; i++) {
            assertTrue(getInternalVoter(voter, i).getClass().isAssignableFrom(internalVoterClasses[i]));
        }
        
    }
    
    private void assertNameStrictWithCharFilteringMatchVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof NameStrictWithCharFilteringMatchVoter);
        
        assertEquals(expectedCharsToFilter, getInternalState(voter, "charsToFilter"));
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
    }
    
    private void assertSectionedNameLevenshteinMatchVoter(AffOrgMatchVoter voter, float expectedMinSimilarity, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof SectionedNameLevenshteinMatchVoter);
        
        assertEquals(expectedMinSimilarity, getInternalState(voter, "minSimilarity"), PRECISION);
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
    }

    
    private void assertSectionedNameStrictMatchVoter(AffOrgMatchVoter voter, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof SectionedNameStrictMatchVoter);
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
    }

    
    private void assertVoterGetOrgNamesFunction(AffOrgMatchVoter voter, Class<? extends Function<AffMatchOrganization, List<String>>> expectedGetOrgNamesFunctionClass) {
        
        Function<AffMatchOrganization, List<String>> voterGetOrgNamesFunction = getInternalState(voter, "getOrgNamesFunction");
        
        assertEquals(expectedGetOrgNamesFunctionClass, voterGetOrgNamesFunction.getClass());
    }
    
    
    private void assertCommonWordsVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, double expectedMinCommonWordsRatio, RatioRelation expectedRatioRelation,
            double expectedMinWordSimilarity, int expectedWordToRemoveMaxLength, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof CommonWordsVoter);
        
        List<Character> charsToFilter = getInternalState(voter, "charsToFilter");
        assertEquals(expectedCharsToFilter, charsToFilter);
        
        double minCommonWordsRatio = getInternalState(voter, "minCommonWordsRatio");
        assertEquals(expectedMinCommonWordsRatio, minCommonWordsRatio, PRECISION);
        
        RatioRelation ratioRelation = getInternalState(voter, "ratioRelation");
        assertEquals(expectedRatioRelation, ratioRelation);
        
        
        double minWordSimilarity = getInternalState(getInternalState(voter, "commonSimilarWordCalculator"), "minWordSimilarity");
        assertEquals(expectedMinWordSimilarity, minWordSimilarity, PRECISION);
        
        int wordToRemoveMaxLength = getInternalState(voter, "wordToRemoveMaxLength");
        assertEquals(expectedWordToRemoveMaxLength, wordToRemoveMaxLength);
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
    }
    
    private void assertCommonAffOrgSectionWordsVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, double expectedMinCommonWordsRatio,
            double expectedMinWordSimilarity, int expectedWordToRemoveMaxLength, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof CommonAffSectionWordsVoter);
        
        List<Character> charsToFilter = getInternalState(voter, "charsToFilter");
        assertEquals(expectedCharsToFilter, charsToFilter);
        
        double minCommonWordsRatio = getInternalState(voter, "minCommonWordsRatio");
        assertEquals(expectedMinCommonWordsRatio, minCommonWordsRatio, PRECISION);
        
        double minWordSimilarity = getInternalState(getInternalState(voter, "commonSimilarWordCalculator"), "minWordSimilarity");
        assertEquals(expectedMinWordSimilarity, minWordSimilarity, PRECISION);
        
        int wordToRemoveMaxLength = getInternalState(voter, "wordToRemoveMaxLength");
        assertEquals(expectedWordToRemoveMaxLength, wordToRemoveMaxLength);
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
        
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
    
    
    private void assertMainSectionHashBucketMatcher(AffOrgMatcher matcher, Class<? extends Function<AffMatchOrganization, List<String>>> expectedGetOrgNamesFunctionClass) {
        
        AffOrgJoiner joiner = getInternalState(matcher, AffOrgJoiner.class);
        assertTrue(joiner instanceof AffOrgHashBucketJoiner);
        
        
        BucketHasher<AffMatchAffiliation> affHasher = getInternalState(joiner, "affiliationBucketHasher");
        assertTrue(affHasher instanceof AffiliationOrgNameBucketHasher);
        
        assertInternalMainSectionBucketHasher(affHasher, OrgSectionType.UNIVERSITY, FallbackSectionPickStrategy.LAST_SECTION);
        
        
        BucketHasher<AffMatchOrganization> orgHasher = getInternalState(joiner, "organizationBucketHasher");
        assertTrue(orgHasher instanceof OrganizationNameBucketHasher);
        
        Function<AffMatchOrganization, List<String>> getOrgNamesFunction = getInternalState(orgHasher, "getOrgNamesFunction");
        assertEquals(expectedGetOrgNamesFunctionClass, getOrgNamesFunction.getClass());
        
        assertInternalMainSectionBucketHasher(orgHasher, OrgSectionType.UNIVERSITY, FallbackSectionPickStrategy.FIRST_SECTION);
    }
    
    
    
    private void assertDocOrgRelationMatcherVoters(List<AffOrgMatchVoter> voters) {
        
        assertEquals(15, voters.size());
        
        assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(1), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
        
        assertCompositeVoter(voters.get(2), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(2), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
        
        
        assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(3), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(4), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
        
        assertCompositeVoter(voters.get(5), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
        assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(5), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
        
        
        assertCompositeVoter(voters.get(6), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertVoterGetOrgNamesFunction(getInternalVoter(voters.get(6), 1), GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(7), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertVoterGetOrgNamesFunction(getInternalVoter(voters.get(7), 1), GetOrgAlternativeNamesFunction.class);
        
        assertCompositeVoter(voters.get(8), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
        assertVoterGetOrgNamesFunction(getInternalVoter(voters.get(8), 1), GetOrgShortNameFunction.class);
        
        
        assertCompositeVoter(voters.get(9), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(9), 1), 0.9f, GetOrgNameFunction.class);
        
        assertCompositeVoter(voters.get(10), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
        assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(10) ,1), 0.9f, GetOrgAlternativeNamesFunction.class);
        
        
        assertCommonWordsVoter(voters.get(11), ImmutableList.of(',', ';'), 0.7f, WITH_REGARD_TO_ORG_WORDS, 0.9f, 2, GetOrgNameFunction.class);
        
        assertCommonWordsVoter(voters.get(12), ImmutableList.of(',', ';'), 0.7f, WITH_REGARD_TO_ORG_WORDS, 0.9f, 2, GetOrgAlternativeNamesFunction.class);
        
        
        assertCommonAffOrgSectionWordsVoter(voters.get(13), ImmutableList.of(',', ';'), 0.8f, 0.85f, 1, GetOrgNameFunction.class);
        
        assertCommonAffOrgSectionWordsVoter(voters.get(14), ImmutableList.of(',', ';'), 0.8f, 0.85f, 1, GetOrgAlternativeNamesFunction.class);
    }
   
   
    private void assertNameMainSectionHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
       assertEquals(5, voters.size());
       
       assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
       
       assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
       
       assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
       assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(2), 1), GetOrgNameFunction.class);
       
       assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
       assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f, GetOrgNameFunction.class);
       
       assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
       assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(4), 1), GetOrgShortNameFunction.class);
   }
   
   
   private void assertAlternativeNameMainSectionHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
       
       assertEquals(4, voters.size());
       
       assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
       
       assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgAlternativeNamesFunction.class);
       
       assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
       assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(2), 1), GetOrgAlternativeNamesFunction.class);
       
       assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
       assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f, GetOrgAlternativeNamesFunction.class);
       
   }
   
   
   private void assertShortNameMainSectionHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
       
       assertEquals(2, voters.size());
       
       assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
       
       assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgShortNameFunction.class);
       
   }
   
   
   private void assertNameFirstWordsHashBucketMatcherVoters(List<AffOrgMatchVoter> voters) {
       assertEquals(6, voters.size());
       
       assertCompositeVoter(voters.get(0), CountryCodeStrictMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(0), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
       
       assertCompositeVoter(voters.get(1), CountryCodeLooseMatchVoter.class, NameStrictWithCharFilteringMatchVoter.class);
       assertNameStrictWithCharFilteringMatchVoter(getInternalVoter(voters.get(1), 1), ImmutableList.of(',', ';'), GetOrgNameFunction.class);
       
       assertCompositeVoter(voters.get(2), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
       
       assertCompositeVoter(voters.get(3), CountryCodeLooseMatchVoter.class, SectionedNameLevenshteinMatchVoter.class);
       assertSectionedNameLevenshteinMatchVoter(getInternalVoter(voters.get(3), 1), 0.9f, GetOrgNameFunction.class);
       
       assertCompositeVoter(voters.get(4), CountryCodeLooseMatchVoter.class, SectionedNameStrictMatchVoter.class);
       assertSectionedNameStrictMatchVoter(getInternalVoter(voters.get(4), 1), GetOrgShortNameFunction.class);
       
       assertCompositeVoter(voters.get(5), CountryCodeLooseMatchVoter.class, CommonWordsVoter.class, CommonWordsVoter.class);
       assertCommonWordsVoter(getInternalVoter(voters.get(5), 1), ImmutableList.of(',', ';'), 0.7f, WITH_REGARD_TO_ORG_WORDS, 0.9f, 2, GetOrgNameFunction.class);
       assertCommonWordsVoter(getInternalVoter(voters.get(5), 2), ImmutableList.of(',', ';'), 0.8f, WITH_REGARD_TO_AFF_WORDS, 0.9f, 2, GetOrgNameFunction.class);
   }
   
}
