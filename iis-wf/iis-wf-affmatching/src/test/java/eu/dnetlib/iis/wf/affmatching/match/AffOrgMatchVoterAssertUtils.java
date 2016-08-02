package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.reflect.Whitebox.getInternalState;

import java.util.List;
import java.util.function.Function;

import eu.dnetlib.iis.wf.affmatching.bucket.BucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher;
import eu.dnetlib.iis.wf.affmatching.bucket.MainSectionBucketHasher.FallbackSectionPickStrategy;
import eu.dnetlib.iis.wf.affmatching.bucket.StringPartFirstLettersHasher;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonAffSectionWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation;
import eu.dnetlib.iis.wf.affmatching.match.voter.CompositeMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.NameStrictWithCharFilteringMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameLevenshteinMatchVoter;
import eu.dnetlib.iis.wf.affmatching.match.voter.SectionedNameStrictMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;

/**
 * Utility methods to assert created {@link AffOrgMatchVoter}s 
 * 
 * @author Łukasz Dumiszewski
*/

class AffOrgMatchVoterAssertUtils {

    
   private static final double PRECISION = 1e10-6;
   
   
   
   
    //------------------------ LOGIC --------------------------
   
    static void assertCompositeVoter(AffOrgMatchVoter voter, Class<?>... internalVoterClasses) {
        
        assertTrue(voter instanceof CompositeMatchVoter);
        assertInternalVotersCount(voter, internalVoterClasses.length);
        
        for (int i = 0; i < internalVoterClasses.length; i++) {
            assertTrue(getInternalVoter(voter, i).getClass().isAssignableFrom(internalVoterClasses[i]));
        }
        
    }
    
    static void assertNameStrictWithCharFilteringMatchVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof NameStrictWithCharFilteringMatchVoter);
        
        assertEquals(expectedCharsToFilter, getInternalState(voter, "charsToFilter"));
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
    }
    
    static void assertSectionedNameLevenshteinMatchVoter(AffOrgMatchVoter voter, float expectedMinSimilarity, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof SectionedNameLevenshteinMatchVoter);
        
        assertEquals(expectedMinSimilarity, getInternalState(voter, "minSimilarity"), PRECISION);
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
    }

    
    static void assertSectionedNameStrictMatchVoter(AffOrgMatchVoter voter, Class<? extends Function<AffMatchOrganization, List<String>>> getOrgNamesFunctionClass) {
        
        assertTrue(voter instanceof SectionedNameStrictMatchVoter);
        
        assertVoterGetOrgNamesFunction(voter, getOrgNamesFunctionClass);
    }

    
   static void assertVoterGetOrgNamesFunction(AffOrgMatchVoter voter, Class<? extends Function<AffMatchOrganization, List<String>>> expectedGetOrgNamesFunctionClass) {
        
        Function<AffMatchOrganization, List<String>> voterGetOrgNamesFunction = getInternalState(voter, "getOrgNamesFunction");
        
        assertEquals(expectedGetOrgNamesFunctionClass, voterGetOrgNamesFunction.getClass());
    }
    
    
   static void assertCommonWordsVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, double expectedMinCommonWordsRatio, RatioRelation expectedRatioRelation,
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
   
    static void assertCommonAffOrgSectionWordsVoter(AffOrgMatchVoter voter, List<Character> expectedCharsToFilter, double expectedMinCommonWordsRatio,
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
   
    static void assertInternalMainSectionBucketHasher(BucketHasher<?> hasher, OrgSectionType mainSectionType, FallbackSectionPickStrategy fallbackSectionStrategy) {
        BucketHasher<String> stringHasher = getInternalState(hasher, "stringHasher");
        assertTrue(stringHasher instanceof MainSectionBucketHasher);
        
        assertEquals(mainSectionType, getInternalState(stringHasher, "mainSectionType"));
        assertEquals(fallbackSectionStrategy, getInternalState(stringHasher, "fallbackSectionPickStrategy"));
    }
    
   
   public static void assertInternalStringPartFirstLettersHasher(BucketHasher<?> hasher, int expectedNumberOfParts, int expectedNumberOfLettersPerPart) {
        BucketHasher<String> stringHasher = getInternalState(hasher, "stringHasher");
        assertTrue(stringHasher instanceof StringPartFirstLettersHasher);
        
        int numberOfParts = getInternalState(stringHasher, "numberOfParts");
        assertEquals(expectedNumberOfParts, numberOfParts);
        
        int numberOfLettersPerPart = getInternalState(stringHasher, "numberOfLettersPerPart");
        assertEquals(expectedNumberOfLettersPerPart, numberOfLettersPerPart);
    }
   
   
   public static void assertInternalVotersCount(AffOrgMatchVoter voter, int expectedCount) {
        
        List<AffOrgMatchVoter> internalVoters = getInternalState(voter, "voters");
        
        assertEquals(expectedCount, internalVoters.size());
    }
    
   
   public static AffOrgMatchVoter getInternalVoter(AffOrgMatchVoter voter, int position) {
        
        List<AffOrgMatchVoter> internalVoters = getInternalState(voter, "voters");
        
        return internalVoters.get(position);
    }
    
    
}
