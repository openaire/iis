package eu.dnetlib.iis.wf.affmatching.match.voter;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * @author madryk, Łukasz Dumiszewski
 */
@ExtendWith(MockitoExtension.class)
public class CommonAffSectionWordsVoterTest {

    
    private static double MIN_COMMON_WORDS_RATIO = 0.8;
    
    private static int WORDS_TO_REMOVE_MAX_LENGTH = 2;
    
    private static int MIN_NUMBER_OF_WORDS_IN_AFF_SECTION = 1;
    
    private static List<Character> CHARS_TO_FILTER = ImmutableList.of(',');
    
    
    @InjectMocks
    private CommonAffSectionWordsVoter voter = new CommonAffSectionWordsVoter(CHARS_TO_FILTER, WORDS_TO_REMOVE_MAX_LENGTH, 
            MIN_COMMON_WORDS_RATIO, MIN_NUMBER_OF_WORDS_IN_AFF_SECTION);
    
    @Mock
    private OrganizationSectionsSplitter organizationSectionsSplitter;
    
    @Mock
    private StringFilter stringFilter;
    
    @Mock
    private CommonSimilarWordCalculator commonSimilarWordCalculator;
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC_ID", 1);
    
    private String affOrgName = "AFF_ORG_NAME";
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG_ID");
    
    
    @BeforeEach
    public void setup() {
        
        affiliation.setOrganizationName(affOrgName);
           
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void constructor_NULL_CHARS_TO_FILTER() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                new CommonAffSectionWordsVoter(null, 2, 0.8, 1));
    
    }
    
    
    @Test
    public void constructor_NEGATIVE_MIN_WORD_LENGTH() {

        // execute
        assertThrows(IllegalArgumentException.class, () ->
                new CommonAffSectionWordsVoter(of(), -1, 0.8, 1));
        
    }
    
    @Test
    public void constructor_TOO_HIGH_MIN_COMMON_WORDS_IN_AFF_RATIO() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                new CommonAffSectionWordsVoter(of(), 2, 1.1, 1));
        
    }
    
    
    @Test
    public void constructor_TOO_LOW_MIN_COMMON_WORDS_IN_AFF_RATIO() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                new CommonAffSectionWordsVoter(of(), 2, 0, 1));
        
    }
    
    @Test
    public void voteMatch_ONE_ORG_NAME__ENOUGH_COMMON_WORDS() {
        
        // given
        
        resetOrgNames("University of Toronto");
        
        when(organizationSectionsSplitter.splitToSections(affOrgName)).thenReturn(newArrayList("Chemistry Department", "Universiti of Toronto"));
        
        whenFilterCharsThen("University of Toronto", "University Toronto");
        
        whenFilterCharsThen("Chemistry Department", "Chemistry Department");
        whenFilterCharsThen("Universiti of Toronto", "Universiti Toronto");
        
        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Chemistry", "Department"), newArrayList("University", "Toronto")))
                                        .thenReturn(0d);

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Universiti", "Toronto"), newArrayList("University", "Toronto")))
                                        .thenReturn(1d);

        
        // execute & assert

        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    
    
    @Test
    public void voteMatch_MANY_ORG_NAMES__ENOUGH_COMMON_WORDS() {
        
        // given
        
        resetOrgNames("Technical University in Poznan", "University of Toronto", "University of Warsaw");
        
        when(organizationSectionsSplitter.splitToSections(affOrgName)).thenReturn(newArrayList("Chemistry Department", "Universiti of Toronto"));
        
        
        whenFilterCharsThen("Chemistry Department", "Chemistry Department");
        whenFilterCharsThen("Universiti of Toronto", "Universiti Toronto");
        
        whenFilterCharsThen("Technical University in Poznan", "Technical University Poznan");
        whenFilterCharsThen("University of Toronto", "University Toronto");

        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Chemistry", "Department"), newArrayList("Technical", "University", "Poznan")))
                                        .thenReturn(0d);

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Universiti", "Toronto"), newArrayList("Technical", "University", "Poznan")))
                                        .thenReturn(0.5d);
        

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Chemistry", "Department"), newArrayList("University", "Toronto")))
                                        .thenReturn(0d);

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Universiti", "Toronto"), newArrayList("University", "Toronto")))
                                        .thenReturn(1d);


        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_ONE_ORG_NAME__NOT_ENOUGH_COMMON_WORDS() {
        
        // given
        
        resetOrgNames("University of Warsaw");
        
        when(organizationSectionsSplitter.splitToSections(affOrgName)).thenReturn(newArrayList("Chemistry Department", "University of Toronto"));
        
        whenFilterCharsThen("University of Warsaw", "University Warsaw");
        
        whenFilterCharsThen("Chemistry Department", "Chemistry Department");
        whenFilterCharsThen("University of Toronto", "University Toronto");
        
        
        // execute & assert

        assertFalse(voter.voteMatch(affiliation, organization));
    }

    @Test
    public void voteMatch_REAL_LIFE_EXAMPLE_VOTE_MATCH_FALSE_POSITIVE() {
        
        // given
        String affCountryCode = null;
        AffMatchAffiliation affiliation = generateRealLifeAffiliation(affCountryCode);
        
        String orgCountryCode = null;
        AffMatchOrganization organization = generateRealLifeOrganization(orgCountryCode);
        
        int wordToRemoveMaxLength = 1;
        int minNumberOfWordsInAffSection = 1;
        double minWordSimilarity = 0.85;
        
        // execute & assert
        assertTrue(checkVote(wordToRemoveMaxLength, minNumberOfWordsInAffSection, minWordSimilarity,
                affiliation, organization));
    }
    
    @Test
    public void voteMatch_REAL_LIFE_EXAMPLE_VOTE_NO_MATCH_TOO_LITLE_WORDS_IN_AFF_SECTION() {
        
        // given
        String affCountryCode = null;
        AffMatchAffiliation affiliation = generateRealLifeAffiliation(affCountryCode);
        
        String orgCountryCode = null;
        AffMatchOrganization organization = generateRealLifeOrganization(orgCountryCode);
        
        int wordToRemoveMaxLength = 1;
        int minNumberOfWordsInAffSection = 2;
        double minWordSimilarity = 0.85;
        
        // execute & assert
        assertFalse(checkVote(wordToRemoveMaxLength, minNumberOfWordsInAffSection, minWordSimilarity,
                affiliation, organization));
    }
        
    @Test
    public void voteMatch_REAL_LIFE_EXAMPLE_VOTE_NO_MATCH_DIFFERENT_COUNTRY_CODE() {
        
        // given
        String affCountryCode = "FR";
        AffMatchAffiliation affiliation = generateRealLifeAffiliation(affCountryCode);
        
        String orgCountryCode = "GR";
        AffMatchOrganization organization = generateRealLifeOrganization(orgCountryCode);
        
        int wordToRemoveMaxLength = 1;
        int minNumberOfWordsInAffSection = 1;
        double minWordSimilarity = 0.85;
        
        // execute & assert
        assertFalse(checkVote(wordToRemoveMaxLength, minNumberOfWordsInAffSection, minWordSimilarity,
                affiliation, organization));
    }
    
    @Test
    public void voteMatch_REAL_LIFE_EXAMPLE_VOTE_MATCH_SAME_COUNTRY_CODE() {
        
        // given
        String countryCode = "sharedCode";
        AffMatchAffiliation affiliation = generateRealLifeAffiliation(countryCode);
        AffMatchOrganization organization = generateRealLifeOrganization(countryCode);
        
        int wordToRemoveMaxLength = 1;
        int minNumberOfWordsInAffSection = 1;
        double minWordSimilarity = 0.85;
        
        // execute & assert
        assertTrue(checkVote(wordToRemoveMaxLength, minNumberOfWordsInAffSection, minWordSimilarity,
                affiliation, organization));
    }
    
    //------------------------ PRIVATE --------------------------
    
    private static AffMatchAffiliation generateRealLifeAffiliation(String countryCode) {
        
        AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC_ID", 1);
        affiliation.setOrganizationName("mistea, inra, montpellier supagro, universite de montpellier");
        affiliation.setCountryCode(countryCode);
        
        return affiliation;
    }
    
    private static AffMatchOrganization generateRealLifeOrganization(String countryCode) {
        
        AffMatchOrganization organization = new AffMatchOrganization("ORG_ID");
        organization.setName("athena research and innovation center in information communication & knowledge technologies");
        organization.setCountryCode(countryCode);
        
        return organization;
    }
    
    private static boolean checkVote(int wordToRemoveMaxLength, int minNumberOfWordsInAffSection, double minWordSimilarity,
            AffMatchAffiliation affiliation, AffMatchOrganization organization) {
        
        CommonAffSectionWordsVoter voter = new CommonAffSectionWordsVoter(CHARS_TO_FILTER, wordToRemoveMaxLength,
                MIN_COMMON_WORDS_RATIO, minNumberOfWordsInAffSection);
        voter.setCommonSimilarWordCalculator(new CommonSimilarWordCalculator(minWordSimilarity));
        
        return voter.voteMatch(affiliation, organization);
    }
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }
    
    private void whenFilterCharsThen(String inValue, String outValue) {
        when(stringFilter.filterCharsAndShortWords(inValue, CHARS_TO_FILTER, WORDS_TO_REMOVE_MAX_LENGTH)).thenReturn(outValue);
    }
}
