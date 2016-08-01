package eu.dnetlib.iis.wf.affmatching.match.voter;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_AFF_WORDS;
import static eu.dnetlib.iis.wf.affmatching.match.voter.CommonWordsVoter.RatioRelation.WITH_REGARD_TO_ORG_WORDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk, Łukasz Dumiszewski
 */
@RunWith(MockitoJUnitRunner.class)
public class CommonWordsVoterTest {

    
    private double MIN_COMMON_WORDS_RATIO = 0.8;
    
    private int WORDS_TO_REMOVE_MAX_LENGTH = 2;
    
    private List<Character> CHARS_TO_FILTER = ImmutableList.of(',');
    
    
    @InjectMocks
    private CommonWordsVoter voter = new CommonWordsVoter(CHARS_TO_FILTER, WORDS_TO_REMOVE_MAX_LENGTH, MIN_COMMON_WORDS_RATIO, WITH_REGARD_TO_ORG_WORDS);
    
    @Mock
    private StringFilter stringFilter;
    
    @Mock
    private CommonSimilarWordCalculator commonSimilarWordCalculator;
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC_ID", 1);
    
    private String affOrgName = "AFF_ORG_NAME";
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG_ID");
    
    
    @Before
    public void setup() {
        
        affiliation.setOrganizationName(affOrgName);
           
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void constructor_NULL_CHARS_TO_FILTER() {
        
        // execute
        
        new CommonWordsVoter(null, 2, 0.8, WITH_REGARD_TO_ORG_WORDS);
    
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_NEGATIVE_MIN_WORD_LENGTH() {

        // execute
        
        new CommonWordsVoter(of(), -1, 0.8, WITH_REGARD_TO_ORG_WORDS);
        
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_COMMON_WORDS_RATIO() {
        
        // execute
        
        new CommonWordsVoter(of(), 2, 1.1, WITH_REGARD_TO_AFF_WORDS);
        
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_COMMON_WORDS_PERCENTAGE() {
        
        // execute
        
        new CommonWordsVoter(of(), 2, 0, WITH_REGARD_TO_AFF_WORDS);
        
    }
    
    @Test(expected = NullPointerException.class)
    public void constructor_NULL_RATIO_RELATION() {
        
        // execute
        
        new CommonWordsVoter(of(), 2, 0.8, null);
        
    }
    
    
    @Test
    public void voteMatch_ONE_ORG_NAME__ENOUGH_COMMON_WORDS__WITH_REGARD_TO_ORG() {
        
        // given
        
        resetOrgNames("University of Toronto");
        
        whenFilterCharsThen(affOrgName, "Department Chemistry University Toronto");
        
        whenFilterCharsThen("University of Toronto", "University Toronto");
        
        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("University", "Toronto"), newArrayList("Department", "Chemistry", "University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO + 0.01);
        
        
        // execute & assert

        assertTrue(voter.voteMatch(affiliation, organization));
    
    }
    
    
    @Test
    public void voteMatch_ONE_ORG_NAME__FILTERED_AFF_ORG_NAME_EMPTY() {
        
        // given
        
        resetOrgNames("University of Toronto");
        
        whenFilterCharsThen(affOrgName, "");
        
        
        // execute & assert

        assertFalse(voter.voteMatch(affiliation, organization));
    
    }

    
    @Test
    public void voteMatch_MANY_ORG_NAMES__ENOUGH_COMMON_WORDS__WITH_REGARD_TO_ORG() {
        
        // given
        
        resetOrgNames("Technical University in Poznan", "University of Toronto", "University of Warsaw");
        
        whenFilterCharsThen(affOrgName, "Department Chemistry University Toronto");
        
        whenFilterCharsThen("University of Toronto", "University Toronto");
        whenFilterCharsThen("University of Warsaw", "University Warsaw");
        whenFilterCharsThen("Technical University in Poznan", "Technical University Poznan");
        
        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("University", "Toronto"), newArrayList("Department", "Chemistry", "University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO - 0.01);

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("University", "Warsaw"), newArrayList("Department", "Chemistry", "University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO + 0.01);
        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Technical", "University", "Poznan"), newArrayList("Department", "Chemistry", "University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO - 0.01);
        

        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_ONE_ORG_NAME__NOT_ENOUGH_COMMON_WORDS__WITH_REGARD_TO_ORG() {
        
        // given
        
        resetOrgNames("University of Toronto");
        
        whenFilterCharsThen(affOrgName, "Department Chemistry University Toronto");
        
        whenFilterCharsThen("University of Toronto", "University Toronto");
        
        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("University", "Toronto"), newArrayList("Department", "Chemistry", "University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO - 0.01);
        
        
        // execute & assert

        assertFalse(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_ONE_ORG_NAME__ENOUGH_COMMON_WORDS__WITH_REGARD_TO_AFF() {
        
        // given
        
        voter.setRatioRelation(WITH_REGARD_TO_AFF_WORDS);
        
        resetOrgNames("University of Toronto");
        
        whenFilterCharsThen(affOrgName, "Department Chemistry University Toronto");
        
        whenFilterCharsThen("University of Toronto", "University Toronto");
        
        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Department", "Chemistry", "University", "Toronto"), newArrayList("University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO + 0.01);
        
        
        // execute & assert

        assertTrue(voter.voteMatch(affiliation, organization));
    
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }
    
    private void whenFilterCharsThen(String inValue, String outValue) {
        when(stringFilter.filterCharsAndShortWords(inValue, CHARS_TO_FILTER, WORDS_TO_REMOVE_MAX_LENGTH)).thenReturn(outValue);
    }
}
