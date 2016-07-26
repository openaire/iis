package eu.dnetlib.iis.wf.affmatching.match.voter;

import static com.google.common.collect.ImmutableList.of;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class FittingAffOrgWordsMatchVoterTest { 
    
    @InjectMocks
    private FittingAffOrgWordsMatchVoter voter = new FittingAffOrgWordsMatchVoter(of(','), 2, 0.8, 0.8);
    
    @Mock
    private StringFilter stringFilter;
    
    @Mock
    private StringSimilarityChecker similarityChecker;
    
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
        new FittingAffOrgWordsMatchVoter(null, 2, 0.8, 0.8);
        
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_NEGATIVE_MIN_WORD_LENGTH() {
    
        // execute
        new FittingAffOrgWordsMatchVoter(of(), -1, 0.8, 0.8);
    
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        
        // execute
        new FittingAffOrgWordsMatchVoter(of(), 2, 1.1, 0.8);
    
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        
        // execute
        new FittingAffOrgWordsMatchVoter(of(), 2, 0, 0.8);
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_WORD_SIMILARITY() {
        
        // execute
        new FittingAffOrgWordsMatchVoter(of(), 2, 0.8, 1.1);
        
    }
    
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_WORD_SIMILARITY() {
        
        // execute
        new FittingAffOrgWordsMatchVoter(of(), 2, 0.8, 0);
        
    }
    
    
    @Test
    public void voteMatch_ORG_CONTAINS_ALL_AFF_ORG_WORDS() {
        
        // given
        
        resetOrgNames("Chemistry Department, University of Toronto");
        
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("University Toronto");
        when(stringFilter.filterCharsAndShortWords("Chemistry Department, University of Toronto", ImmutableList.of(','), 2)).thenReturn("Chemistry Department University Toronto");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Toronto", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Chemistry", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Department", 0.8)).thenReturn(false);
        
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_ORG_CONTAINS_ALL_AFF_WORDS___many_orgs() {
        
        // given
        
        resetOrgNames("Technical University in Poznan", "Chemical University of Toronto", "University of Warsaw");
        
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("University Toronto");
        when(stringFilter.filterCharsAndShortWords("Chemical University of Toronto", ImmutableList.of(','), 2)).thenReturn("Chemical University Toronto");
        when(stringFilter.filterCharsAndShortWords("University of Warsaw", ImmutableList.of(','), 2)).thenReturn("University Warsaw");
        when(stringFilter.filterCharsAndShortWords("Technical University in Poznan", ImmutableList.of(','), 2)).thenReturn("Technical University Poznan");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Toronto", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Chemical", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Warsaw", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Poznan", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Technical", 0.8)).thenReturn(false);
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_ORG_CONTAINS_NOT_ALL_AFF_WORDS() {
        
        // given
        resetOrgNames("George's Hospital Medical School");
                
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("Saint George's Hospital Medical School");
        when(stringFilter.filterCharsAndShortWords("George's Hospital Medical School", ImmutableList.of(','), 2)).thenReturn("George's Hospital Medical School");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Saint", "George's", "Hospital", "Medical", "School"), "George's", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Saint", "George's", "Hospital", "Medical", "School"), "Hospital", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Saint", "George's", "Hospital", "Medical", "School"), "Medical", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Saint", "George's", "Hospital", "Medical", "School"), "School", 0.8)).thenReturn(true);
        
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    @Test
    public void voteMatch_NOT_MATCH_ORG_CONTAINS_TOO_LESS_AFF_WORDS() {
        
        // given
        resetOrgNames("Ohio Religious University");
        
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("Ohio State Business University");
        when(stringFilter.filterCharsAndShortWords("Ohio University", ImmutableList.of(','), 2)).thenReturn("Ohio University");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "State", "Business", "University"), "Ohio", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "State", "Business", "University"), "Religious", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "State", "Business", "University"), "University", 0.8)).thenReturn(true);
        
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }
}
