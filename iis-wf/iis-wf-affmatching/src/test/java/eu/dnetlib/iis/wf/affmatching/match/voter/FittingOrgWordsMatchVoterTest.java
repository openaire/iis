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
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class FittingOrgWordsMatchVoterTest {

    @InjectMocks
    private FittingOrgWordsMatchVoter voter = new FittingOrgWordsMatchVoter(of(','), 2, 0.8, 0.8);
    
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
        new FittingOrgWordsMatchVoter(null, 2, 0.8, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_NEGATIVE_MIN_WORD_LENGTH() {
        // execute
        new FittingOrgWordsMatchVoter(of(), -1, 0.8, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        // execute
        new FittingOrgWordsMatchVoter(of(), 2, 1.1, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        // execute
        new FittingOrgWordsMatchVoter(of(), 2, 0, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_WORD_SIMILARITY() {
        // execute
        new FittingOrgWordsMatchVoter(of(), 2, 0.8, 1.1);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_WORD_SIMILARITY() {
        // execute
        new FittingOrgWordsMatchVoter(of(), 2, 0.8, 0);
    }
    
    
    @Test
    public void voteMatch_AFF_ORG_CONTAINS_ALL_ORG_WORDS() {
        
        // given
        
        resetOrgNames("University of Toronto");
        
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("Department Chemistry University Toronto");
        when(stringFilter.filterCharsAndShortWords("University of Toronto", ImmutableList.of(','), 2)).thenReturn("University Toronto");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "Toronto", 0.8)).thenReturn(true);
        
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_AFF_ORG_CONTAINS_ALL_ORG_WORDS___many_orgs() {
        
        // given
        
        resetOrgNames("Technical University in Poznan", "University of Toronto", "University of Warsaw");
        
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("Department Chemistry University Toronto");
        when(stringFilter.filterCharsAndShortWords("University of Toronto", ImmutableList.of(','), 2)).thenReturn("University Toronto");
        when(stringFilter.filterCharsAndShortWords("University of Warsaw", ImmutableList.of(','), 2)).thenReturn("University Warsaw");
        when(stringFilter.filterCharsAndShortWords("Technical University in Poznan", ImmutableList.of(','), 2)).thenReturn("Technical University Poznan");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "Toronto", 0.8)).thenReturn(true);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "Warsaw", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "Poznan", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Department", "Chemistry", "University", "Toronto"), "Technical", 0.8)).thenReturn(false);
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_AFF_ORG_CONTAINS_NOT_ALL_ORG_WORDS() {
        
        // given
        resetOrgNames("Saint George's Hospital Medical School");
                
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("George's Hospital Medical School");
        when(stringFilter.filterCharsAndShortWords("Saint George's Hospital Medical School", ImmutableList.of(','), 2)).thenReturn("Saint George's Hospital Medical School");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Saint", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "George's", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Hospital", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Medical", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "School", 0.8)).thenReturn(true);
        
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    @Test
    public void voteMatch_NOT_MATCH_AFF_ORG_CONTAINS_TOO_LESS_ORG_WORDS() {
        
        // given
        resetOrgNames("Ohio State University");
        
        when(stringFilter.filterCharsAndShortWords(affOrgName, ImmutableList.of(','), 2)).thenReturn("Ohio University");
        when(stringFilter.filterCharsAndShortWords("Ohio State University", ImmutableList.of(','), 2)).thenReturn("Ohio State University");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "University"), "Ohio", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "University"), "State", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "University"), "University", 0.8)).thenReturn(true);
        
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }
}
