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
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class FittingAffOrgSectionWordsMatchVoterTest {

    @InjectMocks
    private FittingAffOrgSectionWordsMatchVoter voter = new FittingAffOrgSectionWordsMatchVoter(of(','), 2, 0.8, 0.8);
    
    @Mock
    private OrganizationSectionsSplitter sectionsSplitter;
    
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
        new FittingAffOrgSectionWordsMatchVoter(null, 2, 0.8, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_NEGATIVE_MIN_WORD_LENGTH() {
        // execute
        new FittingAffOrgSectionWordsMatchVoter(of(), -1, 0.8, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        // execute
        new FittingAffOrgSectionWordsMatchVoter(of(), 2, 1.1, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        // execute
        new FittingAffOrgSectionWordsMatchVoter(of(), 2, 0, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_WORD_SIMILARITY() {
        // execute
        new FittingAffOrgSectionWordsMatchVoter(of(), 2, 0.8, 1.1);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_WORD_SIMILARITY() {
        // execute
        new FittingAffOrgSectionWordsMatchVoter(of(), 2, 0.8, 0);
    }
    
    
    @Test
    public void voteMatch_AFF_SECTION_CONTAINS_ALL_ORG_WORDS() {
        
        // given
        
        resetOrgNames("University of Toronto");
        
        when(sectionsSplitter.splitToSections(affOrgName)).thenReturn(ImmutableList.of("section1", "section2"));
        
        when(stringFilter.filterCharsAndShortWords("section1", ImmutableList.of(','), 2)).thenReturn("Department Chemistry");
        when(stringFilter.filterCharsAndShortWords("section2", ImmutableList.of(','), 2)).thenReturn("University Toronto");
        
        when(stringFilter.filterCharsAndShortWords("University of Toronto", ImmutableList.of(','), 2)).thenReturn("University Toronto");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Department", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Chemistry", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Toronto", 0.8)).thenReturn(true);
        
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    @Test
    public void voteMatch_AFF_SECTION_CONTAINS_ALL_ORG_WORDS__many_orgs() {
        
        // given
        
        resetOrgNames("University of Warsaw","University of Toronto", "Moscow University of Technology");
        
        when(sectionsSplitter.splitToSections(affOrgName)).thenReturn(ImmutableList.of("section1", "section2"));
        
        when(stringFilter.filterCharsAndShortWords("section1", ImmutableList.of(','), 2)).thenReturn("Department Chemistry");
        when(stringFilter.filterCharsAndShortWords("section2", ImmutableList.of(','), 2)).thenReturn("University Toronto");
        
        when(stringFilter.filterCharsAndShortWords("University of Toronto", ImmutableList.of(','), 2)).thenReturn("University Toronto");
        when(stringFilter.filterCharsAndShortWords("University of Warsaw", ImmutableList.of(','), 2)).thenReturn("University Warsaw");
        when(stringFilter.filterCharsAndShortWords("Moscow University of Technology", ImmutableList.of(','), 2)).thenReturn("Moscow University Technology");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Department", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Chemistry", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Toronto"), "Toronto", 0.8)).thenReturn(true);
        
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Warsaw"), "Department", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Warsaw"), "Chemistry", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Warsaw"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("University", "Warsaw"), "Toronto", 0.8)).thenReturn(false);
        
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Moscow", "University", "Technology"), "Department", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Moscow", "University", "Technology"), "Chemistry", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Moscow", "University", "Technology"), "University", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Moscow", "University", "Technology"), "Toronto", 0.8)).thenReturn(false);
        
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_AFF_SECTION_CONTAINS_NOT_ALL_ORG_WORDS() {
        
        // given
        resetOrgNames("George's Hospital Medical School");
        
        when(sectionsSplitter.splitToSections(affOrgName)).thenReturn(ImmutableList.of("section1", "section2"));
        
        when(stringFilter.filterCharsAndShortWords("section1", ImmutableList.of(','), 2)).thenReturn("Institute Medical Biomedical Education");
        when(stringFilter.filterCharsAndShortWords("section2", ImmutableList.of(','), 2)).thenReturn("Saint George's Hospital Medical School");
        
        when(stringFilter.filterCharsAndShortWords("George's Hospital Medical School", ImmutableList.of(','), 2)).thenReturn("George's Hospital Medical School");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Institute", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Medical", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Biomedical", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Education", 0.8)).thenReturn(false);
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Saint", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "George's", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Hospital", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "Medical", 0.8)).thenReturn(true);
        when(similarityChecker.containSimilarString(ImmutableSet.of("George's", "Hospital", "Medical", "School"), "School", 0.8)).thenReturn(true);
        
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    @Test
    public void voteMatch_NOT_MATCH_AFF_SECTION_CONTAINS_TOO_LESS_ORG_WORDS() {
        
        // given
        resetOrgNames("Ohio University");
        
        when(sectionsSplitter.splitToSections(affOrgName)).thenReturn(ImmutableList.of("section1", "section2"));
        
        when(stringFilter.filterCharsAndShortWords("section1", ImmutableList.of(','), 2)).thenReturn("Molecular Cell Biology Program");
        when(stringFilter.filterCharsAndShortWords("section2", ImmutableList.of(','), 2)).thenReturn("Ohio State University");
        
        when(stringFilter.filterCharsAndShortWords("Ohio University", ImmutableList.of(','), 2)).thenReturn("Ohio University");
        
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "University"), "Molecular", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "University"), "Cell", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "University"), "Biology", 0.8)).thenReturn(false);
        when(similarityChecker.containSimilarString(ImmutableSet.of("Ohio", "University"), "Program", 0.8)).thenReturn(false);
        
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
