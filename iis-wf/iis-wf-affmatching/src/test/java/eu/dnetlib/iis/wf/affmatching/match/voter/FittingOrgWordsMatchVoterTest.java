package eu.dnetlib.iis.wf.affmatching.match.voter;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public class FittingOrgWordsMatchVoterTest {

    private FittingOrgWordsMatchVoter voter = new FittingOrgWordsMatchVoter(of(','), 2, 0.8, 0.8);
    
    
    private AffMatchAffiliation aff = new AffMatchAffiliation("DOC_ID", 1);
    
    private AffMatchOrganization org = new AffMatchOrganization("ORG_ID");
    
    
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
    public void voteMatch_EQUAL_ORG_NAMES() {
        
        // given
        aff.setOrganizationName("University of Toronto");
        org.setName("University of Toronto");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_REARRENGED_ORG_WORDS() {
        
        // given
        aff.setOrganizationName("Institute Max Planck");
        org.setName("Max Planck Institute");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_AFF_ADDITIONAL_WORDS() {
        
        // given
        aff.setOrganizationName("Department of Condensed Matter Physics, The Weizmann Institute of Science");
        org.setName("Weizmann Institute");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_NOT_ALL_ORG_WORDS_MATCHED() {
        
        // given
        aff.setOrganizationName("George's Hospital Medical School");
        org.setName("Saint George's Hospital Medical School"); // 4 out of 5 words matched
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_ORG_NAME_WITH_SHORT_WORDS() {
        
        // given
        aff.setOrganizationName("University of Toronto");
        org.setName("a b c University d of e Toronto fg hi");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_SIMILAR_WORDS() {
        
        // given
        aff.setOrganizationName("Adam Mickiewicz University");
        org.setName("Uniwersytet Adama Mickiewicza");
        
        // Jaro-Winkler similarity: [university] [uniwersytet] 0.86
        // Jaro-Winkler similarity: [adam] [adama] 0.96
        // Jaro-Winkler similarity: [mickiewicz] [mickiewicza] 0.98
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_NOT_MATCH_TOO_LESS_MATCHED_WORDS() {
        
        // given
        aff.setOrganizationName("Molecular and Cell Biology Program, Ohio University");
        org.setName("Ohio State University"); // 2 out of 3 words matched - too low
        
        // execute & assert
        assertFalse(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_NOT_MATCH_TOO_LOW_SIMILARITY() {
        
        // given
        aff.setOrganizationName("Technical University of Denmark");
        org.setName("Danmarks Tekniske Universitet");
        
        // Jaro-Winkler similarity: [denmark] [danmarks] 0.83
        // Jaro-Winkler similarity: [technical] [tekniske] 0.72 - too low
        // Jaro-Winkler similarity: [university] [universitet] 0.94
        
        // execute & assert
        assertFalse(voter.voteMatch(aff, org));
    }
    
}
