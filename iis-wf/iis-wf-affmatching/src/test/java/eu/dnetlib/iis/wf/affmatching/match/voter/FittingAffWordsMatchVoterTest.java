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
public class FittingAffWordsMatchVoterTest {

    private FittingAffWordsMatchVoter voter = new FittingAffWordsMatchVoter(of(','), 2, 0.8, 0.8);
    
    
    private AffMatchAffiliation aff = new AffMatchAffiliation("DOC_ID", 1);
    
    private AffMatchOrganization org = new AffMatchOrganization("ORG_ID");
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void constructor_NULL_CHARS_TO_FILTER() {
        // execute
        new FittingAffWordsMatchVoter(null, 2, 0.8, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_NEGATIVE_MIN_WORD_LENGTH() {
        // execute
        new FittingAffWordsMatchVoter(of(), -1, 0.8, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        // execute
        new FittingAffWordsMatchVoter(of(), 2, 1.1, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_ORG_WORDS_PERCENTAGE() {
        // execute
        new FittingAffWordsMatchVoter(of(), 2, 0, 0.8);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_HIGH_MIN_FITTING_WORD_SIMILARITY() {
        // execute
        new FittingAffWordsMatchVoter(of(), 2, 0.8, 1.1);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void constructor_TOO_LOW_MIN_FITTING_WORD_SIMILARITY() {
        // execute
        new FittingAffWordsMatchVoter(of(), 2, 0.8, 0);
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
        aff.setOrganizationName("Max Planck Institute");
        org.setName("Institute Max Planck");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    
    @Test
    public void voteMatch_ORG_ADDITIONAL_WORDS() {
        
        // given
        aff.setOrganizationName("Weizmann Institute");
        org.setName("The Weizmann Institute of Science");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_AFF_ADDITIONAL_SECTION() {
        
        // given
        aff.setOrganizationName("Department of Chemistry, The Weizmann Institute of Science");
        org.setName("The Weizmann Institute of Science");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    
    @Test
    public void voteMatch_NOT_ALL_AFF_WORDS_MATCHED() {
        
        // given
        aff.setOrganizationName("Institute of Medical and Biomedical Education, Saint George's Hospital Medical School"); // 4 out of 5 words matched
        org.setName("George's Hospital Medical School");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_ORG_NAME_WITH_SHORT_WORDS() {
        
        // given
        aff.setOrganizationName("po i u University yt of re Toronto w qa");
        org.setName("a b c University d of e Toronto fg hi");
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_SIMILAR_WORDS() {
        
        // given
        aff.setOrganizationName("Wydział Nauk Politycznych i Dziennikarstwa Uniwersytetu, Uniwersytet Adama Mickiewicza");
        org.setName("Adam Mickiewicz University");
        
        // Jaro-Winkler similarity: [university] [uniwersytet] 0.86
        // Jaro-Winkler similarity: [adam] [adama] 0.96
        // Jaro-Winkler similarity: [mickiewicz] [mickiewicza] 0.98
        
        // execute & assert
        assertTrue(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_NOT_MATCH_TOO_LESS_MATCHED_WORDS() {
        
        // given
        aff.setOrganizationName("Molecular and Cell Biology Program, Ohio State University"); // 2 out of 3 words matched - too low
        org.setName("Ohio University");
        
        // execute & assert
        assertFalse(voter.voteMatch(aff, org));
    }
    
    @Test
    public void voteMatch_NOT_MATCH_TOO_LOW_SIMILARITY() {
        
        // given
        aff.setOrganizationName("Department of Photonics Engineering, Technical University of Denmark");
        org.setName("Danmarks Tekniske Universitet");
        
        // Jaro-Winkler similarity: [denmark] [danmarks] 0.83
        // Jaro-Winkler similarity: [technical] [tekniske] 0.72 - too low
        // Jaro-Winkler similarity: [university] [universitet] 0.94
        
        // execute & assert
        assertFalse(voter.voteMatch(aff, org));
    }
}
