package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public class SectionedNameLevenshteinMatchVoterTest {

    private SectionedNameLevenshteinMatchVoter voter = new SectionedNameLevenshteinMatchVoter(0.9);
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void voteMatch_match_exact_single_sectioned() {

        // given
        affiliation.setOrganizationName("philipps universitat marburg");
        organization.setName("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_single_sectioned() {

        // given
        affiliation.setOrganizationName("philipps university marburg");
        organization.setName("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_exact_one_section() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps universitat marburg");
        organization.setName("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_one_section() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps university marburg");
        organization.setName("philipps universitat marburg");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_similar_multi_sections() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps university marburg");
        organization.setName("philipps universitat marburg, center for human genetic");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_not_similar_single_sectioned() {

        // given
        affiliation.setOrganizationName("philips university marburg");
        organization.setName("philipps universitat marburg"); // levenshtein distance: 3; similarity: 0.89
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_not_similar_multi_sections() {

        // given
        affiliation.setOrganizationName("center for human genetics, philipps university marburg");
        organization.setName("philipps universitat marburg, center human genetics");
        // levenshtein distance: 2; similarity: 0.92 (university section)
        // levenshtein distance: 4; similarity: 0.81 (center section)
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
}
