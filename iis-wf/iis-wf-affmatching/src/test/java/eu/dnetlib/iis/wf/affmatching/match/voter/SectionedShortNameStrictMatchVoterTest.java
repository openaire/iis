package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public class SectionedShortNameStrictMatchVoterTest {

    private SectionedShortNameStrictMatchVoter voter = new SectionedShortNameStrictMatchVoter();
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void voteMatch_match_single_sectioned() {

        // given
        affiliation.setOrganizationName("ucla");
        organization.setShortName("ucla");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_single_sectioned_org() {
        
        // given
        affiliation.setOrganizationName("school of medicine, ucla");
        organization.setShortName("ucla");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_multi_sectioned_org() {

        // given
        affiliation.setOrganizationName("school of medicine, ucla");
        organization.setShortName("school of medicine, ucla");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_inverted_multi_sectioned_org() {

        // given
        affiliation.setOrganizationName("school of medicine, ucla");
        organization.setShortName("school of medicine, ucla");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_multi_sectioned_org_SEMICOLONS() {

        // given
        affiliation.setOrganizationName("school of medicine; ucla");
        organization.setShortName("school of medicine, ucla");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_aff_with_restricted_section() {

        // given
        affiliation.setOrganizationName("medshape, inc");
        organization.setShortName("medshape");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_org_with_restricted_section() {

        // given
        affiliation.setOrganizationName("medshape");
        organization.setShortName("medshape, inc");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_different_single_sectioned() {

        // given
        affiliation.setOrganizationName("ucla");
        organization.setShortName("inra");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_org_with_different_one_section() {

        // given
        affiliation.setOrganizationName("school of medicine, ucla");
        organization.setShortName("school of chemistry, ucla");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_only_restricted_section() {

        // given
        affiliation.setOrganizationName("inc");
        organization.setShortName("inc");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
}
