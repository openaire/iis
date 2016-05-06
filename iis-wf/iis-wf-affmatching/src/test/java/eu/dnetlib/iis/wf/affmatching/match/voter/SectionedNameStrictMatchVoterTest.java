package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public class SectionedNameStrictMatchVoterTest {

    private SectionedNameStrictMatchVoter voter = new SectionedNameStrictMatchVoter();
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void voteMatch_match_single_sectioned() {

        // given
        affiliation.setOrganizationName("university of alberta");
        organization.setName("university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_single_sectioned_org() {
        
        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        organization.setName("university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_multi_sectioned_org() {

        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        organization.setName("department of pediatrics, university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_inverted_multi_sectioned_org() {

        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        organization.setName("university of alberta, department of pediatrics");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_multi_sectioned_aff_to_multi_sectioned_org_SEMICOLONS() {

        // given
        affiliation.setOrganizationName("department of pediatrics; faculty of medicine and dentistry; university of alberta");
        organization.setName("department of pediatrics, university of alberta");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_aff_with_restricted_section() {

        // given
        affiliation.setOrganizationName("medshape, inc");
        organization.setName("medshape");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_org_with_restricted_section() {

        // given
        affiliation.setOrganizationName("medshape");
        organization.setName("medshape, inc");
        
        // execute & assert
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_different_single_sectioned() {

        // given
        affiliation.setOrganizationName("university of ontario");
        organization.setName("university of alberta");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_org_with_different_one_section() {

        // given
        affiliation.setOrganizationName("department of pediatrics, faculty of medicine and dentistry, university of alberta");
        organization.setName("department of psychology, university of alberta");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_dont_match_only_restricted_section() {

        // given
        affiliation.setOrganizationName("inc");
        organization.setName("inc");
        
        // execute & assert
        assertFalse(voter.voteMatch(affiliation, organization));
        
    }
    
}
