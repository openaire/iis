package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class NameCountryStrictEqVoterTest {

    private NameCountryStrictMatchVoter voter = new NameCountryStrictMatchVoter();
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void voteMatch_match() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's ice creams");
        affiliation.setCountryCode("us");
        
        organization.setName("mickey mouse's ice creams");
        organization.setCountryCode("us");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    
    @Test
    public void voteMatch_dont_match_diff_org_name() {
        
        // given
        
        affiliation.setOrganizationName("donald duck's ice creams");
        affiliation.setCountryCode("us");
        
        organization.setName("mickey mouse's ice creams");
        organization.setCountryCode("us");
        
        
        // execute & assert
        
        assertFalse(voter.voteMatch(affiliation, organization));
        
        
    }
    
    
    @Test
    public void voteMatch_dont_match_diff_country_codes() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's ice creams");
        affiliation.setCountryCode("ca");
        
        organization.setName("mickey mouse's ice creams");
        organization.setCountryCode("us");
        
        
        // execute & assert
        
        assertFalse(voter.voteMatch(affiliation, organization));
        
        
    }

}
