package eu.dnetlib.iis.wf.affmatching.match.voter;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CountryCodeLooseMatchVoterTest {

    private CountryCodeLooseMatchVoter voter = new CountryCodeLooseMatchVoter();
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void voteMatch_match_same_country_code() {
        
        // given
        
        affiliation.setCountryCode("pl");
        organization.setCountryCode("pl");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    @Test
    public void voteMatch_dont_match_different_country_code() {
        
        // given
        
        affiliation.setCountryCode("pl");
        organization.setCountryCode("us");
        
        
        // execute & assert
        
        assertFalse(voter.voteMatch(affiliation, organization));
        
        
    }
    
    @Test
    public void voteMatch_match_one_empty_country_code() {
        
        // given
        
        affiliation.setCountryCode("pl");
        organization.setCountryCode("");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }
    
    @Test
    public void voteMatch_match_both_empty_country_code() {
        
        // given
        
        affiliation.setCountryCode("");
        organization.setCountryCode("");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
    }

}
