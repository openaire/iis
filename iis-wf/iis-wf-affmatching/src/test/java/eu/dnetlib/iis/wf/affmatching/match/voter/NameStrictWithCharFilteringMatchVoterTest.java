package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/

public class NameStrictWithCharFilteringMatchVoterTest {

    private NameStrictWithCharFilteringMatchVoter voter = new NameStrictWithCharFilteringMatchVoter(Lists.newArrayList(','));
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void voteMatch_match() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's ice creams");
        
        organization.setName("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    @Test
    public void voteMatch_match_diff_only_filtered_chars() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's, ice creams");
        
        organization.setName("mickey mouse's ice creams");
        
        
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

}
