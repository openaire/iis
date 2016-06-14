package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class NameStrictWithCharFilteringMatchVoterTest {

    @InjectMocks
    private NameStrictWithCharFilteringMatchVoter voter = new NameStrictWithCharFilteringMatchVoter(Lists.newArrayList(','));
    
    @Mock
    private StringFilter stringFilter;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void voteMatch_match() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's ice creams");
        
        organization.setName("mickey mouse's ice creams");
        
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    @Test
    public void voteMatch_match_diff_only_filtered_chars() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's, ice creams");
        
        organization.setName("mickey mouse's ice creams");
        
        when(stringFilter.filterChars("mickey mouse's, ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    @Test
    public void voteMatch_dont_match_diff_org_name() {
        
        // given
        
        affiliation.setOrganizationName("donald duck's ice creams");
        
        organization.setName("mickey mouse's ice creams");
        
        when(stringFilter.filterChars("donald duck's ice creams", ImmutableList.of(','))).thenReturn("donald duck's ice creams");
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertFalse(voter.voteMatch(affiliation, organization));
        
        
    }

}
