package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class NameStrictWithCharFilteringMatchVoterTest {

    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction;
    
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
        
        resetOrgNames("mickey mouse's ice creams");
        
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    
    @Test
    public void voteMatch_match___many_orgs() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's ice creams");
        
        resetOrgNames("donald duck company", "goofy's kennel", "mickey mouse's ice creams");

        when(stringFilter.filterChars("donald duck company", ImmutableList.of(','))).thenReturn("donald duck company");
        when(stringFilter.filterChars("goofy's kennel", ImmutableList.of(','))).thenReturn("goofy's kennel");
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");

        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    
    
    @Test
    public void voteMatch_match_diff_only_filtered_chars() {
        
        // given
        
        affiliation.setOrganizationName("mickey mouse's, ice creams");
        
        resetOrgNames("mickey mouse's ice creams");
        
        when(stringFilter.filterChars("mickey mouse's, ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
        
        
    }
    
    @Test
    public void voteMatch_dont_match_diff_org_name() {
        
        // given
        
        affiliation.setOrganizationName("donald duck's ice creams");
        
        resetOrgNames("mickey mouse's ice creams");
        
        when(stringFilter.filterChars("donald duck's ice creams", ImmutableList.of(','))).thenReturn("donald duck's ice creams");
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertFalse(voter.voteMatch(affiliation, organization));
        
        
    }
    
    
    @Test
    public void voteMatch_dont_match_diff_org_name___many_orgs() {
        
        // given
        
        affiliation.setOrganizationName("donald duck's ice creams");
        
        resetOrgNames("minnie's house", "mickey mouse's ice creams");
        
        when(stringFilter.filterChars("donald duck's ice creams", ImmutableList.of(','))).thenReturn("donald duck's ice creams");
        when(stringFilter.filterChars("minnie's house", ImmutableList.of(','))).thenReturn("minnie's house");
        when(stringFilter.filterChars("mickey mouse's ice creams", ImmutableList.of(','))).thenReturn("mickey mouse's ice creams");
        
        
        // execute & assert
        
        assertFalse(voter.voteMatch(affiliation, organization));
        
        
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }

}
