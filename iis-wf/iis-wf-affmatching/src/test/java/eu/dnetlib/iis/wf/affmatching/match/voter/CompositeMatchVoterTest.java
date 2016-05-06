package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class CompositeMatchVoterTest {

    private CompositeMatchVoter compositeMatchVoter;
    
    @Mock
    private AffOrgMatchVoter voter1;
    
    @Mock
    private AffOrgMatchVoter voter2;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC1", 1);
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG1");
    
    
    @Before
    public void setup() {
        compositeMatchVoter = new CompositeMatchVoter(Lists.newArrayList(voter1, voter2));
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void voteMatch_match_when_all_matched() {
        
        // given
        when(voter1.voteMatch(affiliation, organization)).thenReturn(true);
        when(voter2.voteMatch(affiliation, organization)).thenReturn(true);
        
        // execute
        boolean returnValue = compositeMatchVoter.voteMatch(affiliation, organization);
        
        // assert
        assertTrue(returnValue);
        verify(voter1).voteMatch(affiliation, organization);
        verify(voter2).voteMatch(affiliation, organization);
        verifyNoMoreInteractions(voter1, voter2);
    }
    
    @Test
    public void voteMatch_dont_match_when_first_not_matched() {

        // given
        when(voter1.voteMatch(affiliation, organization)).thenReturn(false);
        
        // execute
        boolean returnValue = compositeMatchVoter.voteMatch(affiliation, organization);
        
        // assert
        assertFalse(returnValue);
        verify(voter1).voteMatch(affiliation, organization);
        verifyNoMoreInteractions(voter1, voter2);
    }
    
    @Test
    public void voteMatch_dont_match_when_second_not_matched() {

        // given
        when(voter1.voteMatch(affiliation, organization)).thenReturn(true);
        when(voter2.voteMatch(affiliation, organization)).thenReturn(false);
        
        // execute
        boolean returnValue = compositeMatchVoter.voteMatch(affiliation, organization);
        
        // assert
        assertFalse(returnValue);
        verify(voter1).voteMatch(affiliation, organization);
        verify(voter2).voteMatch(affiliation, organization);
        verifyNoMoreInteractions(voter1, voter2);
    }
    
}
