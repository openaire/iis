package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class AffOrgMatchStrengthRecalculatorTest {

    
    private AffOrgMatchStrengthRecalculator recalculator = new AffOrgMatchStrengthRecalculator();
    
    @Mock
    private AffMatchResult affMatchResult;
    
    @Mock
    private AffOrgMatchVoter voter;
    
    @Mock
    private AffMatchAffiliation affiliation;
    
    @Mock
    private AffMatchOrganization organization;
    
    
    @Before
    public void before() {
        
        doReturn(20f).when(affMatchResult).getMatchStrength();
        doReturn(affiliation).when(affMatchResult).getAffiliation();
        doReturn(organization).when(affMatchResult).getOrganization();
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void recalculateMatchStrength_affMatchResult_null() {
        
        // execute
        
        recalculator.recalculateMatchStrength(null, voter);
    
    }

    
    @Test(expected = NullPointerException.class)
    public void recalculateMatchStrength_voter_null() {
        
        // execute
        
        recalculator.recalculateMatchStrength(affMatchResult, null);
    
    }

    
    @Test
    public void recalculateMatchStrength_match() {
        
        // given
        
        when(voter.voteMatch(affiliation, organization)).thenReturn(true);
        when(affMatchResult.getMatchStrength()).thenReturn(0.7f);
        when(voter.getMatchStrength()).thenReturn(0.5f);
        
        // execute
        
        AffMatchResult recalcAffMatchResult = recalculator.recalculateMatchStrength(affMatchResult, voter);
        
        
        // assert
        
        assertEquals(0.85f, recalcAffMatchResult.getMatchStrength(), 0.0001f);
        assertTrue(affiliation == recalcAffMatchResult.getAffiliation());
        assertTrue(organization == recalcAffMatchResult.getOrganization());
        
    
    }
    
    
    @Test
    public void recalculateMatchStrength_DONT_match() {
        
        // given
        
        when(voter.voteMatch(affiliation, organization)).thenReturn(false);
        when(affMatchResult.getMatchStrength()).thenReturn(0.7f);
        when(voter.getMatchStrength()).thenReturn(0.5f);
        
        
        // execute
        
        AffMatchResult recalcAffMatchResult = recalculator.recalculateMatchStrength(affMatchResult, voter);
        
        
        // assert
        
        assertEquals(0.7f, recalcAffMatchResult.getMatchStrength(), 0.0001);
        assertTrue(affiliation == recalcAffMatchResult.getAffiliation());
        assertTrue(organization == recalcAffMatchResult.getOrganization());
        
    
    }

}
