package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

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
        
        recalculator.recalculateMatchStrength(null, voter, 2);
    
    }

    
    @Test(expected = NullPointerException.class)
    public void recalculateMatchStrength_voter_null() {
        
        // execute
        
        recalculator.recalculateMatchStrength(affMatchResult, null, 2);
    
    }

    
    @Test(expected = IllegalArgumentException.class)
    public void recalculateMatchStrength_voter_strength_less_than_one() {
        
        // execute
        
        recalculator.recalculateMatchStrength(affMatchResult, voter, 0);
    
    }
    
    
    @Test
    public void recalculateMatchStrength_match() {
        
        // given
        
        doReturn(true).when(voter).voteMatch(affiliation, organization);
        
        
        // execute
        
        AffMatchResult recalcAffMatchResult = recalculator.recalculateMatchStrength(affMatchResult, voter, 10);
        
        
        // assert
        
        assertEquals(30f, recalcAffMatchResult.getMatchStrength(), 0.01);
        assertTrue(affiliation == recalcAffMatchResult.getAffiliation());
        assertTrue(organization == recalcAffMatchResult.getOrganization());
        
    
    }
    
    
    @Test
    public void recalculateMatchStrength_DONT_match() {
        
        // given
        
        doReturn(false).when(voter).voteMatch(affiliation, organization);
        
        
        // execute
        
        AffMatchResult recalcAffMatchResult = recalculator.recalculateMatchStrength(affMatchResult, voter, 10);
        
        
        // assert
        
        assertEquals(20f, recalcAffMatchResult.getMatchStrength(), 0.01);
        assertTrue(affiliation == recalcAffMatchResult.getAffiliation());
        assertTrue(organization == recalcAffMatchResult.getOrganization());
        
    
    }

}
