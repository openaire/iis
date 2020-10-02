package eu.dnetlib.iis.wf.affmatching.match;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith({MockitoExtension.class})
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
    
    
    @BeforeEach
    public void before() {
        
        lenient().doReturn(20f).when(affMatchResult).getMatchStrength();
        lenient().doReturn(affiliation).when(affMatchResult).getAffiliation();
        lenient().doReturn(organization).when(affMatchResult).getOrganization();
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void recalculateMatchStrength_affMatchResult_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> recalculator.recalculateMatchStrength(null, voter));
    
    }

    
    @Test
    public void recalculateMatchStrength_voter_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> recalculator.recalculateMatchStrength(affMatchResult, null));

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
        assertSame(affiliation, recalcAffMatchResult.getAffiliation());
        assertSame(organization, recalcAffMatchResult.getOrganization());
        
    
    }
    
    
    @Test
    public void recalculateMatchStrength_DONT_match() {
        
        // given
        
        when(voter.voteMatch(affiliation, organization)).thenReturn(false);
        when(affMatchResult.getMatchStrength()).thenReturn(0.7f);

        
        // execute
        
        AffMatchResult recalcAffMatchResult = recalculator.recalculateMatchStrength(affMatchResult, voter);
        
        
        // assert
        
        assertEquals(0.7f, recalcAffMatchResult.getMatchStrength(), 0.0001);
        assertSame(affiliation, recalcAffMatchResult.getAffiliation());
        assertSame(organization, recalcAffMatchResult.getOrganization());
        
    
    }

}
