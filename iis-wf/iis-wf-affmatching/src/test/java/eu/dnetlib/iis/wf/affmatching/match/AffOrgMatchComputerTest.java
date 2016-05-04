package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class AffOrgMatchComputerTest {

    
    @InjectMocks
    private AffOrgMatchComputer affOrgMatchComputer = new AffOrgMatchComputer();
    
    @Mock
    private List<AffOrgMatchVoter> affOrgMatchVoters;
    
    @Mock
    private AffOrgMatchVoterStrengthCalculator voterStrengthCalculator;

    @Mock
    private AffOrgMatchStrengthRecalculator affOrgMatchStrengthRecalculator;
 
    @Mock
    private AffOrgMatchVoter affOrgMatchVoter1;
    
    @Mock
    private AffOrgMatchVoter affOrgMatchVoter2;
    
    @Mock
    private JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrgs;

    @Mock
    private JavaRDD<AffMatchResult> affMatchResults;
    
    @Mock
    private JavaRDD<AffMatchResult> recalcAffMatchResults1;
    
    @Mock
    private JavaRDD<AffMatchResult> recalcAffMatchResults2;

    @Mock
    private JavaRDD<AffMatchResult> filteredAffMatchResults;
    
    @Mock
    private JavaRDD<AffMatchResult> unifiedStrengthAffMatchResults;
    
    
    @Captor
    private ArgumentCaptor<Function<Tuple2<AffMatchAffiliation, AffMatchOrganization>, AffMatchResult>> mapToMatchResultFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, AffMatchResult>> recalcMatchStrengthFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, Boolean>> filterAffMatchResultFunction;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, AffMatchResult>> unifyMatchStrengthFunction;
    
    
    
    @Before
    public void before() {
        
        doReturn(2).when(affOrgMatchVoters).size();
        doReturn(affOrgMatchVoter1).when(affOrgMatchVoters).get(0);
        doReturn(affOrgMatchVoter2).when(affOrgMatchVoters).get(1);
        
    }
    

    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void computeMatches_null() {
        
        // execute
        
        affOrgMatchComputer.computeMatches(null);
        
    }
    

    @Test
    public void computeMatches() throws Exception {
        
        // given
        
        doReturn(affMatchResults).when(joinedAffOrgs).map(Mockito.any());
        doReturn(8).when(voterStrengthCalculator).calculateStrength(0, 2);
        doReturn(4).when(voterStrengthCalculator).calculateStrength(1, 2);
        doReturn(recalcAffMatchResults1).when(affMatchResults).map(Mockito.any());
        doReturn(recalcAffMatchResults2).when(recalcAffMatchResults1).map(Mockito.any());
        doReturn(filteredAffMatchResults).when(recalcAffMatchResults2).filter(Mockito.any());
        doReturn(unifiedStrengthAffMatchResults).when(filteredAffMatchResults).map(Mockito.any());
        
        
        // execute
        
        JavaRDD<AffMatchResult> retAffMatchResults = affOrgMatchComputer.computeMatches(joinedAffOrgs);
        
        
        // assert
        
        assertNotNull(retAffMatchResults);
        assertTrue(unifiedStrengthAffMatchResults == retAffMatchResults);
        
        verify(joinedAffOrgs).map(mapToMatchResultFunction.capture());
        assertMapToMatchResultFunction(mapToMatchResultFunction.getValue());
        
        verify(affMatchResults).map(recalcMatchStrengthFunction.capture());
        assertRecalcMatchStrengthFunction(recalcMatchStrengthFunction.getValue(), affOrgMatchVoter1, 8);
        
        verify(recalcAffMatchResults1).map(recalcMatchStrengthFunction.capture());
        assertRecalcMatchStrengthFunction(recalcMatchStrengthFunction.getValue(), affOrgMatchVoter2, 4);
        
        verify(recalcAffMatchResults2).filter(filterAffMatchResultFunction.capture());
        assertFilterRecalcAffMatchResultFunction(filterAffMatchResultFunction.getValue());
        
        verify(filteredAffMatchResults).map(unifyMatchStrengthFunction.capture());
        assertUnifyAffMatchResultFunction(unifyMatchStrengthFunction.getValue());
    }

    
    
    //------------------------ PRIVATE --------------------------
    

    private void assertMapToMatchResultFunction(Function<Tuple2<AffMatchAffiliation, AffMatchOrganization>, AffMatchResult> function) throws Exception {
        
        // given
        
        AffMatchAffiliation affiliation = mock(AffMatchAffiliation.class);
        AffMatchOrganization organization = mock(AffMatchOrganization.class);
        
        
        // execute
        
        AffMatchResult affMatchResult = function.call(new Tuple2<>(affiliation, organization));
        
        
        // assert
        
        assertTrue(affiliation == affMatchResult.getAffiliation());
        assertTrue(organization == affMatchResult.getOrganization());
        
        
    }
    
    
    private void assertRecalcMatchStrengthFunction(Function<AffMatchResult, AffMatchResult> function, AffOrgMatchVoter voter, int voterStrength) throws Exception {
        
        // given
        
        AffMatchResult affMatchResult = mock(AffMatchResult.class);
        AffMatchResult expectedRecalcAffMatchResult = mock(AffMatchResult.class);
        
        doReturn(expectedRecalcAffMatchResult).when(affOrgMatchStrengthRecalculator).recalculateMatchStrength(affMatchResult, voter, voterStrength);
        
        
        // execute
        
        AffMatchResult recalcAffMatchResult = function.call(affMatchResult);
        
        
        // assert
        
        assertTrue(expectedRecalcAffMatchResult == recalcAffMatchResult);
        
    }
    
    
    private void assertFilterRecalcAffMatchResultFunction(Function<AffMatchResult, Boolean> function) throws Exception {
        
        // given
        
        AffMatchResult affMatchResult1 = mock(AffMatchResult.class);
        doReturn(5f).when(affMatchResult1).getMatchStrength();

        AffMatchResult affMatchResult2 = mock(AffMatchResult.class);
        doReturn(0f).when(affMatchResult2).getMatchStrength();
        
        
        // execute & assert
        
        assertTrue(function.call(affMatchResult1));
        assertFalse(function.call(affMatchResult2));
        
    }
    
    
    private void assertUnifyAffMatchResultFunction(Function<AffMatchResult, AffMatchResult> function) throws Exception {
        
        // given
        
        AffMatchResult affMatchResult = mock(AffMatchResult.class);
        AffMatchAffiliation affiliation = mock(AffMatchAffiliation.class);
        AffMatchOrganization organization = mock(AffMatchOrganization.class);
        
        doReturn(affiliation).when(affMatchResult).getAffiliation();
        doReturn(organization).when(affMatchResult).getOrganization();
        doReturn(4f).when(affMatchResult).getMatchStrength();
        
        
        // execute 
        
        AffMatchResult recalcAffMatchResult = function.call(affMatchResult);
        
        
        // assert
        
        assertNotNull(recalcAffMatchResult);
        assertTrue(affMatchResult != recalcAffMatchResult);
        assertTrue(affiliation == recalcAffMatchResult.getAffiliation());
        assertTrue(organization == recalcAffMatchResult.getOrganization());
        assertEquals(4/12f, recalcAffMatchResult.getMatchStrength(), 0.001);
        
    }
    
    
}
