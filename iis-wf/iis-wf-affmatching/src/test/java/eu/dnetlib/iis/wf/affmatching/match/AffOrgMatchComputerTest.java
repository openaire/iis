package eu.dnetlib.iis.wf.affmatching.match;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.wf.affmatching.match.voter.AffOrgMatchVoter;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Tuple2;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class AffOrgMatchComputerTest {

    
    @InjectMocks
    private AffOrgMatchComputer affOrgMatchComputer = new AffOrgMatchComputer();
    
    @Mock
    private AffOrgMatchStrengthRecalculator affOrgMatchStrengthRecalculator;
 
    @Mock
    private AffOrgMatchVoter affOrgMatchVoter1;
    
    @Mock
    private AffOrgMatchVoter affOrgMatchVoter2;
    
    private List<AffOrgMatchVoter> affOrgMatchVoters;
        
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
    
    
    @BeforeEach
    public void before() {
        
        affOrgMatchVoters = Lists.newArrayList(affOrgMatchVoter1, affOrgMatchVoter2);
        
        affOrgMatchComputer.setAffOrgMatchVoters(affOrgMatchVoters);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void computeMatches_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> affOrgMatchComputer.computeMatches(null));

    }
    
    
    @Test
    public void computeMatches_no_voters() {
        
        // given
        
        affOrgMatchComputer.setAffOrgMatchVoters(Lists.newArrayList());
        
        
        // execute
        assertThrows(IllegalStateException.class, () -> affOrgMatchComputer.computeMatches(joinedAffOrgs));

    }
    

    @Test
    public void computeMatches() throws Exception {
        
        // given
        
        doReturn(affMatchResults).when(joinedAffOrgs).map(Mockito.any());
        doReturn(recalcAffMatchResults1).when(affMatchResults).map(Mockito.any());
        doReturn(recalcAffMatchResults2).when(recalcAffMatchResults1).map(Mockito.any());
        doReturn(filteredAffMatchResults).when(recalcAffMatchResults2).filter(Mockito.any());
        
        
        // execute
        
        JavaRDD<AffMatchResult> retAffMatchResults = affOrgMatchComputer.computeMatches(joinedAffOrgs);
        
        
        // assert
        
        assertNotNull(retAffMatchResults);
        assertSame(filteredAffMatchResults, retAffMatchResults);
        
        verify(joinedAffOrgs).map(mapToMatchResultFunction.capture());
        assertMapToMatchResultFunction(mapToMatchResultFunction.getValue());
        
        verify(affMatchResults).map(recalcMatchStrengthFunction.capture());
        assertRecalcMatchStrengthFunction(recalcMatchStrengthFunction.getValue(), affOrgMatchVoter1);
        
        verify(recalcAffMatchResults1).map(recalcMatchStrengthFunction.capture());
        assertRecalcMatchStrengthFunction(recalcMatchStrengthFunction.getValue(), affOrgMatchVoter2);
        
        verify(recalcAffMatchResults2).filter(filterAffMatchResultFunction.capture());
        assertFilterRecalcAffMatchResultFunction(filterAffMatchResultFunction.getValue());
        
    }

    
    
    //------------------------ PRIVATE --------------------------
    

    private void assertMapToMatchResultFunction(Function<Tuple2<AffMatchAffiliation, AffMatchOrganization>, AffMatchResult> function) throws Exception {
        
        // given
        
        AffMatchAffiliation affiliation = mock(AffMatchAffiliation.class);
        AffMatchOrganization organization = mock(AffMatchOrganization.class);
        
        
        // execute
        
        AffMatchResult affMatchResult = function.call(new Tuple2<>(affiliation, organization));
        
        
        // assert

        assertSame(affiliation, affMatchResult.getAffiliation());
        assertSame(organization, affMatchResult.getOrganization());
        
        
    }
    
    
    private void assertRecalcMatchStrengthFunction(Function<AffMatchResult, AffMatchResult> function, AffOrgMatchVoter voter) throws Exception {
        
        // given
        
        AffMatchResult affMatchResult = mock(AffMatchResult.class);
        AffMatchResult expectedRecalcAffMatchResult = mock(AffMatchResult.class);
        
        doReturn(expectedRecalcAffMatchResult).when(affOrgMatchStrengthRecalculator).recalculateMatchStrength(affMatchResult, voter);
        
        
        // execute
        
        AffMatchResult recalcAffMatchResult = function.call(affMatchResult);
        
        
        // assert

        assertSame(expectedRecalcAffMatchResult, recalcAffMatchResult);
        
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
    
    
    
    
    
}
