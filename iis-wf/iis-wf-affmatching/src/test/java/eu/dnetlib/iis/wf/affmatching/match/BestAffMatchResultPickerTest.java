package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class BestAffMatchResultPickerTest {

    
    @InjectMocks
    private BestAffMatchResultPicker picker = new BestAffMatchResultPicker();
    
    
    @Mock
    private AffMatchResultChooser affMatchResultChooser = new AffMatchResultChooser();
    
    @Mock
    private JavaRDD<AffMatchResult> affMatchResults;
    
    @Mock
    private JavaPairRDD<String, AffMatchResult> idAffMatchResults;
    
    @Mock
    private JavaPairRDD<String, AffMatchResult> bestIdAffMatchResults;
    
    @Mock
    private JavaRDD<AffMatchResult> bestAffMatchResults;
    
    
    @Captor
    private ArgumentCaptor<PairFunction<AffMatchResult, String, AffMatchResult>> pairFunction;
    
    @Captor
    private ArgumentCaptor<Function2<AffMatchResult, AffMatchResult, AffMatchResult>> chooseBestReduceFunction;
    
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void pickBestAffMatchResults_null() {
        
        // execute
        
        picker.pickBestAffMatchResults(null);
        
    }

    
    @Test
    public void pickBestAffMatchResults() throws Exception {
        
        // given
        
        doReturn(idAffMatchResults).when(affMatchResults).mapToPair(Mockito.any());
        doReturn(bestIdAffMatchResults).when(idAffMatchResults).reduceByKey(Mockito.any());
        doReturn(bestAffMatchResults).when(bestIdAffMatchResults).values();
        
        
        // execute
        
        picker.pickBestAffMatchResults(affMatchResults);

        
        // assert
        
        verify(affMatchResults).mapToPair(pairFunction.capture());
        verify(idAffMatchResults).reduceByKey(chooseBestReduceFunction.capture());
        
        assertPairFunction(pairFunction.getValue());
        assertChooseBestReduceFunction(chooseBestReduceFunction.getValue());
        
    }

    
    
    //------------------------ PRIVATE --------------------------
    
    
    private void assertPairFunction(PairFunction<AffMatchResult, String, AffMatchResult> function) throws Exception {
        
        // given
        
        AffMatchAffiliation aff = mock(AffMatchAffiliation.class);
        when(aff.getId()).thenReturn("ABC");
        
        AffMatchOrganization org = mock(AffMatchOrganization.class);
        AffMatchResult affMatchResult = new AffMatchResult(aff, org, 0.5f);
        
        
        // execute
        
        Tuple2<String, AffMatchResult> idAffMatchResult = function.call(affMatchResult);
        
        
        // assert
        
        assertNotNull(idAffMatchResult);
        assertTrue(affMatchResult == idAffMatchResult._2());
        assertEquals("ABC", idAffMatchResult._1());
        
    }
    
    
    private void assertChooseBestReduceFunction(Function2<AffMatchResult, AffMatchResult, AffMatchResult> function) throws Exception {
        
        // given
        
        AffMatchResult affMatchResult1 = mock(AffMatchResult.class);
        AffMatchResult affMatchResult2 = mock(AffMatchResult.class);
        doReturn(affMatchResult2).when(affMatchResultChooser).chooseBetter(affMatchResult1, affMatchResult2);
        
        // execute
        
        AffMatchResult chosenAffMatchResult = function.call(affMatchResult1, affMatchResult2);
        
        
        // assert
        
        assertNotNull(chosenAffMatchResult);
        assertTrue(affMatchResult2 == chosenAffMatchResult);
        
    }
    
    
}
