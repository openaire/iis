package eu.dnetlib.iis.wf.affmatching.write;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.MatchedAffiliation;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
* @author Łukasz Dumiszewski
*/

@RunWith(MockitoJUnitRunner.class)
public class IisAffMatchResultWriterTest {

    
    @InjectMocks
    private IisAffMatchResultWriter writer = new IisAffMatchResultWriter();
    
    @Mock
    private AffMatchResultConverter affMatchResultConverter;
    
    @Mock
    private SparkAvroSaver sparkAvroSaver;
    
    @Mock
    private JavaRDD<AffMatchResult> affMatchResults;
    
    @Mock
    private JavaRDD<MatchedAffiliation> matchedAffiliations;
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, MatchedAffiliation>> convertFunction;
    
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void write_matchedAffOrgs_null() {
        
        // execute
        
        writer.write(null, "/aaa");
        
    }
    

    @Test(expected = IllegalArgumentException.class)
    public void write_outputPath_blank() {
        
        // execute
        
        writer.write(affMatchResults, "  ");
        
    }
    
    @Test
    public void write() throws Exception {
        
        // given
        
        String outputPath = "/data/matchedAffiliations";
        
        doReturn(matchedAffiliations).when(affMatchResults).map(Mockito.any());
        
        
        // execute
        
        writer.write(affMatchResults, outputPath);
        
        
        // assert
        
        verify(sparkAvroSaver).saveJavaRDD(matchedAffiliations, MatchedAffiliation.SCHEMA$, outputPath);
        
        verify(affMatchResults).map(convertFunction.capture());
        
        assertConvertFunction(convertFunction.getValue());
    }
    
    
    
    //------------------------ PRIVATE --------------------------

    
    private void assertConvertFunction(Function<AffMatchResult, MatchedAffiliation> function) throws Exception {

        // given
        
        AffMatchResult affMatchResult = mock(AffMatchResult.class);
        MatchedAffiliation matchedAff = mock(MatchedAffiliation.class);
        
        when(affMatchResultConverter.convert(affMatchResult)).thenReturn(matchedAff);

        
        // execute
        
        MatchedAffiliation retMatchedAff = function.call(affMatchResult);

        
        // assert
        
        assertNotNull(retMatchedAff);
        assertTrue(matchedAff == retMatchedAff);
        
    }
}
