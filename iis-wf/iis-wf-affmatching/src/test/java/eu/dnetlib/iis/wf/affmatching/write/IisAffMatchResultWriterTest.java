package eu.dnetlib.iis.wf.affmatching.write;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
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
    
    
    // SERVICES
    
    @Mock
    private AffMatchResultConverter affMatchResultConverter;
    
    @Mock
    private BestMatchedAffiliationWithinDocumentPicker bestMatchedAffiliationWithinDocumentPicker;
    
    @Mock
    private SparkAvroSaver sparkAvroSaver;
    
    
    // DATA
    
    @Mock
    private JavaRDD<AffMatchResult> affMatchResults;
    
    @Mock
    private JavaRDD<MatchedAffiliation> matchedAffiliations;
    
    @Mock
    private JavaPairRDD<CharSequence, MatchedAffiliation> matchedAffiliationsDocIdKey;
    
    @Mock
    private JavaPairRDD<CharSequence, Iterable<MatchedAffiliation>> matchedAffiliationsGroupedByDocId;
    
    @Mock
    private JavaPairRDD<CharSequence, MatchedAffiliation> matchedAffiliationsBestPicked;
    
    @Mock
    private JavaRDD<MatchedAffiliation> matchedAffiliationsBestPickedValues;
    
    
    // FUNCTIONS CAPTORS
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, MatchedAffiliation>> convertFunction;
    
    @Captor
    private ArgumentCaptor<Function<MatchedAffiliation, CharSequence>> extractDocIdFunction;
    
    @Captor
    private ArgumentCaptor<Function<Iterable<MatchedAffiliation>, MatchedAffiliation>> pickBestMatchFunction;
    
    
    
    
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
        
        doReturn(matchedAffiliations).when(affMatchResults).map(any());
        doReturn(matchedAffiliationsDocIdKey).when(matchedAffiliations).keyBy(any());
        doReturn(matchedAffiliationsGroupedByDocId).when(matchedAffiliationsDocIdKey).groupByKey();
        doReturn(matchedAffiliationsBestPicked).when(matchedAffiliationsGroupedByDocId).mapValues(any());
        doReturn(matchedAffiliationsBestPickedValues).when(matchedAffiliationsBestPicked).values();
        
        
        // execute
        
        writer.write(affMatchResults, outputPath);
        
        
        // assert
        
        verify(sparkAvroSaver).saveJavaRDD(matchedAffiliationsBestPickedValues, MatchedAffiliation.SCHEMA$, outputPath);
        
        
        verify(affMatchResults).map(convertFunction.capture());
        assertConvertFunction(convertFunction.getValue());
        
        verify(matchedAffiliations).keyBy(extractDocIdFunction.capture());
        assertExtractDocIdFunction(extractDocIdFunction.getValue());
        
        verify(matchedAffiliationsDocIdKey).groupByKey();
        
        verify(matchedAffiliationsGroupedByDocId).mapValues(pickBestMatchFunction.capture());
        assertPickBestMatchFunction(pickBestMatchFunction.getValue());
        
        verify(matchedAffiliationsBestPicked).values();
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
    
    private void assertExtractDocIdFunction(Function<MatchedAffiliation, CharSequence> function) throws Exception {
        
        // given
        MatchedAffiliation matchedAff = new MatchedAffiliation("DOC_ID", "ORG_ID", 0.6f);
        
        // execute
        CharSequence extractedDocId = function.call(matchedAff);
        
        // assert
        assertEquals("DOC_ID", extractedDocId);
    }
    
    private void assertPickBestMatchFunction(Function<Iterable<MatchedAffiliation>, MatchedAffiliation> function) throws Exception {
        
        // given
        
        MatchedAffiliation matchedAff1 = mock(MatchedAffiliation.class);
        MatchedAffiliation matchedAff2 = mock(MatchedAffiliation.class);
        
        when(bestMatchedAffiliationWithinDocumentPicker.pickBest(of(matchedAff1, matchedAff2))).thenReturn(matchedAff2);
        
        
        // execute
        
        MatchedAffiliation retMatchedAff = function.call(of(matchedAff1, matchedAff2));
        
        
        // assert
        
        assertNotNull(retMatchedAff);
        assertTrue(retMatchedAff == matchedAff2);
    }
}
