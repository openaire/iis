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
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
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
    private BestMatchedOrganizationWithinDocumentPicker bestMatchedOrganizationWithinDocumentPicker;
    
    @Mock
    private SparkAvroSaver sparkAvroSaver;
    
    
    // DATA
    
    @Mock
    private JavaRDD<AffMatchResult> affMatchResults;
    
    @Mock
    private JavaRDD<MatchedOrganization> matchedOrganizations;
    
    @Mock
    private JavaPairRDD<CharSequence, MatchedOrganization> matchedOrganizationsDocIdKey;
    
    @Mock
    private JavaPairRDD<CharSequence, Iterable<MatchedOrganization>> matchedOrganizationsGroupedByDocId;
    
    @Mock
    private JavaPairRDD<CharSequence, MatchedOrganization> matchedOrganizationsBestPicked;
    
    @Mock
    private JavaRDD<MatchedOrganization> matchedOrganizationsBestPickedValues;
    
    
    // FUNCTIONS CAPTORS
    
    @Captor
    private ArgumentCaptor<Function<AffMatchResult, MatchedOrganization>> convertFunction;
    
    @Captor
    private ArgumentCaptor<Function<MatchedOrganization, CharSequence>> extractDocIdFunction;
    
    @Captor
    private ArgumentCaptor<Function<Iterable<MatchedOrganization>, MatchedOrganization>> pickBestMatchFunction;
    
    
    
    
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
        
        doReturn(matchedOrganizations).when(affMatchResults).map(any());
        doReturn(matchedOrganizationsDocIdKey).when(matchedOrganizations).keyBy(any());
        doReturn(matchedOrganizationsGroupedByDocId).when(matchedOrganizationsDocIdKey).groupByKey();
        doReturn(matchedOrganizationsBestPicked).when(matchedOrganizationsGroupedByDocId).mapValues(any());
        doReturn(matchedOrganizationsBestPickedValues).when(matchedOrganizationsBestPicked).values();
        
        
        // execute
        
        writer.write(affMatchResults, outputPath);
        
        
        // assert
        
        verify(sparkAvroSaver).saveJavaRDD(matchedOrganizationsBestPickedValues, MatchedOrganization.SCHEMA$, outputPath);
        
        
        verify(affMatchResults).map(convertFunction.capture());
        assertConvertFunction(convertFunction.getValue());
        
        verify(matchedOrganizations).keyBy(extractDocIdFunction.capture());
        assertExtractDocIdFunction(extractDocIdFunction.getValue());
        
        verify(matchedOrganizationsDocIdKey).groupByKey();
        
        verify(matchedOrganizationsGroupedByDocId).mapValues(pickBestMatchFunction.capture());
        assertPickBestMatchFunction(pickBestMatchFunction.getValue());
        
        verify(matchedOrganizationsBestPicked).values();
    }
    
    
    
    //------------------------ PRIVATE --------------------------

    
    private void assertConvertFunction(Function<AffMatchResult, MatchedOrganization> function) throws Exception {

        // given
        
        AffMatchResult affMatchResult = mock(AffMatchResult.class);
        MatchedOrganization matchedAff = mock(MatchedOrganization.class);
        
        when(affMatchResultConverter.convert(affMatchResult)).thenReturn(matchedAff);

        
        // execute
        
        MatchedOrganization retMatchedAff = function.call(affMatchResult);

        
        // assert
        
        assertNotNull(retMatchedAff);
        assertTrue(matchedAff == retMatchedAff);
        
    }
    
    private void assertExtractDocIdFunction(Function<MatchedOrganization, CharSequence> function) throws Exception {
        
        // given
        MatchedOrganization matchedOrg = new MatchedOrganization("DOC_ID", "ORG_ID", 0.6f);
        
        // execute
        CharSequence extractedDocId = function.call(matchedOrg);
        
        // assert
        assertEquals("DOC_ID", extractedDocId);
    }
    
    private void assertPickBestMatchFunction(Function<Iterable<MatchedOrganization>, MatchedOrganization> function) throws Exception {
        
        // given
        
        MatchedOrganization matchedOrg1 = mock(MatchedOrganization.class);
        MatchedOrganization matchedOrg2 = mock(MatchedOrganization.class);
        
        when(bestMatchedOrganizationWithinDocumentPicker.pickBest(of(matchedOrg1, matchedOrg2))).thenReturn(matchedOrg2);
        
        
        // execute
        
        MatchedOrganization retMatchedOrg = function.call(of(matchedOrg1, matchedOrg2));
        
        
        // assert
        
        assertNotNull(retMatchedOrg);
        assertTrue(retMatchedOrg == matchedOrg2);
    }
}
