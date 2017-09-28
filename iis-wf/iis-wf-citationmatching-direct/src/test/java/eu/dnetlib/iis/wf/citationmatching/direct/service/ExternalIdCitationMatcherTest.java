package eu.dnetlib.iis.wf.citationmatching.direct.service;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import scala.Tuple2;

/**
 * 
 * @author madryk
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ExternalIdCitationMatcherTest {

    private ExternalIdCitationMatcher externalIdCitationMatcher = new ExternalIdCitationMatcher();
    
    @Mock
    private IdentifierMappingExtractor idMappingExtractor;
    
    @Mock
    private ExternalIdReferenceExtractor referencePicker;
    
    
    @Mock
    private JavaRDD<DocumentMetadata> documentsMetadataRdd;
    @Mock
    private Function<Iterable<DocumentMetadata>, DocumentMetadata> pickOneDocumentMetadataFunction;
    
    @Mock
    private JavaPairRDD<String, String> idMappingRdd;
    
    @Mock
    private JavaPairRDD<String, String> referenceIdMappingRdd;
    
    @Mock
    private JavaPairRDD<String, Citation> externalIdReferencesRdd;
    
    @Mock
    private JavaPairRDD<String, Tuple2<Citation, String>> externalIdReferencesJoinedWithReferenceIdMappingRdd;
    
    @Mock
    private JavaPairRDD<String, Citation> externalIdReferencesJoinedWithReferenceIdMappingMappedToPairRdd;
    
    @Mock
    private JavaPairRDD<String, Tuple2<Citation, String>> joinedRdd;
    
    @Captor
    private ArgumentCaptor<Function<Tuple2<String, Tuple2<Citation, String>>, Citation>> fillCitationFunctionArg;
    
    @Captor
    private ArgumentCaptor<PairFunction<Tuple2<String, Tuple2<Citation, String>>, String, Citation>> mapToPairFunctionArg;
    
    @Mock
    private JavaRDD<Citation> citationsRdd;
    
    
    @Before
    public void setUp() {
        Whitebox.setInternalState(externalIdCitationMatcher, "idMappingExtractor", idMappingExtractor);
        Whitebox.setInternalState(externalIdCitationMatcher, "referencePicker", referencePicker);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void matchCitations_NULL_DOCUMENTS_METADATA_RDD() {
        
        // execute
        externalIdCitationMatcher.matchCitations(null, "someIdType", pickOneDocumentMetadataFunction);
    }
    
    @Test(expected = NullPointerException.class)
    public void matchCitations_NULL_ID_TYPE() {
        
        // execute
        externalIdCitationMatcher.matchCitations(documentsMetadataRdd, null, pickOneDocumentMetadataFunction);
    }
    
    @Test(expected = NullPointerException.class)
    public void matchCitations_NULL_PICK_SINGLE_FUNCTION() {
        
        // execute
        externalIdCitationMatcher.matchCitations(documentsMetadataRdd, "someIdType", null);
    }
    
    
    @Test
    public void matchCitations() throws Exception {
        
        // given
        
        doReturn(idMappingRdd).when(idMappingExtractor).extractIdMapping(documentsMetadataRdd, "someIdType", pickOneDocumentMetadataFunction);
        doReturn(externalIdReferencesRdd).when(referencePicker).extractExternalIdReferences(documentsMetadataRdd, "someIdType");
        
        doReturn(joinedRdd).when(externalIdReferencesRdd).join(idMappingRdd);
        doReturn(citationsRdd).when(joinedRdd).map(any());
        
        
        
        
        // execute
        
        JavaRDD<Citation> retCitationsRdd = externalIdCitationMatcher.matchCitations(documentsMetadataRdd, "someIdType", pickOneDocumentMetadataFunction);
        
        
        // assert
        
        assertTrue(retCitationsRdd == citationsRdd);
        
        verify(idMappingExtractor).extractIdMapping(documentsMetadataRdd, "someIdType", pickOneDocumentMetadataFunction);
        verify(referencePicker).extractExternalIdReferences(documentsMetadataRdd, "someIdType");
        verify(externalIdReferencesRdd).join(idMappingRdd);
        
        verify(joinedRdd).map(fillCitationFunctionArg.capture());
        assertFillCitationFunction(fillCitationFunctionArg.getValue());
        
    }

    @Test(expected = NullPointerException.class)
    public void matchCitationsIndirectly_NULL_DOCUMENTS_METADATA_RDD() {
        
        // execute
        externalIdCitationMatcher.matchCitations(null, referenceIdMappingRdd, "entityIdType", "referenceIdType", pickOneDocumentMetadataFunction);
    }
    
    @Test(expected = NullPointerException.class)
    public void matchCitationsIndirectly_NULL_REFERENCE_ID_MAPPINGS_RDD() {
        
        // execute
        externalIdCitationMatcher.matchCitations(documentsMetadataRdd, null, "entityIdType", "referenceIdType", pickOneDocumentMetadataFunction);
    }
    
    @Test(expected = NullPointerException.class)
    public void matchCitationsIndirectly_NULL_ENTITY_ID_TYPE() {
        
     // execute
        externalIdCitationMatcher.matchCitations(documentsMetadataRdd, referenceIdMappingRdd, null, "referenceIdType", pickOneDocumentMetadataFunction);
    }
    
    @Test(expected = NullPointerException.class)
    public void matchCitationsIndirectly_NULL_REFERENCE_ID_TYPE() {
        
     // execute
        externalIdCitationMatcher.matchCitations(documentsMetadataRdd, referenceIdMappingRdd, "entityIdType", null, pickOneDocumentMetadataFunction);
    }
    
    @Test(expected = NullPointerException.class)
    public void matchCitationsIndirectly_NULL_PICK_SINGLE_FUNCTION() {
        
     // execute
        externalIdCitationMatcher.matchCitations(documentsMetadataRdd, referenceIdMappingRdd, "entityIdType", "referenceIdType", null);
    }
    
    @Test
    public void matchCitationsIndirectly() throws Exception {
        
        // given
        String mainEntityIdType = "pmc";
        String referenceIdType = "pmid";
        
        doReturn(idMappingRdd).when(idMappingExtractor).extractIdMapping(documentsMetadataRdd, mainEntityIdType, pickOneDocumentMetadataFunction);
        doReturn(externalIdReferencesRdd).when(referencePicker).extractExternalIdReferences(documentsMetadataRdd, referenceIdType);
        
        doReturn(externalIdReferencesJoinedWithReferenceIdMappingRdd).when(externalIdReferencesRdd).join(referenceIdMappingRdd);
        
        doReturn(externalIdReferencesJoinedWithReferenceIdMappingMappedToPairRdd).when(externalIdReferencesJoinedWithReferenceIdMappingRdd).mapToPair(any());
        
        doReturn(joinedRdd).when(externalIdReferencesJoinedWithReferenceIdMappingMappedToPairRdd).join(idMappingRdd);
        doReturn(citationsRdd).when(joinedRdd).map(any());
        
        // execute
        
        JavaRDD<Citation> retCitationsRdd = externalIdCitationMatcher.matchCitations(documentsMetadataRdd, referenceIdMappingRdd,
                mainEntityIdType, referenceIdType, pickOneDocumentMetadataFunction);
        
        // assert
        
        assertTrue(retCitationsRdd == citationsRdd);
        
        verify(idMappingExtractor).extractIdMapping(documentsMetadataRdd, mainEntityIdType, pickOneDocumentMetadataFunction);
        verify(referencePicker).extractExternalIdReferences(documentsMetadataRdd, referenceIdType);
        
        verify(externalIdReferencesRdd).join(referenceIdMappingRdd);
        
        verify(externalIdReferencesJoinedWithReferenceIdMappingRdd).mapToPair(mapToPairFunctionArg.capture());
        assertMaptoPairFunction(mapToPairFunctionArg.getValue());
        
        verify(externalIdReferencesJoinedWithReferenceIdMappingMappedToPairRdd).join(idMappingRdd);
        
        verify(joinedRdd).map(fillCitationFunctionArg.capture());
        assertFillCitationFunction(fillCitationFunctionArg.getValue());

    }
    
    //------------------------ PRIVATE --------------------------
    
    private void assertFillCitationFunction(Function<Tuple2<String, Tuple2<Citation, String>>, Citation> function) throws Exception {
        
        Citation partialCitation = new Citation("source-id", 4, null);
        Tuple2<Citation, String> partialCitationWithDestDocumentId = new Tuple2<>(partialCitation, "dest-id");
        
        Tuple2<String, Tuple2<Citation, String>> tuple = new Tuple2<>("external-id", partialCitationWithDestDocumentId);
        
        
        Citation retCitation = function.call(tuple);
        
        
        assertEquals(new Citation("source-id", 4, "dest-id"), retCitation);
    }
    
    private void assertMaptoPairFunction(PairFunction<Tuple2<String, Tuple2<Citation, String>>, String, Citation> function) throws Exception {
        
        Citation inputCitation = new Citation("source-id", 1, null);
        String inputExternalId = "external-id";
        Tuple2<String, Tuple2<Citation, String>> inputTuple = new Tuple2<String, Tuple2<Citation,String>>("groupedId", 
                new Tuple2<Citation, String>(inputCitation, inputExternalId));
        
        Tuple2<String, Citation> result = function.call(inputTuple);
        
        assertEquals(inputExternalId, result._1);
        assertEquals(inputCitation, result._2);
    }
    
}
