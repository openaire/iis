package eu.dnetlib.iis.wf.citationmatching.direct.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 
 * @author madryk
 *
 */
@ExtendWith(MockitoExtension.class)
public class IdentifierMappingExtractorTest {

    private IdentifierMappingExtractor idMappingExtractor = new IdentifierMappingExtractor();
    
    @Mock
    private JavaRDD<DocumentMetadata> documentsMetadataRdd;
    
    
    @Captor
    private ArgumentCaptor<Function<DocumentMetadata, Boolean>> filterDocumentsMetadataFunctionArg;
    @Mock
    private JavaRDD<DocumentMetadata> filteredDocumentsMetadataRdd;
    
    
    @Captor
    private ArgumentCaptor<Function<DocumentMetadata, String>> attachExternalIdKeyFunctionArg;
    @Mock
    private JavaPairRDD<String, DocumentMetadata> documentsMetadataWithExternalIdKeyRdd;
    
    
    @Mock
    private JavaPairRDD<String, Iterable<DocumentMetadata>> groupedByKeyDocumentsMetadataRdd;
    
    
    @Mock
    private Function<Iterable<DocumentMetadata>, DocumentMetadata> pickOneDocumentMetadataFunction;
    @Mock
    private JavaPairRDD<String, DocumentMetadata> documentsMetadataWithUniqueExternalIdKeyRdd;
    
    
    @Captor
    private ArgumentCaptor<Function<DocumentMetadata, String>> extractIdFunctionArg;
    @Mock
    private JavaPairRDD<String, String> idMappingRdd;
    
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void extractIdMapping_NULL_DOCUMENTS_METADATA_RDD() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                idMappingExtractor.extractIdMapping(null, "someIdType", pickOneDocumentMetadataFunction));
    }
    
    
    @Test
    public void extractIdMapping_NULL_ID_TYPE() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                idMappingExtractor.extractIdMapping(documentsMetadataRdd, null, pickOneDocumentMetadataFunction));
    }
    
    
    @Test
    public void extractIdMapping_NULL_PICK_SINGLE_FUNCTION() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                idMappingExtractor.extractIdMapping(documentsMetadataRdd, "someIdType", null));
    }
    
    
    @Test
    public void extractIdMapping() throws Exception {
        
        // given
        
        doReturn(filteredDocumentsMetadataRdd).when(documentsMetadataRdd).filter(any());
        doReturn(documentsMetadataWithExternalIdKeyRdd).when(filteredDocumentsMetadataRdd).keyBy(any());
        doReturn(groupedByKeyDocumentsMetadataRdd).when(documentsMetadataWithExternalIdKeyRdd).groupByKey();
        doReturn(documentsMetadataWithUniqueExternalIdKeyRdd).when(groupedByKeyDocumentsMetadataRdd).mapValues(any());
        doReturn(idMappingRdd).when(documentsMetadataWithUniqueExternalIdKeyRdd).mapValues(any());
        
        
        
        // execute
        
        JavaPairRDD<String, String> retIdMappingRdd = idMappingExtractor.extractIdMapping(documentsMetadataRdd, "someIdType", pickOneDocumentMetadataFunction);
        
        
        // assert

        assertSame(retIdMappingRdd, idMappingRdd);
        
        verify(documentsMetadataRdd).filter(filterDocumentsMetadataFunctionArg.capture());
        assertFilterFunction(filterDocumentsMetadataFunctionArg.getValue());
        
        verify(filteredDocumentsMetadataRdd).keyBy(attachExternalIdKeyFunctionArg.capture());
        assertAttachExternalIdKeyFunction(attachExternalIdKeyFunctionArg.getValue());
        
        verify(documentsMetadataWithExternalIdKeyRdd).groupByKey();
        
        verify(groupedByKeyDocumentsMetadataRdd).mapValues(same(pickOneDocumentMetadataFunction));
        
        verify(documentsMetadataWithUniqueExternalIdKeyRdd).mapValues(extractIdFunctionArg.capture());
        assertExtractIdFunction(extractIdFunctionArg.getValue());
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertFilterFunction(Function<DocumentMetadata, Boolean> function) throws Exception {
        DocumentMetadata docMetadata1 = new DocumentMetadata("id-1", null, null, Lists.newArrayList());
        DocumentMetadata docMetadata2 = new DocumentMetadata("id-1", Maps.newHashMap(), null, Lists.newArrayList());
        
        DocumentMetadata docMetadata3 = new DocumentMetadata("id-1", Maps.newHashMap(), null, Lists.newArrayList());
        docMetadata3.getExternalIdentifiers().put("someIdType", "some-id-1");
        docMetadata3.getExternalIdentifiers().put("someOtherIdType", "some-other-id-1");
        
        DocumentMetadata docMetadata4 = new DocumentMetadata("id-1", Maps.newHashMap(), null, Lists.newArrayList());
        docMetadata3.getExternalIdentifiers().put("someOtherIdType", "some-other-id-1");
        docMetadata3.getExternalIdentifiers().put("yetAnotherIdType", "yet-another-id-1");
        
        
        assertFalse(function.call(docMetadata1));
        assertFalse(function.call(docMetadata2));
        assertTrue(function.call(docMetadata3));
        assertFalse(function.call(docMetadata4));
    }
    
    
    private void assertAttachExternalIdKeyFunction(Function<DocumentMetadata, String> function) throws Exception {
        DocumentMetadata docMetadata1 = new DocumentMetadata("id-1", Maps.newHashMap(), null, Lists.newArrayList());
        docMetadata1.getExternalIdentifiers().put("someIdType", "some-id-1");
        docMetadata1.getExternalIdentifiers().put("someOtherIdType", "some-other-id-1");
        
        DocumentMetadata docMetadata2 = new DocumentMetadata("id-2", Maps.newHashMap(), null, Lists.newArrayList());
        docMetadata2.getExternalIdentifiers().put("someIdType", "some-id-2");
        
        assertThat(function.call(docMetadata1), equalTo("some-id-1"));
        assertThat(function.call(docMetadata2), equalTo("some-id-2"));
    }
    
    
    private void assertExtractIdFunction(Function<DocumentMetadata, String> function) throws Exception {
        DocumentMetadata docMetadata1 = new DocumentMetadata("id-1", Maps.newHashMap(), null, Lists.newArrayList());
        docMetadata1.getExternalIdentifiers().put("someIdType", "some-id-1");
        docMetadata1.getExternalIdentifiers().put("someOtherIdType", "some-other-id-1");
        
        DocumentMetadata docMetadata2 = new DocumentMetadata("id-2", Maps.newHashMap(), null, Lists.newArrayList());
        docMetadata2.getExternalIdentifiers().put("someIdType", "some-id-2");
        
        assertThat(function.call(docMetadata1), equalTo("id-1"));
        assertThat(function.call(docMetadata2), equalTo("id-2"));
    }
    
}
