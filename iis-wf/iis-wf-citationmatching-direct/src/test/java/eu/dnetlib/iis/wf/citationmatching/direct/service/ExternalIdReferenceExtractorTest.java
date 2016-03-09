package eu.dnetlib.iis.wf.citationmatching.direct.service;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata;
import scala.Tuple2;

/**
 * 
 * @author madryk
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class ExternalIdReferenceExtractorTest {

    private ExternalIdReferenceExtractor externalIdReferenceExtractor = new ExternalIdReferenceExtractor();
    
    
    @Mock
    private JavaRDD<DocumentMetadata> docMetadataRdd;
    
    @Mock
    private JavaPairRDD<String, Citation> externalIdReferencesRdd;
    
    @Captor
    private ArgumentCaptor<PairFlatMapFunction<DocumentMetadata, String, Citation>> flatMapCitationsFunctionArg;
    
    
    //------------------------ TEST --------------------------
    
    @Test(expected = NullPointerException.class)
    public void extractExternalIdReferences_NULL_ID_TYPE() {
        // execute
        externalIdReferenceExtractor.extractExternalIdReferences(docMetadataRdd, null);
    }
    
    @Test(expected = NullPointerException.class)
    public void extractExternalIdReferences_NULL_DOC_METADATA() {
        // execute
        externalIdReferenceExtractor.extractExternalIdReferences(null, "someIdType");
    }
    
    @Test
    public void extractExternalIdReferences() throws Exception {
        
        // given
        doReturn(externalIdReferencesRdd).when(docMetadataRdd).flatMapToPair(any());
        
        
        // execute
        
        JavaPairRDD<String, Citation> retExternalIdReferencesRdd = externalIdReferenceExtractor.extractExternalIdReferences(docMetadataRdd, "someIdType");
        
        
        // assert
        
        assertTrue(retExternalIdReferencesRdd == externalIdReferencesRdd);
        verify(docMetadataRdd).flatMapToPair(flatMapCitationsFunctionArg.capture());
        
        assertFlatMapCitationsFunction(flatMapCitationsFunctionArg.getValue());
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertFlatMapCitationsFunction(PairFlatMapFunction<DocumentMetadata, String, Citation> function) throws Exception {
        ReferenceMetadata referenceMetadata1 = new ReferenceMetadata(1, null);
        ReferenceMetadata referenceMetadata2 = new ReferenceMetadata(2, Maps.newHashMap());
        
        ReferenceMetadata referenceMetadata3 = new ReferenceMetadata(3, Maps.newHashMap());
        referenceMetadata3.getExternalIds().put("someIdType", "ref.id1");
        referenceMetadata3.getExternalIds().put("someOtherIdType", "ref.other.id1");
        
        ReferenceMetadata referenceMetadata4 = new ReferenceMetadata(4, Maps.newHashMap());
        referenceMetadata4.getExternalIds().put("someIdType", "ref.id2");
        referenceMetadata4.getExternalIds().put("someOtherIdType", "ref.other.id2");
        
        
        
        assertThat(function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList())), iterableWithSize(0));
        assertThat(function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList(referenceMetadata1))), iterableWithSize(0));
        assertThat(function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList(referenceMetadata2))), iterableWithSize(0));
        
        Iterable<Tuple2<String, Citation>> references = function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList(referenceMetadata3, referenceMetadata4)));
        
        assertThat(references, iterableWithSize(2));
        Iterator<Tuple2<String, Citation>> referenceIterator = references.iterator();
        Tuple2<String, Citation> firstReference = referenceIterator.next();
        Tuple2<String, Citation> secondReference = referenceIterator.next();
        
        assertThat(firstReference, equalTo(new Tuple2<String, Citation>("ref.id1", new Citation("id-1", 3, null))));
        assertThat(secondReference, equalTo(new Tuple2<String, Citation>("ref.id2", new Citation("id-1", 4, null))));
        
        
    }
    
    
}
