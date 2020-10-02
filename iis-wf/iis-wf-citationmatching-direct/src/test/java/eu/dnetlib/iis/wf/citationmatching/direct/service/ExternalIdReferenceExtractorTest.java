package eu.dnetlib.iis.wf.citationmatching.direct.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Tuple2;

import java.util.Iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author madryk
 *
 */
@ExtendWith(MockitoExtension.class)
public class ExternalIdReferenceExtractorTest {

    private ExternalIdReferenceExtractor externalIdReferenceExtractor = new ExternalIdReferenceExtractor();

    @Mock
    private JavaRDD<DocumentMetadata> docMetadataRdd;

    @Mock
    private JavaPairRDD<String, Citation> externalIdReferencesRdd;

    @Captor
    private ArgumentCaptor<PairFlatMapFunction<DocumentMetadata, String, Citation>> flatMapCitationsFunctionArg;

    //------------------------ TEST --------------------------

    @Test
    public void extractExternalIdReferences_NULL_ID_TYPE() {
        // execute
        assertThrows(NullPointerException.class, () ->
                externalIdReferenceExtractor.extractExternalIdReferences(docMetadataRdd, null));
    }

    @Test
    public void extractExternalIdReferences_NULL_DOC_METADATA() {
        // execute
        assertThrows(NullPointerException.class, () ->
                externalIdReferenceExtractor.extractExternalIdReferences(null, "someIdType"));
    }

    @Test
    public void extractExternalIdReferences() throws Exception {
        // given
        doReturn(externalIdReferencesRdd).when(docMetadataRdd).flatMapToPair(any());

        // execute
        JavaPairRDD<String, Citation> retExternalIdReferencesRdd = externalIdReferenceExtractor.extractExternalIdReferences(docMetadataRdd, "someIdType");

        // assert
        assertSame(retExternalIdReferencesRdd, externalIdReferencesRdd);
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

        assertThat(toIterable(function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList()))),
                iterableWithSize(0));
        assertThat(toIterable(function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList(referenceMetadata1)))),
                iterableWithSize(0));
        assertThat(toIterable(function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList(referenceMetadata2)))),
                iterableWithSize(0));

        ImmutableList<Tuple2<String, Citation>> references = ImmutableList
                .copyOf(function.call(new DocumentMetadata("id-1", null, null, Lists.newArrayList(referenceMetadata3, referenceMetadata4))));

        assertEquals(references.size(), 2);
        Tuple2<String, Citation> firstReference = references.get(0);
        Tuple2<String, Citation> secondReference = references.get(1);
        
        assertThat(firstReference, equalTo(new Tuple2<>("ref.id1", new Citation("id-1", 3, null))));
        assertThat(secondReference, equalTo(new Tuple2<>("ref.id2", new Citation("id-1", 4, null))));
    }

    private static <T> Iterable<T> toIterable(Iterator<T> iterator){
        return () -> iterator;
    }
}
