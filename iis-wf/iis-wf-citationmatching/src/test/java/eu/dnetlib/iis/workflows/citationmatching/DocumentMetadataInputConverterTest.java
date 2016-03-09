package eu.dnetlib.iis.workflows.citationmatching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.workflows.citationmatching.converter.DocumentMetadataToMatchableConverter;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class DocumentMetadataInputConverterTest {

    private DocumentMetadataInputConverter documentMetadataInputConverter = new DocumentMetadataInputConverter();

    @Mock
    private DocumentMetadataToMatchableConverter converter;

    @Mock
    private JavaPairRDD<String, DocumentMetadata> inputDocuments;

    @Mock
    private JavaPairRDD<String, MatchableEntity> documents;

    @Captor
    private ArgumentCaptor<PairFunction<Tuple2<String, DocumentMetadata>, String, MatchableEntity>> convertDocumentFunction;


    @Before
    public void setup() {
        documentMetadataInputConverter.setConverter(converter);
    }


    //------------------------ LOGIC --------------------------

    @Test
    public void convertDocuments() throws Exception {

        // given

        doReturn(documents).when(inputDocuments).mapToPair(any());


        // execute

        JavaPairRDD<String, MatchableEntity> retDocuments = documentMetadataInputConverter.convertDocuments(inputDocuments);


        // assert

        assertTrue(retDocuments == documents);
        verify(inputDocuments).mapToPair(convertDocumentFunction.capture());
        assertConvertDocumentFunction(convertDocumentFunction.getValue());

    }


    //------------------------ PRIVATE --------------------------

    private void assertConvertDocumentFunction(PairFunction<Tuple2<String, DocumentMetadata>, String, MatchableEntity> function) throws Exception {
        DocumentMetadata documentMetadata = mock(DocumentMetadata.class);
        MatchableEntity matchableEntity = mock(MatchableEntity.class);

        when(converter.convertToMatchableEntity("doc_someId", documentMetadata)).thenReturn(matchableEntity);


        Tuple2<String, MatchableEntity> retDocument = function.call(new Tuple2<>("doc_someId", documentMetadata));


        assertTrue(retDocument._2 == matchableEntity);
        assertEquals("doc_someId", retDocument._1);
    }
}
