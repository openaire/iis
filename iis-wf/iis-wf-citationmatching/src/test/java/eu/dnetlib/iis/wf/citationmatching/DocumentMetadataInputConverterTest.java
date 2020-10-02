package eu.dnetlib.iis.wf.citationmatching;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.wf.citationmatching.converter.DocumentMetadataToMatchableConverter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
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


    @BeforeEach
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

        assertSame(retDocuments, documents);
        verify(inputDocuments).mapToPair(convertDocumentFunction.capture());
        assertConvertDocumentFunction(convertDocumentFunction.getValue());

    }


    //------------------------ PRIVATE --------------------------

    private void assertConvertDocumentFunction(PairFunction<Tuple2<String, DocumentMetadata>, String, MatchableEntity> function) throws Exception {
        DocumentMetadata documentMetadata = mock(DocumentMetadata.class);
        MatchableEntity matchableEntity = mock(MatchableEntity.class);

        when(converter.convertToMatchableEntity("doc_someId", documentMetadata)).thenReturn(matchableEntity);


        Tuple2<String, MatchableEntity> retDocument = function.call(new Tuple2<>("doc_someId", documentMetadata));


        assertSame(retDocument._2, matchableEntity);
        assertEquals("doc_someId", retDocument._1);
    }
}
