package eu.dnetlib.iis.wf.citationmatching;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Tuple2;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class ReferenceMetadataInputReaderTest {

    private ReferenceMetadataInputReader referenceMetadataInputReader = new ReferenceMetadataInputReader();

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaPairRDD<AvroKey<DocumentMetadata>, NullWritable> inputRecords;

    @Mock
    private JavaRDD<DocumentMetadata> documents;

    @Mock
    private JavaPairRDD<String, ReferenceMetadata> citations;

    @Captor
    private ArgumentCaptor<PairFlatMapFunction<DocumentMetadata, String, ReferenceMetadata>> documentToCitationsFunction;


    //------------------------ TESTS --------------------------

    @SuppressWarnings("unchecked")
    @Test
    public void readCitations() throws Exception {

        // given
        doReturn(inputRecords).when(sparkContext).newAPIHadoopFile(any(), any(), any(), any(), any());
        doReturn(documents).when(inputRecords).map(any());

        doReturn(citations).when(documents).flatMapToPair(any());


        // execute
        JavaPairRDD<String, ReferenceMetadata> retCitations = referenceMetadataInputReader.readCitations(sparkContext, "/some/path");


        // assert
        assertSame(retCitations, citations);
        verify(sparkContext).newAPIHadoopFile(
                eq("/some/path"), eq(AvroKeyInputFormat.class),
                eq(DocumentMetadata.class), eq(NullWritable.class),
                isA(Configuration.class));
        verify(documents).flatMapToPair(documentToCitationsFunction.capture());
        assertDocToCitationsFunction(documentToCitationsFunction.getValue());
    }


    //------------------------ PRIVATE --------------------------

    private void assertDocToCitationsFunction(PairFlatMapFunction<DocumentMetadata, String, ReferenceMetadata> function) throws Exception {
        ReferenceMetadata refMetadata1 = ReferenceMetadata.newBuilder().setPosition(3).setBasicMetadata(new BasicMetadata()).build();
        ReferenceMetadata refMetadata2 = ReferenceMetadata.newBuilder().setPosition(5).setBasicMetadata(new BasicMetadata()).build();
        ReferenceMetadata refMetadata3 = ReferenceMetadata.newBuilder().setPosition(6).setBasicMetadata(new BasicMetadata()).build();

        DocumentMetadata docMetadata = DocumentMetadata.newBuilder()
                .setId("someId")
                .setBasicMetadata(new BasicMetadata())
                .setReferences(Lists.newArrayList(refMetadata1, refMetadata2, refMetadata3))
                .build();


        Iterable<Tuple2<String, ReferenceMetadata>> retCitations = function.call(docMetadata);


        List<Tuple2<String, ReferenceMetadata>> retCitationsList = Lists.newArrayList(retCitations);
        assertEquals(3, retCitationsList.size());

        assertEquals("cit_someId_3", retCitationsList.get(0)._1);
        assertSame(retCitationsList.get(0)._2, refMetadata1);

        assertEquals("cit_someId_5", retCitationsList.get(1)._1);
        assertSame(retCitationsList.get(1)._2, refMetadata2);

        assertEquals("cit_someId_6", retCitationsList.get(2)._1);
        assertSame(retCitationsList.get(2)._2, refMetadata3);
    }
}
