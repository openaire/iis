package eu.dnetlib.iis.wf.citationmatching;

import com.google.common.collect.Lists;
import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

/**
 * @author madryk
 */
@ExtendWith(MockitoExtension.class)
public class DocumentMetadataInputReaderTest {

    private DocumentMetadataInputReader documentMetadataInputReader = new DocumentMetadataInputReader();

    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaPairRDD<AvroKey<DocumentMetadata>, NullWritable> inputRecords;

    @Mock
    private JavaRDD<DocumentMetadata> inputDocuments;

    @Mock
    private JavaPairRDD<String, DocumentMetadata> documents;

    @Captor
    private ArgumentCaptor<PairFunction<DocumentMetadata, String, DocumentMetadata>> attachIdFunction;


    //------------------------ TESTS --------------------------

    @SuppressWarnings("unchecked")
    @Test
    public void readDocuments() throws Exception {

        // given

        doReturn(inputRecords).when(sparkContext).newAPIHadoopFile(any(), any(), any(), any(), any());
        doReturn(inputDocuments).when(inputRecords).map(any());

        doReturn(documents).when(inputDocuments).mapToPair(any());


        // execute

        JavaPairRDD<String, DocumentMetadata> retDocuments = documentMetadataInputReader.readDocuments(sparkContext, "/some/path");


        // assert

        assertSame(retDocuments, documents);
        verify(sparkContext).newAPIHadoopFile(
                eq("/some/path"), eq(AvroKeyInputFormat.class),
                eq(DocumentMetadata.class), eq(NullWritable.class),
                isA(Configuration.class));

        verify(inputDocuments).mapToPair(attachIdFunction.capture());
        assertAttachIdFunction(attachIdFunction.getValue());

    }


    //------------------------ TESTS --------------------------

    private void assertAttachIdFunction(PairFunction<DocumentMetadata, String, DocumentMetadata> function) throws Exception {

        DocumentMetadata docMetadata = DocumentMetadata.newBuilder()
                .setId("someId")
                .setBasicMetadata(new BasicMetadata())
                .setReferences(Lists.newArrayList())
                .build();


        Tuple2<String, DocumentMetadata> retDocWithId = function.call(docMetadata);


        assertEquals("doc_someId", retDocWithId._1);
        assertSame(retDocWithId._2, docMetadata);

    }
}
