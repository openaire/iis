package eu.dnetlib.iis.wf.citationmatching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.BasicMetadata;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import scala.Tuple2;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
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

        assertTrue(retDocuments == documents);
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
        assertTrue(retDocWithId._2 == docMetadata);

    }
}
