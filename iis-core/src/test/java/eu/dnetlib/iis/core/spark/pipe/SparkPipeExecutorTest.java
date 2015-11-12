package eu.dnetlib.iis.core.spark.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.core.schemas.standardexamples.personwithdocuments.Document;
import scala.Tuple2;

/**
 * 
 * @author madryk
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SparkPipeExecutorTest {

	private SparkPipeExecutor pipeExecutor = new SparkPipeExecutor();
	
	
	@Mock private JavaPairRDD<AvroKey<GenericRecord>, NullWritable> avroKeyValueRDD;
	
	@Mock private JavaRDD<AvroKey<GenericRecord>> avroRDD;
	
	@Mock private JavaRDD<String> mappedStringRDD;
	
	@Mock private JavaPairRDD<String, String> mappedKeyValueRDD;
	
	
	@Mock private JavaPairRDD<String, String> sortedKeyValueRDD;
	
	@Mock private JavaRDD<String> stringRDD;
	
	@Mock private JavaRDD<String> reducedStringRDD;
	
	@Mock private JavaRDD<Document> reducedAvroDocumentRDD;
	
	@Mock private JavaPairRDD<AvroKey<GenericRecord>, NullWritable> reducedAvroKeyValueRDD;
	
	
	@Captor
	private ArgumentCaptor<PairFunction<String, String, String>> stringToKeyValueFunctionArg;
	
	@Captor
	private ArgumentCaptor<Function<Tuple2<String, String>, String>> keyValueToStringFunctionArg;
	
	@Captor
	private ArgumentCaptor<Function<String, Document>> jsonToAvroDocumentFunctionArg;
	
	@Captor
	private ArgumentCaptor<PairFunction<Document, AvroKey<GenericRecord>, NullWritable>> avroDocumentToAvroKeyValueFunctionArg;
	
	
	@Before
	public void setUp() {
		
		SparkEnv sparkEnv = mock(SparkEnv.class);
		when(sparkEnv.sparkFilesDir()).thenReturn("/some/spark/dir");
		SparkEnv.set(sparkEnv);
		
	}
	
	
	//------------------------ TESTS --------------------------
	
	@Test
	public void mapTest() throws Exception {
		
		// given
		
		when(avroKeyValueRDD.keys()).thenReturn(avroRDD);
		when(avroRDD.pipe(anyString())).thenReturn(mappedStringRDD);
		when(mappedStringRDD.mapToPair(Matchers.<PairFunction<String, String, String>>any())).thenReturn(mappedKeyValueRDD);
		
		
		// execute
		
		JavaPairRDD<String, String> retMappedRDD = pipeExecutor.doMap(avroKeyValueRDD, "mapScriptName", "-arg=value");
		
		
		// assert
		
		assertTrue(mappedKeyValueRDD == retMappedRDD);
		
		verify(avroKeyValueRDD).keys();
		verify(avroRDD).pipe("/some/spark/dir/mapScriptName -arg=value");
		
		verify(mappedStringRDD).mapToPair(stringToKeyValueFunctionArg.capture());
		assertStringToKeyValueFunction(stringToKeyValueFunctionArg.getValue());
		
		verifyNoMoreInteractions(avroKeyValueRDD, avroRDD, mappedStringRDD, mappedKeyValueRDD);
	}
	
	@Test
	public void reduceTest() throws Exception {
		
		// given
		
		when(mappedKeyValueRDD.sortByKey()).thenReturn(sortedKeyValueRDD);
		when(sortedKeyValueRDD.map(Matchers.<Function<Tuple2<String, String>, String>>any())).thenReturn(stringRDD);
		when(stringRDD.pipe(anyString())).thenReturn(reducedStringRDD);
		
		when(reducedStringRDD.map(Matchers.<Function<String, Document>>any())).thenReturn(reducedAvroDocumentRDD);
		when(reducedAvroDocumentRDD.mapToPair(Matchers.<PairFunction<Document, AvroKey<GenericRecord>, NullWritable>>any())).thenReturn(reducedAvroKeyValueRDD);
		
		
		// execute
		
		JavaPairRDD<AvroKey<GenericRecord>, NullWritable> retReducedRDD = pipeExecutor.doReduce(mappedKeyValueRDD, "reducerScriptName", "-arg=value", Document.class);
		
		
		// assert
		
		assertTrue(reducedAvroKeyValueRDD == retReducedRDD);
		
		verify(mappedKeyValueRDD).sortByKey();
		
		verify(sortedKeyValueRDD).map(keyValueToStringFunctionArg.capture());
		assertKeyValueToStringFunction(keyValueToStringFunctionArg.getValue());
		
		verify(stringRDD).pipe("/some/spark/dir/reducerScriptName -arg=value");
		
		verify(reducedStringRDD).map(jsonToAvroDocumentFunctionArg.capture());
		assertJsonToAvroDocumentFunction(jsonToAvroDocumentFunctionArg.getValue());
		
		verify(reducedAvroDocumentRDD).mapToPair(avroDocumentToAvroKeyValueFunctionArg.capture());
		assertAvroDocumentToAvroKeyValueFunction(avroDocumentToAvroKeyValueFunctionArg.getValue());
		
		
		verifyNoMoreInteractions(mappedKeyValueRDD, sortedKeyValueRDD, stringRDD,
				reducedStringRDD, reducedAvroDocumentRDD, reducedAvroKeyValueRDD);
		
	}
	
	
	//------------------------ PRIVATE --------------------------
	
	private void assertStringToKeyValueFunction(PairFunction<String, String, String> function) throws Exception {
		
		assertEquals("key", function.call("key\tvalue")._1);
		assertEquals("value", function.call("key\tvalue")._2);
		
		assertEquals("key_only", function.call("key_only")._1);
		assertNull(function.call("key_only")._2);
		
	}
	
	private void assertKeyValueToStringFunction(Function<Tuple2<String, String>, String> function) throws Exception {
		
		assertEquals("key\tvalue", function.call(new Tuple2<String, String>("key", "value")));
		assertEquals("key_only", function.call(new Tuple2<String, String>("key_only", null)));
		
	}
	
	private void assertJsonToAvroDocumentFunction(Function<String, Document> function) throws Exception {
		
		assertEquals(new Document(5, "doc_title"), function.call("{\"id\": 5, \"title\": \"doc_title\"}"));
		
	}
	
	private void assertAvroDocumentToAvroKeyValueFunction(PairFunction<Document, AvroKey<GenericRecord>, NullWritable> function) throws Exception {
		Document document = new Document(5, "doc_title");
		
		Tuple2<AvroKey<GenericRecord>, NullWritable> retTuple = function.call(document);
		
		assertEquals(document, retTuple._1.datum());
	}
}
