package eu.dnetlib.iis.common.spark.avro;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import scala.Tuple2;


/**
 * @author Łukasz Dumiszewski
 */
@RunWith(MockitoJUnitRunner.class)
public class SparkAvroSaverTest {

    
    @Captor
    ArgumentCaptor<PairFunction<String, AvroKey<String>, NullWritable>> mapToPairFCaptor;
    
    @Captor
    ArgumentCaptor<Configuration> capturedConfiguration;
    
    @Mock
    JavaRDD<String> javaRDD;
    
    @Mock
    JavaPairRDD<String, NullWritable> javaPairRDD;
    
    @Mock
    private Schema avroSchema;
    
    private String path = "/location";

    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected=IllegalArgumentException.class)
    public void saveDataFrame_EmptyPath() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveDataFrame(Mockito.mock(DataFrame.class), avroSchema, " ");
    }
    
    
    @Test(expected=NullPointerException.class)
    public void saveDataFrame_NullSchema() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveDataFrame(Mockito.mock(DataFrame.class), null, path);
    }
    
    // the saveDataFrame is not tested here, there is only an invocation of a static method there. How to test an invocation of a static method?

    @Test(expected=NullPointerException.class)
    public void saveDataFrame_NullDataFrame() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveJavaRDD(null, avroSchema, path);
    }

    
    
    @Test(expected=IllegalArgumentException.class)
    public void saveJavaPairKeyRDD_EmptyPath() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveJavaPairKeyRDD(javaPairRDD, avroSchema, null);
    }
    
    
    @Test(expected=NullPointerException.class)
    public void saveJavaPairRDD_NullSchema() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveJavaPairKeyRDD(javaPairRDD, null, path);
    }
    

    @Test(expected=NullPointerException.class)
    public void saveJavaPairKeyRDD_NullJavaRDD() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveJavaPairKeyRDD(null, avroSchema, path);
    }
    
    
    @Test
    public void saveJavaPairKeyRDD() {
        
        // given
        
        JavaPairRDD<?, ?> javaPairRDD = Mockito.mock(JavaPairRDD.class);
        
        
        // execute
        
        SparkAvroSaver.saveJavaPairKeyRDD(javaPairRDD, avroSchema, path);
        
        
        // assert
        
        verify(javaPairRDD).saveAsNewAPIHadoopFile(eq(path), eq(AvroKey.class), eq(NullWritable.class), eq(AvroKeyOutputFormat.class), capturedConfiguration.capture());
        assertEquals(avroSchema.toString(), capturedConfiguration.getValue().get("avro.schema.output.key"));
        
        
    }

    @Test(expected=IllegalArgumentException.class)
    public void saveJavaRDD_EmptyPath() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveJavaRDD(javaRDD, avroSchema, " ");
    }
    
    
    @Test(expected=NullPointerException.class)
    public void saveJavaRDD_NullSchema() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveJavaRDD(javaRDD, null, path);
    }
    

    @Test(expected=NullPointerException.class)
    public void saveJavaRDD_NullJavaRDD() throws Exception {
        
        // execute
        
        SparkAvroSaver.saveJavaRDD(null, avroSchema, path);
    }

    
    
    @Test
    public void saveJavaRDD() throws Exception {
        
        // given
        
        doReturn(javaPairRDD).when(javaRDD).mapToPair(Matchers.any());
        
        
        // execute
        
        SparkAvroSaver.saveJavaRDD(javaRDD, avroSchema, path);
        
        
        // assert
        
        verify(javaRDD).mapToPair(mapToPairFCaptor.capture());
        
        assertMapToPairFunction(mapToPairFCaptor.getValue());
        
        verify(javaPairRDD).saveAsNewAPIHadoopFile(eq(path), eq(AvroKey.class), eq(NullWritable.class), eq(AvroKeyOutputFormat.class), capturedConfiguration.capture());
        assertEquals(avroSchema.toString(), capturedConfiguration.getValue().get("avro.schema.output.key"));
         
        
    }
    
    
    //------------------------ PRIVATE --------------------------

    private void assertMapToPairFunction(PairFunction<String, AvroKey<String>, NullWritable> mapToPairFunction) throws Exception {
        Tuple2<AvroKey<String>, NullWritable> tuple = mapToPairFunction.call("xyz");
        assertEquals(new AvroKey<String>("xyz"), tuple._1());
        assertEquals(NullWritable.get(), tuple._2());
    }
}
