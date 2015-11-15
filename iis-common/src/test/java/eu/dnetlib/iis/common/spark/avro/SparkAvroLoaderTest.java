package eu.dnetlib.iis.common.spark.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.avro.Country;
import scala.Tuple2;

/**
 * 
 * @author madryk
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class SparkAvroLoaderTest {

    @Mock
    private JavaSparkContext sparkContext;
    
    @Mock
    private JavaPairRDD<AvroKey<Country>, NullWritable> javaPairRDD;
    
    @Mock
    private JavaRDD<Country> javaRDD;
    
    @Captor
    private ArgumentCaptor<Configuration> configurationCaptor;
    
    @Captor
    private ArgumentCaptor<Function<Tuple2<AvroKey<Country>, NullWritable>, Country>> mapFunctionCaptor;


    //------------------------ TESTS --------------------------
    
    @SuppressWarnings("unchecked")
    @Test
    public void loadJavaRDD() throws Exception {
        
        // given
        
        doReturn(javaPairRDD).when(sparkContext).newAPIHadoopFile(any(), any(), any(), any(), any());
        doReturn(javaRDD).when(javaPairRDD).map(any());
        
        
        // execute
        
        JavaRDD<Country> retJavaRDD = SparkAvroLoader.loadJavaRDD(sparkContext, "/avro/datastore", Country.class);
        
        
        // assert
        
        assertTrue(javaRDD == retJavaRDD);
        
        
        verify(sparkContext).newAPIHadoopFile(
                eq("/avro/datastore"), eq(AvroKeyInputFormat.class),
                eq(Country.class), eq(NullWritable.class),
                configurationCaptor.capture());
        
        assertEquals(Country.SCHEMA$.toString(), configurationCaptor.getValue().get("avro.schema.input.key"));
        
        verify(javaPairRDD).map(mapFunctionCaptor.capture());
        assertMapFunction(mapFunctionCaptor.getValue());
        
        verifyNoMoreInteractions(sparkContext, javaPairRDD);
        verifyZeroInteractions(javaRDD);

    }
    
    @Test(expected = NullPointerException.class)
    public void loadJavaRDD_NULL_CONTEXT() {
        
        // execute
        SparkAvroLoader.loadJavaRDD(null, "/avro/datastore", Country.class);
        
    }
    
    @Test(expected = NullPointerException.class)
    public void loadJavaRDD_NULL_AVRO_PATH() {
        
        // execute
        SparkAvroLoader.loadJavaRDD(sparkContext, null, Country.class);
        
    }
    
    @Test(expected = NullPointerException.class)
    public void loadJavaRDD_NULL_AVRO_CLASS() {
        
        // execute
        SparkAvroLoader.loadJavaRDD(sparkContext, "/avro/datastore", null);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertMapFunction(Function<Tuple2<AvroKey<Country>, NullWritable>, Country> function) throws Exception {
        Country country = mock(Country.class);
        AvroKey<Country> avroKey = new AvroKey<Country>(country);
        
        Tuple2<AvroKey<Country>, NullWritable> pair = new Tuple2<>(avroKey, NullWritable.get());
        
        assertTrue(country == function.call(pair));
    }
    
}
