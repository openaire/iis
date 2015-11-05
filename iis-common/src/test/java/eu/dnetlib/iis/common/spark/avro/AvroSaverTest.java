package eu.dnetlib.iis.common.spark.avro;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.common.avro.Country;
import eu.dnetlib.iis.common.spark.test.SparkJob;
import eu.dnetlib.iis.common.spark.test.SparkJobBuilder;
import eu.dnetlib.iis.common.spark.test.SparkJobExecutor;
import eu.dnetlib.iis.core.common.AvroTestUtils;

/**
 * @author Łukasz Dumiszewski
 */
@Category(IntegrationTest.class)
public class AvroSaverTest {

    
    private static final Logger log = LoggerFactory.getLogger(AvroSaverTest.class);
    
    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private static File workingDir;
    
    private static String outputDirPath;
    
    
    
    
    @Before
    public void before() {
        
        workingDir = Files.createTempDir();
        outputDirPath = workingDir + "/spark_sql_avro_cloner/output";
    }
    
    
    @After
    public void after() throws IOException {
        
        FileUtils.deleteDirectory(workingDir);
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void test() throws IOException {
        
        // given
        
        SparkJob sparkJob = SparkJobBuilder
                                           .create()
                                           
                                           .setAppName("Spark Avro Saver Test")
                                           
                                           .setMainClass(AvroSaverTest.class)
                                           .build();
        
        
        // execute
        
        executor.execute(sparkJob);
        
        
        
        // assert
        
        
        List<Country> countries = AvroTestUtils.readLocalAvroDataStore(outputDirPath);

        log.info(countries.toString());
        
        assertEquals(4, countries.size());
        
        assertEquals(1, countries.stream().filter(c->c.getIso().equals("PL")).count());
        
        
    }
    
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        
        SparkConf conf = new SparkConf();
       
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            SQLContext sqlContext = new SQLContext(sc);
            
            DataFrame countries = sqlContext.jsonFile("src/test/resources/eu/dnetlib/iis/common/avro/countries.json");
            
            // without these 2 lines below there is no guarantee as to the field order and then 
            // they can be saved not in accordance with avro schema
            countries.registerTempTable("countries");
            countries = sqlContext.sql("select id, name, iso from countries");
            
            log.info(countries.javaRDD().collect().toString());
            
            AvroSaver.save(countries, Country.SCHEMA$, outputDirPath);

            
        }
        
        
    }

    
  
}
