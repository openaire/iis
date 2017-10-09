package eu.dnetlib.iis.wf.transformers.avro2json;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;


/**
 * Spark job transforming Avro records into Json records.
 * @author mhorst
 *
 */
public class Avro2JsonTransformer {
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AvroToRdbTransformerJobParameters params = new AvroToRdbTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame input = sqlContext.read().format("com.databricks.spark.avro").load(params.input);
            input.write().json(params.output);
        }
    }
    
    //------------------------ PRIVATE --------------------------
    
    
    @Parameters(separators = "=")
    private static class AvroToRdbTransformerJobParameters {
        
        @Parameter(names = "-input", required = true)
        private String input;
        
        @Parameter(names = "-output", required = true)
        private String output;

    }

}