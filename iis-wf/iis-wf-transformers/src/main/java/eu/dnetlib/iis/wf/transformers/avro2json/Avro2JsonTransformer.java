package eu.dnetlib.iis.wf.transformers.avro2json;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

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
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            SQLContext sqlContext = new SQLContext(sc);
            Dataset<Row> input = sqlContext.read().format("com.databricks.spark.avro").load(params.input);
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