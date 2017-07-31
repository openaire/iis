package eu.dnetlib.iis.wf.ptm.avro2avro;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;


public class AvroToAvroTransformerJob {
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AvroToAvroTransformerJobParameters params = new AvroToAvroTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            SQLContext sqlContext = new SQLContext(sc);
            
            // Creates a DataFrame from a file
            DataFrame df = sqlContext.read()
                    .format("com.databricks.spark.avro")
                    .load(params.inputAvroPath);

            // filtering
            DataFrame filteredDf = df.filter("confidenceLevel > " + params.confidenceLevelThreshold);
            
            // Saves the subset of the Avro records read in
            filteredDf.write()
                    .format("com.databricks.spark.avro")
                    // causes failure when set for both read and write
                    // does nothing, output file looks exactly the same
//                  .option("avroSchema", DocumentToProject.SCHEMA$.toString())
                    .save(params.outputAvroPath);
            
            // TODO how to set avro schema explicitly, we could surely use AvroSaver
            
        }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    
    @Parameters(separators = "=")
    private static class AvroToAvroTransformerJobParameters {
        
        @Parameter(names = "-confidenceLevelThreshold", required = true)
        private Float confidenceLevelThreshold;
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
