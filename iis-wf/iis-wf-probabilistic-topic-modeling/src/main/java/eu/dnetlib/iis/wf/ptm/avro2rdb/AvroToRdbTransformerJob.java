package eu.dnetlib.iis.wf.ptm.avro2rdb;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;

import eu.dnetlib.iis.common.java.io.HdfsUtils;


public class AvroToRdbTransformerJob {
    
    private static final Logger log = Logger.getLogger(AvroToRdbTransformerJob.class);
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AvroToRdbTransformerJobParameters params = new AvroToRdbTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPublicationAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPubGrantAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputCitationAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPubCitationAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            SQLContext sqlContext = new SQLContext(sc);

            // reading Avro data to dataframes
            
            DataFrame metadata = sqlContext.read().format("com.databricks.spark.avro").load(params.inputMetadataAvroPath);

            DataFrame text = sqlContext.read().format("com.databricks.spark.avro").load(params.inputTextAvroPath);
            
            DataFrame project = sqlContext.read().format("com.databricks.spark.avro").load(params.inputProjectAvroPath);
            
            DataFrame documentToProject = sqlContext.read().format("com.databricks.spark.avro").load(params.inputDocumentToProjectAvroPath);
            
            
            // filtering
            DataFrame metadataSubset = metadata.select(
                    metadata.col("id").as("pubId"),
                    metadata.col("title"), 
                    metadata.col("abstract"), 
                    metadata.col("year").as("pubyear"),
                    metadata.col("keywords")    // TODO make single string out of array
                    );
            // in final solution join text with meta as the last step of processing, after all transformations
            // TODO remove duplicated column and flattenize by creating publication dataframe ready to be written to rdb
            // TODO filter by either text or abstract not empty
            
//            DataFrame metadataJoinedWithText = metadataSubset.join(text, metadataSubset.col("pubId").equalTo(text.col("id")));
            // this fails with "running beyond memory limits"!
            
            DataFrame documentJoinedWithProjectDetails = documentToProject.join(project, 
                    documentToProject.col("projectId").equalTo(project.col("id")));
            
            DataFrame normalizedPubGrant = documentJoinedWithProjectDetails
                    .filter("confidenceLevel > " + params.confidenceLevelThreshold)
                    .select(
                            documentJoinedWithProjectDetails.col("documentId").as("pubId"),
                            documentJoinedWithProjectDetails.col("projectGrantId"),
                            documentJoinedWithProjectDetails.col("fundingClass")
                    );
            // WHOA! IT WORKED!
            
            // TODO perform repartition to minimize number of partitions what potentially could kill postgress db
            
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.start();

            // writing dataframes to avro first!
//            writeToAvro(metadataJoinedWithText, params.outputPublicationAvroPath);
            writeToAvro(normalizedPubGrant, params.outputPubGrantAvroPath);
             
            log.warn("time taken to write all the data: " + stopWatch.elapsedMillis());
            
        }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private Properties prepareConnectionProperties() {
        Properties props = new Properties();
        props.setProperty("user", "openaire");
        props.setProperty("password", "xxx");
        // TODO should we provide driver class?
        return props;
    }
    
    private static void writeToRdbV1(DataFrame dataFrame,
            String url, String table, Properties connectionProperties) {
        // based on:
        // http://www.sparkexpert.com/2015/04/17/save-apache-spark-dataframe-to-database/
        dataFrame.write().jdbc(url, table, connectionProperties);
    }
    
    private static void writeToRdbV2(DataFrame dataFrame,
            String url, String table, Properties connectionProperties) {
        // based on:
        // https://stackoverflow.com/questions/34849293/recommended-ways-to-load-large-csv-to-rdb-like-mysql
        // all written in single transaction
        JdbcUtils.saveTable(dataFrame, url, table, connectionProperties);

    }
    
    private static void writeToAvro(DataFrame dataFrame, String outputPath) {
     // Saves the subset of the Avro records read in
        dataFrame.write().format("com.databricks.spark.avro")
                // causes failure when set for both read and write
                // does nothing, output file looks exactly the same
//              .option("avroSchema", DocumentToProject.SCHEMA$.toString())
                .save(outputPath);

        // TODO how to set avro schema explicitly, we could surely use AvroSaver
    }
    
    
    @Parameters(separators = "=")
    private static class AvroToRdbTransformerJobParameters {
        
        @Parameter(names = "-inputMetadataAvroPath", required = true)
        private String inputMetadataAvroPath;
        
        @Parameter(names = "-inputTextAvroPath", required = true)
        private String inputTextAvroPath;
        
        @Parameter(names = "-inputProjectAvroPath", required = true)
        private String inputProjectAvroPath;
        
        @Parameter(names = "-inputDocumentToProjectAvroPath", required = true)
        private String inputDocumentToProjectAvroPath;

        
        @Parameter(names = "-confidenceLevelThreshold", required = true)
        private Float confidenceLevelThreshold;
        
        
        @Parameter(names = "-outputPublicationAvroPath", required = true)
        private String outputPublicationAvroPath;
        
        @Parameter(names = "-outputPubGrantAvroPath", required = true)
        private String outputPubGrantAvroPath;
        
        @Parameter(names = "-outputCitationAvroPath", required = true)
        private String outputCitationAvroPath;
        
        @Parameter(names = "-outputPubCitationAvroPath", required = true)
        private String outputPubCitationAvroPath;
        
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
