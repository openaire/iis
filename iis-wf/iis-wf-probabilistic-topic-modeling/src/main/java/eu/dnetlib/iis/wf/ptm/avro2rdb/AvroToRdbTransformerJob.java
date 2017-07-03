package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static org.apache.spark.sql.functions.explode;

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
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPubKeywordAvroPath);
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
            
            // ==============================================================================
            // Publication
            // ==============================================================================
            // in final solution join text with meta as the last step of processing, after all transformations
            // TODO remove duplicated column and flattenize by creating publication dataframe ready to be written to rdb
            // TODO filter by either text or abstract not empty
            /*
            metadata.printSchema();
            
            DataFrame metadataSubset = metadata.select(
                    metadata.col("id").as("pubId"),
                    metadata.col("title"), 
                    metadata.col("abstract"), 
                    metadata.col("year"),
                    metadata.col("keywords")
                    );
            DataFrame metadataJoinedWithText = metadataSubset.join(text, metadataSubset.col("pubId").equalTo(text.col("id")));
            DataFrame normalizedPublication = metadataJoinedWithText.select(
                    metadataJoinedWithText.col("pubId"),
                    metadataJoinedWithText.col("title"), 
                    metadataJoinedWithText.col("abstract"), 
                    metadataJoinedWithText.col("year").as("pubyear"),
                    metadataJoinedWithText.col("keywords"),
                    metadataJoinedWithText.col("text").as("fulltext")
                    );
            writeToJson(normalizedPublication, params.outputPublicationAvroPath);
            */
            // this fails with "running beyond memory limits" on 7g, 4 cores
            
            // ==============================================================================
            // Pub Grant
            // ==============================================================================
            
            DataFrame documentJoinedWithProjectDetails = documentToProject.join(project, 
                    documentToProject.col("projectId").equalTo(project.col("id")));
            
            DataFrame normalizedPubGrant = documentJoinedWithProjectDetails
                    .filter("confidenceLevel > " + params.confidenceLevelThreshold)
                    .select(
                            documentJoinedWithProjectDetails.col("documentId").as("pubId"),
                            documentJoinedWithProjectDetails.col("projectGrantId"),
                            documentJoinedWithProjectDetails.col("fundingClass")
                    );
            writeToJson(normalizedPubGrant, params.outputPubGrantAvroPath);
            
            // ==============================================================================
            // PubKeyword
            // ==============================================================================
            metadata.printSchema();
            DataFrame metadataKeywordsExploded = metadata.select(
                    metadata.col("id").as("pubId"),
                    explode(metadata.col("keywords")).as("keyword"));
            metadataKeywordsExploded.printSchema(); 
            writeToJson(metadataKeywordsExploded, params.outputPubKeywordAvroPath);
            
            // ==============================================================================
            // Citation
            // ==============================================================================
            metadata.printSchema();
            DataFrame metadataReferencesExploded = metadata.select(
                    metadata.col("id").as("pubId"),
                    explode(metadata.col("references.text")).as("reference"));
            metadataReferencesExploded.printSchema(); 
            writeToJson(metadataReferencesExploded, params.outputCitationAvroPath);
            // accessing nested text elements: references.text works properly but the column name is "col"

            // https://stackoverflow.com/questions/30501300/is-spark-dataframe-nested-structure-limited-for-selection
            // there is a problem with arrays and lists from java beans
            // https://stackoverflow.com/questions/35986157/creating-a-dataframe-out-of-nested-user-defined-objects
            
            // ==============================================================================
            // Pub Citation
            // ==============================================================================

            
            // ==============================================================================
            
            
            
            // TODO perform repartition to minimize number of partitions what potentially could kill postgress db
            
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
    
    private static void writeToJson(DataFrame dataFrame, String outputPath) {
        // Saves the subset of the Avro records read in
           dataFrame.write().json(outputPath);
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
        
        @Parameter(names = "-outputPubKeywordAvroPath", required = true)
        private String outputPubKeywordAvroPath;
        
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
