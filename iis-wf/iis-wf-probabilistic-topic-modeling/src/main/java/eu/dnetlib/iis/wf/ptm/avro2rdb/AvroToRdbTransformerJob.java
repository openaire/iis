package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static org.apache.spark.sql.functions.explode;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;

import eu.dnetlib.iis.common.java.io.HdfsUtils;


public class AvroToRdbTransformerJob {
    
    private static final Logger log = Logger.getLogger(AvroToRdbTransformerJob.class);
    
    private static final String RDB_URL = "jdbc:postgresql://10.19.65.16/ptm";
    
    private static final SaveMode SAVE_MODE = SaveMode.Append;
    
    private static final String TABLE_PUBLICATION = "Publication";
    private static final String TABLE_PUB_GRANT = "PubGrant";
    private static final String TABLE_PUB_KEYWORD = "PubKeyword";
    private static final String TABLE_CITATION = "Citation";
    private static final String TABLE_PUB_CITATION = "PubCitation";
    
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
            
            DataFrame metadataSubset = metadata.select(
                    metadata.col("id").as("pubId"),
                    metadata.col("title"), 
                    metadata.col("abstract"), 
                    metadata.col("year")
            );
            
            DataFrame textDeduped = text.dropDuplicates(new String[] {"id"});
            DataFrame metadataJoinedWithText = metadataSubset.join(
                    textDeduped, metadataSubset.col("pubId").equalTo(textDeduped.col("id")), "left_outer");
            
            DataFrame metadataFiltered = metadataJoinedWithText.filter("abstract is not null or text is not null");
            
            DataFrame normalizedPublication = metadataFiltered.select(
                    metadataFiltered.col("pubId"),
                    metadataFiltered.col("title"), 
                    metadataFiltered.col("abstract"), 
                    metadataFiltered.col("year").as("pubyear"),
                    metadataFiltered.col("text").as("fulltext")
                    );
            
            // writeToRdb(normalizedPublication, TABLE_PUBLICATION, params.postgresPassword);
            
            // ==============================================================================
            // Pub Grant
            // ==============================================================================
            
            // FIXME make sure proper join type is used, inner join should be fine here
            // joinType One of: `inner`, `outer`, `left_outer`, `right_outer`, `leftsemi`
            DataFrame documentJoinedWithProjectDetails = documentToProject.join(
                    project, documentToProject.col("projectId").equalTo(project.col("id")));
            
            DataFrame normalizedPubGrant = documentJoinedWithProjectDetails
                    .filter("confidenceLevel > " + params.confidenceLevelThreshold)
                    .select(
                            documentJoinedWithProjectDetails.col("documentId").as("pubId"),
                            documentJoinedWithProjectDetails.col("projectGrantId").as("project_code"),
                            documentJoinedWithProjectDetails.col("fundingClass").as("funder")
                    );
            
            // writeToRdb(normalizedPubGrant, TABLE_PUB_GRANT, params.postgresPassword);
            
            // ==============================================================================
            // PubKeyword
            // ==============================================================================

            DataFrame metadataKeywordsExploded = metadata.select(
                    metadata.col("id").as("pubId"),
                    explode(metadata.col("keywords")).as("keyword"));
            
            // writeToRdb(metadataKeywordsExploded, TABLE_PUB_KEYWORD, params.postgresPassword);
            
            // ==============================================================================
            // Citation
            // ==============================================================================
            // TODO in fact we are interested only in matched citations, so we should rely on citationmatching outcome rather than references
            
            DataFrame metadataReferencesExploded = metadata.select(
                    metadata.col("id").as("pubId"),
                    explode(metadata.col("references.text")).as("reference"));
            // metadataReferencesExploded.printSchema();
            // notice: currently we cannot write to this table due PK restriction, we do have duplicates
            // writeToRdb(metadataReferencesExploded, TABLE_CITATION);
            
            // https://stackoverflow.com/questions/30501300/is-spark-dataframe-nested-structure-limited-for-selection
            // there is a problem with arrays and lists from java beans
            // https://stackoverflow.com/questions/35986157/creating-a-dataframe-out-of-nested-user-defined-objects
            
            // ==============================================================================
            // Pub Citation
            // ==============================================================================

            
            // ==============================================================================
            
        }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static Properties prepareConnectionProperties(String postgresPassword) {
        Properties props = new Properties();
        props.setProperty("user", "openaire");
        props.setProperty("password", postgresPassword);
        props.setProperty("driver", "org.postgresql.Driver");
        return props;
    }
    
    private static void writeToRdb(DataFrame dataFrame, String table, String postgresPassword) {
        // based on:
        // http://www.sparkexpert.com/2015/04/17/save-apache-spark-dataframe-to-database/
        dataFrame.write().mode(SAVE_MODE).jdbc(RDB_URL, table, prepareConnectionProperties(postgresPassword));
    }
    
    private static void writeToRdbV2(DataFrame dataFrame, String table, String postgresPassword) {
        // based on:
        // https://stackoverflow.com/questions/34849293/recommended-ways-to-load-large-csv-to-rdb-like-mysql
        // all written in single transaction
        JdbcUtils.saveTable(dataFrame, RDB_URL, table, prepareConnectionProperties(postgresPassword));

    }
    
    private static void writeToJson(DataFrame dataFrame, String outputPath) {
        // Saves the subset of the Avro records read in
           dataFrame.write().json(outputPath);
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
        
        @Parameter(names = "-postgresPassword", required = true)
        private String postgresPassword;
    }
}
