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
            
            SQLContext sqlContext = new SQLContext(sc);

            // reading Avro data to dataframes
            
            DataFrame metadata = sqlContext.read().format("com.databricks.spark.avro").load(params.inputMetadataAvroPath);

            DataFrame text = sqlContext.read().format("com.databricks.spark.avro").load(params.inputTextAvroPath);
            
            DataFrame project = sqlContext.read().format("com.databricks.spark.avro").load(params.inputProjectAvroPath);
            
            DataFrame documentToProject = sqlContext.read().format("com.databricks.spark.avro").load(params.inputDocumentToProjectAvroPath);
            
            DataFrame citation = sqlContext.read().format("com.databricks.spark.avro").load(params.inputCitationAvroPath);
            
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
            
            writeToRdb(normalizedPublication, TABLE_PUBLICATION, params.postgresPassword);
            
            // ==============================================================================
            // Pub Grant
            // ==============================================================================
            
            // make sure proper join type is used, inner join should be fine here
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
            
            writeToRdb(normalizedPubGrant, TABLE_PUB_GRANT, params.postgresPassword);
            
            // ==============================================================================
            // PubKeyword
            // ==============================================================================

            DataFrame metadataKeywordsExploded = metadata.select(
                    metadata.col("id").as("pubId"),
                    explode(metadata.col("keywords")).as("keyword"));
            
            writeToRdb(metadataKeywordsExploded, TABLE_PUB_KEYWORD, params.postgresPassword);
            
            // ==============================================================================
            // Citation
            // ==============================================================================
            // TODO we should handle externally matched citations as well
            DataFrame citationInternal = citation.filter("entry.destinationDocumentId is not null").select(
                    citation.col("sourceDocumentId").as("pubId"),
                    citation.col("entry.destinationDocumentId").as("citationId"),
                    citation.col("entry.rawText").as("reference"));
            
            DataFrame citationInternalDedupedByCitationIdWithReferences = citationInternal.select(
                    citationInternal.col("citationId"),
                    citationInternal.col("reference")).dropDuplicates(new String[] {"citationId"});
            // dropping duplicates by citationId is required due to PK restriction
            writeToRdb(citationInternalDedupedByCitationIdWithReferences, TABLE_CITATION, params.postgresPassword);
            
            // ==============================================================================
            // Pub Citation
            // ==============================================================================
            DataFrame citationInternalDedupedWithoutReference = citationInternal.select(
                    citationInternal.col("pubId"),
                    citationInternal.col("citationId")).dropDuplicates();
            writeToRdb(citationInternalDedupedWithoutReference, TABLE_PUB_CITATION, params.postgresPassword);
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

        @Parameter(names = "-inputCitationAvroPath", required = true)
        private String inputCitationAvroPath;
        
        @Parameter(names = "-confidenceLevelThreshold", required = true)
        private Float confidenceLevelThreshold;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
        @Parameter(names = "-postgresPassword", required = true)
        private String postgresPassword;
    }
}
