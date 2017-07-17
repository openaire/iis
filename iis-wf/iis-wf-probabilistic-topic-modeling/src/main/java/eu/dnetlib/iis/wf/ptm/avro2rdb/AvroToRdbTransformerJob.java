package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static org.apache.spark.sql.functions.explode;

import java.io.IOException;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;


public class AvroToRdbTransformerJob {
    
    private static final SaveMode SAVE_MODE = SaveMode.Append;
    
    private static final String TABLE_PUBLICATION = "Publication";
    private static final String TABLE_PUB_GRANT = "PubGrant";
    private static final String TABLE_PUB_KEYWORD = "PubKeyword";
    private static final String TABLE_PUB_FULLTEXT = "PubFulltext";
    private static final String TABLE_CITATION = "Citation";
    private static final String TABLE_PUB_CITATION = "PubCitation";
    private static final String TABLE_PUB_PDBCODE = "PubPDBCode";
    
    private static final String JOIN_TYPE_INNER = "inner";
    private static final String JOIN_TYPE_LEFT_OUTER = "left_outer";
    private static final String JOIN_TYPE_LEFTSEMI = "leftsemi";
    
    private static final String FIELD_PUBID = "pubId";
    
    private static final String FIELD_GRANTID = "grantId";
    private static final String FIELD_FUNDER = "funder";
    
    private static final String FIELD_TITLE = "title";
    private static final String FIELD_ABSTRACT = "abstract";
    private static final String FIELD_FULLTEXT = "fulltext";
    private static final String FIELD_PUBYEAR = "pubyear";
    
    private static final String FIELD_KEYWORD = "keyword";
    
    private static final String FIELD_CITATIONID = "citationId";
    private static final String FIELD_REFERENCE = "reference";
    
    private static final String FIELD_PMCID = "pmcId";
    private static final String FIELD_PDBCODE = "pdbcode";
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AvroToRdbTransformerJobParameters params = new AvroToRdbTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        DatabaseContext dbCtx = new DatabaseContext(params.databaseUrl, params.databaseUserName, params.databasePassword);
        
        SparkConf conf = new SparkConf();
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            SQLContext sqlContext = new SQLContext(sc);

            // reading Avro data to dataframes
            
            DataFrame metadata = sqlContext.read().format("com.databricks.spark.avro").load(params.inputMetadataAvroPath);

            DataFrame text = sqlContext.read().format("com.databricks.spark.avro").load(params.inputTextAvroPath);
            
            DataFrame project = sqlContext.read().format("com.databricks.spark.avro").load(params.inputProjectAvroPath);
            
            DataFrame documentToProject = sqlContext.read().format("com.databricks.spark.avro").load(params.inputDocumentToProjectAvroPath);
            
            DataFrame documentToPdb = sqlContext.read().format("com.databricks.spark.avro").load(params.inputDocumentToPdbAvroPath);
            
            DataFrame citation = sqlContext.read().format("com.databricks.spark.avro").load(params.inputCitationAvroPath);
            

            // ==============================================================================
            // Pub Grant
            // ==============================================================================
            
            DataFrame documentJoinedWithProjectDetails = documentToProject.join(project, 
                    documentToProject.col("projectId").equalTo(project.col("id")), JOIN_TYPE_INNER);
            
            DataFrame normalizedPubGrant = documentJoinedWithProjectDetails
                    .filter(documentJoinedWithProjectDetails.col("confidenceLevel").$greater$eq(
                            params.confidenceLevelDocumentToProjectThreshold).and(
                                    documentJoinedWithProjectDetails.col("fundingClass").rlike(params.fundingClassWhitelist)))
                    .select(
                            documentJoinedWithProjectDetails.col("documentId").as(FIELD_PUBID),
                            documentJoinedWithProjectDetails.col("projectGrantId").as(FIELD_GRANTID),
                            documentJoinedWithProjectDetails.col("fundingClass").as(FIELD_FUNDER)
                    );
            // PubGrant will be written later, after filtering by filteredMetadata
            
            // ==============================================================================
            // Publication
            // ==============================================================================
            
            DataFrame metadataSubset = metadata.select(
                    metadata.col("id").as(FIELD_PUBID),
                    metadata.col(FIELD_TITLE), 
                    metadata.col(FIELD_ABSTRACT), 
                    metadata.col("year"),
                    metadata.col("keywords")
            );
            
            DataFrame textDeduped = text.dropDuplicates(new String[] {"id"});
            DataFrame metadataJoinedWithText = metadataSubset.join(textDeduped, 
                    metadataSubset.col(FIELD_PUBID).equalTo(textDeduped.col("id")), JOIN_TYPE_LEFT_OUTER);
            
            DataFrame metadataFilteredByText = metadataJoinedWithText.filter(
                    metadataJoinedWithText.col(FIELD_ABSTRACT).isNotNull().or(metadataJoinedWithText.col("text").isNotNull()));
            
            DataFrame metadataFilteredByTextAndGrant = metadataFilteredByText.join(normalizedPubGrant, 
                    metadataFilteredByText.col(FIELD_PUBID).equalTo(normalizedPubGrant.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
            
            DataFrame normalizedPublication = metadataFilteredByTextAndGrant.select(
                    metadataFilteredByTextAndGrant.col(FIELD_PUBID),
                    metadataFilteredByTextAndGrant.col(FIELD_TITLE), 
                    metadataFilteredByTextAndGrant.col(FIELD_ABSTRACT), 
                    metadataFilteredByTextAndGrant.col("year").as(FIELD_PUBYEAR)
                    );
            
            writeToRdb(normalizedPublication, TABLE_PUBLICATION, dbCtx);
            
            // filtering pubgrant relations by metadataFiltered (limiting only to the exported publications)
            DataFrame normalizedFilteredPubGrant = normalizedPubGrant.join(normalizedPublication, 
                    normalizedPubGrant.col(FIELD_PUBID).equalTo(normalizedPublication.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
            writeToRdb(normalizedFilteredPubGrant, TABLE_PUB_GRANT, dbCtx);
            
            // ==============================================================================
            // PubKeyword
            // ==============================================================================
            // no need to filter keywords by null, explode does the job
            DataFrame metadataKeywordsExploded = metadataFilteredByTextAndGrant
                    .select(
                            metadataFilteredByTextAndGrant.col(FIELD_PUBID),
                            explode(metadataFilteredByTextAndGrant.col("keywords")).as(FIELD_KEYWORD));
            
            writeToRdb(metadataKeywordsExploded, TABLE_PUB_KEYWORD, dbCtx);
            
            // ==============================================================================
            // PubFulltext
            // ==============================================================================
            DataFrame publicationFulltext = metadataFilteredByTextAndGrant
                    .filter(metadataFilteredByTextAndGrant.col("text").isNotNull())
                    .select(
                            metadataFilteredByTextAndGrant.col(FIELD_PUBID),
                            metadataFilteredByTextAndGrant.col("text").as(FIELD_FULLTEXT));
            
            writeToRdb(publicationFulltext, TABLE_PUB_FULLTEXT, dbCtx);
            
            // ==============================================================================
            // Citation
            // ==============================================================================
            // TODO we should handle externally matched citations as well
            DataFrame citationInternal = citation
                    .filter(citation.col("entry.destinationDocumentId").isNotNull().and(
                            citation.col("entry.confidenceLevel").$greater$eq(params.confidenceLevelCitationThreshold)))
                    .select(
                        citation.col("sourceDocumentId").as(FIELD_PUBID),
                        citation.col("entry.destinationDocumentId").as(FIELD_CITATIONID),
                        citation.col("entry.rawText").as(FIELD_REFERENCE));
            
            DataFrame citationFiltered = citationInternal.join(normalizedPublication, 
                    citationInternal.col(FIELD_PUBID).equalTo(normalizedPublication.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
            
            DataFrame citationInternalDedupedByCitationIdWithReferences = citationFiltered.select(
                    citationFiltered.col(FIELD_CITATIONID),
                    citationFiltered.col(FIELD_REFERENCE)).dropDuplicates(new String[] {FIELD_CITATIONID});
            // dropping duplicates by citationId is required due to PK restriction
            writeToRdb(citationInternalDedupedByCitationIdWithReferences, TABLE_CITATION, dbCtx);
            
            // ==============================================================================
            // Pub Citation
            // ==============================================================================
            DataFrame citationInternalDedupedWithoutReference = citationFiltered.select(
                    citationFiltered.col(FIELD_PUBID),
                    citationFiltered.col(FIELD_CITATIONID)).dropDuplicates();
            writeToRdb(citationInternalDedupedWithoutReference, TABLE_PUB_CITATION, dbCtx);

            // ==============================================================================
            // Pub PDBcodes
            // ==============================================================================
            DataFrame documentToPdbFiltered = documentToPdb.join(normalizedPublication, 
                    documentToPdb.col("documentId").equalTo(normalizedPublication.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
            
            DataFrame normalizedPubPDB = documentToPdbFiltered
                    .filter(documentToPdbFiltered.col("confidenceLevel").$greater$eq(params.confidenceLevelDocumentToPdbThreshold))
                    .select(
                            documentToPdbFiltered.col("documentId").as(FIELD_PMCID),
                            documentToPdbFiltered.col("conceptId").as(FIELD_PDBCODE)
                    ).distinct();
            writeToRdb(normalizedPubPDB, TABLE_PUB_PDBCODE, dbCtx);
            
            // ==============================================================================
        }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    /**
     * Prepares connection properties based on {@link DatabaseContext}.
     */
    private static Properties prepareConnectionProperties(DatabaseContext dbCtx) {
        Properties props = new Properties();
        props.setProperty("user", dbCtx.userName);
        props.setProperty("password", dbCtx.password);
        props.setProperty("driver", "org.postgresql.Driver");
        return props;
    }
    
    /**
     * Writes {@link DataFrame} to given table using database context.
     * 
     * @param dataFrame source data frame to be written
     * @param table destination relational database table
     * @param dbCtx database connection details
     */
    private static void writeToRdb(DataFrame dataFrame, String table, DatabaseContext dbCtx) {
        dataFrame.write().mode(SAVE_MODE).jdbc(dbCtx.url, table, prepareConnectionProperties(dbCtx));
    }
    
    //----------------------- INNER CLASSES ---------------------
    
    private static class DatabaseContext {
        
        private final String url;
        private final String userName;
        private final String password;
        
        public DatabaseContext(String url, String userName, String password) {
            this.url = url;
            this.userName = userName;
            this.password = password;
        }
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
        
        @Parameter(names = "-inputDocumentToPdbAvroPath", required = true)
        private String inputDocumentToPdbAvroPath;

        @Parameter(names = "-inputCitationAvroPath", required = true)
        private String inputCitationAvroPath;
        
        @Parameter(names = "-confidenceLevelCitationThreshold", required = true)
        private Float confidenceLevelCitationThreshold;
        
        @Parameter(names = "-confidenceLevelDocumentToProjectThreshold", required = true)
        private Float confidenceLevelDocumentToProjectThreshold;
        
        @Parameter(names = "-confidenceLevelDocumentToPdbThreshold", required = true)
        private Float confidenceLevelDocumentToPdbThreshold;
        
        @Parameter(names = "-fundingClassWhitelist", required = true)
        private String fundingClassWhitelist;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
        @Parameter(names = "-databaseUrl", required = true)
        private String databaseUrl;
        
        @Parameter(names = "-databaseUserName", required = true)
        private String databaseUserName;
        
        @Parameter(names = "-databasePassword", required = true)
        private String databasePassword;
    }

}
