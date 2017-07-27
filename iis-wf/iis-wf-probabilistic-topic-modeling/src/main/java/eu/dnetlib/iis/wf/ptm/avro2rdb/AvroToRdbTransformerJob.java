package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static org.apache.spark.sql.functions.explode;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.schemas.ReportEntry;


/**
 * Spark job exporting data stored in HDFS as avro records into relational database.
 * @author mhorst
 *
 */
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
    private static final String FIELD_DOI = "doi";
    
    private static final String FIELD_KEYWORD = "keyword";
    
    private static final String FIELD_CITATIONID = "citationId";
    private static final String FIELD_REFERENCE = "reference";
    
    private static final String FIELD_PMCID = "pmcId";
    private static final String FIELD_PDBCODE = "pdbcode";
    
    
    private static AvroToRdbCounterReporter counterReporter = new AvroToRdbCounterReporter();
    
    
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
            
            DataFrame inputMetadata = sqlContext.read().format("com.databricks.spark.avro").load(params.inputMetadataAvroPath);

            DataFrame inputText = sqlContext.read().format("com.databricks.spark.avro").load(params.inputTextAvroPath);
            
            DataFrame inputProject = sqlContext.read().format("com.databricks.spark.avro").load(params.inputProjectAvroPath);
            
            DataFrame inputDocumentToProject = sqlContext.read().format("com.databricks.spark.avro").load(params.inputDocumentToProjectAvroPath);
            
            DataFrame inputDocumentToPdb = sqlContext.read().format("com.databricks.spark.avro").load(params.inputDocumentToPdbAvroPath);
            
            DataFrame inputCitation = sqlContext.read().format("com.databricks.spark.avro").load(params.inputCitationAvroPath);


            List<ReportEntry> reportEntries = Lists.newArrayList();
            
            // ==============================================================================
            // Pub Grant
            // ==============================================================================
            
            DataFrame normalizedPubGrant = buildPubGrant(inputDocumentToProject, inputProject, 
                    params.confidenceLevelDocumentToProjectThreshold, params.fundingClassWhitelist);
            normalizedPubGrant.cache();
            // PubGrant will be written later, after filtering by filteredMetadata
            
            // ==============================================================================
            // Publication
            // ==============================================================================

            DataFrame metadataFilteredByTextAndGrant = filterMetadata(inputMetadata, inputText, normalizedPubGrant);
            metadataFilteredByTextAndGrant.cache();
            
            DataFrame publicationId = metadataFilteredByTextAndGrant.select(metadataFilteredByTextAndGrant.col(FIELD_PUBID));
            publicationId.cache();
            
            writeToRdb(metadataFilteredByTextAndGrant.select(
                    metadataFilteredByTextAndGrant.col(FIELD_PUBID),
                    metadataFilteredByTextAndGrant.col(FIELD_TITLE), 
                    metadataFilteredByTextAndGrant.col(FIELD_ABSTRACT),
                    metadataFilteredByTextAndGrant.col(FIELD_DOI),
                    metadataFilteredByTextAndGrant.col("year").as(FIELD_PUBYEAR)), TABLE_PUBLICATION, dbCtx, reportEntries);
            
            // filtering pubgrant relations by metadataFiltered (limiting only to the exported publications)
            DataFrame normalizedFilteredPubGrant = normalizedPubGrant.join(publicationId, 
                    normalizedPubGrant.col(FIELD_PUBID).equalTo(publicationId.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
            writeToRdb(normalizedFilteredPubGrant, TABLE_PUB_GRANT, dbCtx, reportEntries);
            
            // ==============================================================================
            // PubKeyword
            // ==============================================================================

            writeToRdb(buildPubKeyword(metadataFilteredByTextAndGrant), TABLE_PUB_KEYWORD, dbCtx, reportEntries);
            
            // ==============================================================================
            // PubFulltext
            // ==============================================================================

            writeToRdb(buildPubFulltext(metadataFilteredByTextAndGrant), TABLE_PUB_FULLTEXT, dbCtx, reportEntries);
            
            // ==============================================================================
            // Citation
            // ==============================================================================
            
            DataFrame citationFiltered = filterCitation(inputCitation, publicationId, 
                    params.confidenceLevelCitationThreshold);
            citationFiltered.cache();
            
            // dropping duplicates by citationId is required due to PK restriction
            writeToRdb(citationFiltered
                    .select(citationFiltered.col(FIELD_CITATIONID), citationFiltered.col(FIELD_REFERENCE))
                    .dropDuplicates(new String[] { FIELD_CITATIONID }), TABLE_CITATION, dbCtx, reportEntries);
            
            // ==============================================================================
            // Pub Citation
            // ==============================================================================

            writeToRdb(buildPubCitation(citationFiltered), TABLE_PUB_CITATION, dbCtx, reportEntries);

            // ==============================================================================
            // Pub PDBcodes
            // ==============================================================================

            writeToRdb(buildPubPDBCodes(inputDocumentToPdb, publicationId, params.confidenceLevelDocumentToPdbThreshold),
                    TABLE_PUB_PDBCODE, dbCtx, reportEntries);
            
            // ==============================================================================
            // Writing report
            // ==============================================================================
            counterReporter.report(sc, reportEntries, params.outputReportPath);
            
        }
    }
    
    protected static DataFrame filterMetadata(DataFrame metadata, DataFrame text, DataFrame pubGrant) {
        DataFrame metadataSubset = metadata.select(
                metadata.col("id").as(FIELD_PUBID),
                metadata.col(FIELD_TITLE), 
                metadata.col(FIELD_ABSTRACT),
                metadata.col("language"),
                metadata.col("externalIdentifiers").getField("doi").as(FIELD_DOI),
                metadata.col("year"),
                metadata.col("keywords")
        );
        
        DataFrame textDeduped = text.dropDuplicates(new String[] {"id"});
        DataFrame metadataJoinedWithText = metadataSubset.join(textDeduped, 
                metadataSubset.col(FIELD_PUBID).equalTo(textDeduped.col("id")), JOIN_TYPE_LEFT_OUTER);
        
        // filtering by abstract OR text being not null and English language (or unspecified)
        // TODO introduce language recognition for unspecified lang
        DataFrame metadataFilteredByText = metadataJoinedWithText.filter(
                metadataJoinedWithText.col(FIELD_ABSTRACT).isNotNull().or(metadataJoinedWithText.col("text").isNotNull()).and(
                        metadataJoinedWithText.col("language").isNull().or(metadataJoinedWithText.col("language").equalTo("eng"))));
        
        return metadataFilteredByText.join(pubGrant, 
                metadataFilteredByText.col(FIELD_PUBID).equalTo(pubGrant.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
    }
    
    /**
     * @param metadata dataframe with {@link #FIELD_PUBID} and keywords columns
     */
    protected static DataFrame buildPubKeyword(DataFrame metadata) {
     // no need to filter keywords by null, explode does the job
        return metadata.select(metadata.col(FIELD_PUBID), explode(metadata.col("keywords")).as(FIELD_KEYWORD));
    }
    
    /**
     * @param metadata dataframe with {@link #FIELD_PUBID} and text columns
     */
    protected static DataFrame buildPubFulltext(DataFrame metadata) {
        return metadata.filter(metadata.col("text").isNotNull()).select(metadata.col(FIELD_PUBID),
                metadata.col("text").as(FIELD_FULLTEXT));
    }
    
    /**
     * @param citation dataframe with citations read from input avro datastore
     * @param publicationId dataframe with {@link #FIELD_PUBID} column, required for filtering citation datastore
     * @param confidenceLevelThreshold matched citation confidence level threshold
     */
    protected static DataFrame filterCitation(DataFrame citation, DataFrame publicationId,
            float confidenceLevelThreshold) {
        // TODO we should handle externally matched citations as well
        DataFrame citationInternal = citation
                .filter(citation.col("entry.destinationDocumentId").isNotNull().and(
                        citation.col("entry.confidenceLevel").$greater$eq(confidenceLevelThreshold)))
                .select(
                    citation.col("sourceDocumentId").as(FIELD_PUBID),
                    citation.col("entry.destinationDocumentId").as(FIELD_CITATIONID),
                    citation.col("entry.rawText").as(FIELD_REFERENCE));
        
        // filtering by publications subset
        return citationInternal.join(publicationId, 
                citationInternal.col(FIELD_PUBID).equalTo(publicationId.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
    }
    
    /**
     * @param citation dataframe with {@link #FIELD_PUBID} and {@link #FIELD_CITATIONID} columns
     */
    protected static DataFrame buildPubCitation(DataFrame citation) {
        return citation.select(citation.col(FIELD_PUBID), citation.col(FIELD_CITATIONID)).dropDuplicates();
    }
    
    /**
     * @param documentToProject dataframe with document to project relations read from input avro datastore 
     * @param project dataframe with projects read from input avro datastore
     * @param confidenceLevelThreshold document to project relations confidence level threshold
     * @param fundingClassWhitelist project funding class whitelist regex
     */
    protected static DataFrame buildPubGrant(DataFrame documentToProject, DataFrame project, 
            float confidenceLevelThreshold, String fundingClassWhitelist) {
        DataFrame documentJoinedWithProjectDetails = documentToProject.join(project,
                documentToProject.col("projectId").equalTo(project.col("id")), JOIN_TYPE_INNER);
        return documentJoinedWithProjectDetails
                .filter(documentJoinedWithProjectDetails.col("confidenceLevel").$greater$eq(confidenceLevelThreshold)
                        .and(documentJoinedWithProjectDetails.col("fundingClass").rlike(fundingClassWhitelist)))
                .select(documentJoinedWithProjectDetails.col("documentId").as(FIELD_PUBID),
                        documentJoinedWithProjectDetails.col("projectGrantId").as(FIELD_GRANTID),
                        documentJoinedWithProjectDetails.col("fundingClass").as(FIELD_FUNDER));
    }
    
    
    /**
     * @param documentToPdb dataframe with document to pdb relations read from input avro datastore
     * @param publicationId dataframe with {@link #FIELD_PUBID} column, required for filtering documentToPdb datastore 
     * @param confidenceLevelThreshold document to pdb relations confidence level threshold
     */
    protected static DataFrame buildPubPDBCodes(DataFrame documentToPdb, DataFrame publicationId, 
            float confidenceLevelThreshold) {
        // filtering by publications subset
        DataFrame documentToPdbFiltered = documentToPdb.join(publicationId, 
                documentToPdb.col("documentId").equalTo(publicationId.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
        
        return documentToPdbFiltered
                .filter(documentToPdbFiltered.col("confidenceLevel").$greater$eq(confidenceLevelThreshold))
                .select(documentToPdbFiltered.col("documentId").as(FIELD_PMCID),
                        documentToPdbFiltered.col("conceptId").as(FIELD_PDBCODE)
                ).distinct();
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
     * @param tableName destination relational database table name
     * @param dbCtx database connection details
     * @param reportEntries list of report entries to be supplemented
     */
    private static void writeToRdb(DataFrame dataFrame, String tableName, DatabaseContext dbCtx, List<ReportEntry> reportEntries) {
        dataFrame.cache();
        dataFrame.write().mode(SAVE_MODE).jdbc(dbCtx.url, tableName, prepareConnectionProperties(dbCtx));
        reportEntries.add(counterReporter.generateCountReportEntry(dataFrame, tableName));
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
