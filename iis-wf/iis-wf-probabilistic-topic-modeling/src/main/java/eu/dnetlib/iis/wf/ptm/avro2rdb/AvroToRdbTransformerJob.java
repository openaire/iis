package eu.dnetlib.iis.wf.ptm.avro2rdb;

import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.buildPubCitation;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.buildPubFulltext;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.buildPubGrant;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.buildPubKeyword;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.buildPubPDBCodes;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.filterCitation;
import static eu.dnetlib.iis.wf.ptm.avro2rdb.AvroToRdbTransformerUtils.filterMetadata;

import java.io.File;
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
    
    private static final String URL_PREFIX_JDBC = "jdbc:";
    
    protected static final String TABLE_PUBLICATION = "Publication";
    protected static final String TABLE_PUB_GRANT = "PubGrant";
    protected static final String TABLE_PUB_KEYWORD = "PubKeyword";
    protected static final String TABLE_PUB_FULLTEXT = "PubFulltext";
    protected static final String TABLE_CITATION = "Citation";
    protected static final String TABLE_PUB_CITATION = "PubCitation";
    protected static final String TABLE_PUB_PDBCODE = "PubPDBCode";
    
    protected static final String JOIN_TYPE_INNER = "inner";
    protected static final String JOIN_TYPE_LEFT_OUTER = "left_outer";
    protected static final String JOIN_TYPE_LEFTSEMI = "leftsemi";
    
    protected static final String FIELD_PUBID = "pubId";
    
    protected static final String FIELD_GRANTID = "grantId";
    protected static final String FIELD_FUNDER = "funder";
    
    protected static final String FIELD_TITLE = "title";
    protected static final String FIELD_ABSTRACT = "abstract";
    protected static final String FIELD_FULLTEXT = "fulltext";
    protected static final String FIELD_PUBYEAR = "pubyear";
    protected static final String FIELD_DOI = "doi";
    
    protected static final String FIELD_KEYWORD = "keyword";
    
    protected static final String FIELD_CITATIONID = "citationId";
    protected static final String FIELD_REFERENCE = "reference";
    
    protected static final String FIELD_PMCID = "pmcId";
    protected static final String FIELD_PDBCODE = "pdbcode";
    
    private static AvroToRdbCounterReporter counterReporter = new AvroToRdbCounterReporter();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        AvroToRdbTransformerJobParameters params = new AvroToRdbTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        DatabaseContext dbCtx = new DatabaseContext(params.databaseUrl, params.databaseUserName, params.databasePassword);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
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
            
            write(metadataFilteredByTextAndGrant.select(
                    metadataFilteredByTextAndGrant.col(FIELD_PUBID),
                    metadataFilteredByTextAndGrant.col(FIELD_TITLE), 
                    metadataFilteredByTextAndGrant.col(FIELD_ABSTRACT),
                    metadataFilteredByTextAndGrant.col(FIELD_DOI),
                    metadataFilteredByTextAndGrant.col("year").as(FIELD_PUBYEAR)), TABLE_PUBLICATION, dbCtx, reportEntries);
            
            // filtering pubgrant relations by metadataFiltered (limiting only to the exported publications)
            DataFrame normalizedFilteredPubGrant = normalizedPubGrant.join(publicationId, 
                    normalizedPubGrant.col(FIELD_PUBID).equalTo(publicationId.col(FIELD_PUBID)), JOIN_TYPE_LEFTSEMI);
            write(normalizedFilteredPubGrant, TABLE_PUB_GRANT, dbCtx, reportEntries);
            
            // ==============================================================================
            // PubKeyword
            // ==============================================================================

            write(buildPubKeyword(metadataFilteredByTextAndGrant), TABLE_PUB_KEYWORD, dbCtx, reportEntries);
            
            // ==============================================================================
            // PubFulltext
            // ==============================================================================

            write(buildPubFulltext(metadataFilteredByTextAndGrant), TABLE_PUB_FULLTEXT, dbCtx, reportEntries);
            
            // ==============================================================================
            // Citation
            // ==============================================================================
            
            DataFrame citationFiltered = filterCitation(inputCitation, publicationId, 
                    params.confidenceLevelCitationThreshold);
            citationFiltered.cache();
            
            // dropping duplicates by citationId is required due to PK restriction
            write(citationFiltered
                    .select(citationFiltered.col(FIELD_CITATIONID), citationFiltered.col(FIELD_REFERENCE))
                    .dropDuplicates(new String[] { FIELD_CITATIONID }), TABLE_CITATION, dbCtx, reportEntries);
            
            // ==============================================================================
            // Pub Citation
            // ==============================================================================

            write(buildPubCitation(citationFiltered), TABLE_PUB_CITATION, dbCtx, reportEntries);

            // ==============================================================================
            // Pub PDBcodes
            // ==============================================================================

            write(buildPubPDBCodes(inputDocumentToPdb, publicationId, params.confidenceLevelDocumentToPdbThreshold),
                    TABLE_PUB_PDBCODE, dbCtx, reportEntries);
            
            // ==============================================================================
            // Writing report
            // ==============================================================================
            counterReporter.report(sc, reportEntries, params.outputReportPath);
            
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
     * @param tableName destination relational database table name
     * @param dbCtx database connection details
     * @param reportEntries list of report entries to be supplemented
     */
    private static void write(DataFrame dataFrame, String tableName, DatabaseContext dbCtx, List<ReportEntry> reportEntries) {
        dataFrame.cache();
        if (dbCtx.url.startsWith(URL_PREFIX_JDBC)) {
            dataFrame.write().mode(SAVE_MODE).jdbc(dbCtx.url, tableName, prepareConnectionProperties(dbCtx));
        } else {
            dataFrame.write().json(dbCtx.url + File.separatorChar +  tableName);
        }
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
