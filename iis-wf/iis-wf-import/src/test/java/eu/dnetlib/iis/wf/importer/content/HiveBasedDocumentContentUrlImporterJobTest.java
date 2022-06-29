package eu.dnetlib.iis.wf.importer.content;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;

/**
 * @author mhorst
 *
 */
public class HiveBasedDocumentContentUrlImporterJobTest extends TestWithSharedSparkSession {

    @TempDir
    public Path workingDir;
    
    private Path outputDir;
    private Path outputReportDir;
    
    private HiveBasedDocumentContentUrlImporterJobTest() {
        // enabling hive support in spark session instantiated for this set of tests
        super(true);
    }
    
    @BeforeEach
    public void beforeEach() {
        super.beforeEach();

        outputDir = workingDir.resolve("output");
        outputReportDir = workingDir.resolve("output_report");
    }

    @Test
    public void testImportFromNonExistingTable() throws Exception {
     // when
        String inputTableName = "nonExistingTable";
        String hiveMetastoreUris = "localhost";

        assertThrows(RuntimeException.class, () -> HiveBasedDocumentContentUrlImporterJob.main(new String[]{
                "-sharedSparkSession",
                "-inputTableName", inputTableName,
                "-hiveMetastoreUris", hiveMetastoreUris,
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString(),
        }));
    }

    @Test
    public void testImportFromExistingLocalTable() throws Exception {
        // when
        String inputDbName = "test_aggregation_import_db";
        String inputTableName = "agg_payload";
        String hiveMetastoreUris = "";
        
        // initializing test database
        initializeLocalDatabase(inputDbName, inputTableName);
        spark().sql("INSERT INTO TABLE " + inputDbName + '.' + inputTableName + 
                " VALUES ('od______2367::0001a50c6388e9bfcb791a924ec4b837', "
                + "'s3://some_location', "
                + "'application/pdf', '1024', '3a0fb023de6c8735a52ac9c1edace612')");

        HiveBasedDocumentContentUrlImporterJob.main(new String[]{
                "-sharedSparkSession",
                "-inputTableName", inputDbName + '.' + inputTableName,
                "-hiveMetastoreUris", hiveMetastoreUris,
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString(),
        });
        
        // then
        String expectedOutputPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/content/output/unit_test/document_content_url.json");
        String expectedReportPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/content/output/unit_test/report.json");
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), expectedOutputPath, DocumentContentUrl.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), expectedReportPath, ReportEntry.class);
        
    }
    
    @Test
    public void testImportFromEmptyLocalTable() throws Exception {
        // when
        String inputDbName = "test_aggregation_import_db";
        String inputTableName = "agg_payload";
        String hiveMetastoreUris = "";
        
        // initializing test database
        initializeLocalDatabase(inputDbName, inputTableName);

        HiveBasedDocumentContentUrlImporterJob.main(new String[]{
                "-sharedSparkSession",
                "-inputTableName", inputDbName + '.' + inputTableName,
                "-hiveMetastoreUris", hiveMetastoreUris,
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString(),
        });
        
        // then
        String expectedOutputPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/content/output/unit_test/document_content_url_empty.json");
        String expectedReportPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/content/output/unit_test/report_empty.json");
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.toString(), expectedOutputPath, DocumentContentUrl.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), expectedReportPath, ReportEntry.class);
        
    }
    
    /**
     * Initializes local hive database.
     * @param inputDbName database name to be initialized 
     * @param inputTableName table to name to be recreated
     */
    private void initializeLocalDatabase(String inputDbName, String inputTableName) {
        spark().sql("CREATE DATABASE IF NOT EXISTS " + inputDbName);
        spark().sql("DROP TABLE IF EXISTS " + inputDbName + '.' + inputTableName);    
        spark().sql("CREATE TABLE " + inputDbName + '.' + inputTableName + 
                "(id string, location string, mimetype string, size string, hash string)"
                + " STORED AS PARQUET");
    }
    
}
