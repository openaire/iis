package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithHiveEnabledSparkSession;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkConfHelper;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;


/**
 * Html payloads metadata importer reading data from hive table being a part of pdf aggregation subsystem. 
 * 
 * @author mhorst
 *
 */
public class HiveBasedHtmlPayloadsMetadataImporterJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static final String COUNTER_IMPORTED_RECORDS_TOTAL = "import.crawled.html.urls.fromAggregator";

    private static final String DATABASE_NAME_PLACEHOLDER = "${databaseName}";

    private static final String QUERY_RESOURCE_PATH = "/eu/dnetlib/iis/wf/importer/content/import_html_payloads_metadata.sql";
    
    public static void main(String[] args) throws Exception {

        HiveBasedHtmlPayloadsImporterJobParameters params = new HiveBasedHtmlPayloadsImporterJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = SparkConfHelper.withKryo(new SparkConf());
        if (!StringUtils.isEmpty(params.hiveMetastoreUris)) {
            conf.set("hive.metastore.uris", params.hiveMetastoreUris);    
        }
        
        runWithHiveEnabledSparkSession(conf, params.isSparkSessionShared, sparkSession -> {
            
            HdfsUtils.remove(sparkSession.sparkContext().hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sparkSession.sparkContext().hadoopConfiguration(), params.outputReportPath);
            
            Dataset<Row> result = sparkSession.sql(generateQuery(params.inputDatabaseName));
            
            
            JavaRDD<ReportEntry> reports = generateReportEntries(sparkSession, result.count()); 

            avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, params.outputReportPath);

            result.write().option("header", "true").csv(params.outputPath);
        });
    }

    private static String generateQuery(String databaseName) {
        String queryTemplate = ClassPathResourceProvider.getResourceContent(QUERY_RESOURCE_PATH);
        return queryTemplate.replace(DATABASE_NAME_PLACEHOLDER, databaseName);
    }

    private static JavaRDD<ReportEntry> generateReportEntries(SparkSession sparkSession, long recordsCount) {
        return sparkSession.createDataset(
                Lists.newArrayList(
                        ReportEntryFactory.createCounterReportEntry(COUNTER_IMPORTED_RECORDS_TOTAL, recordsCount)),
                Encoders.kryo(ReportEntry.class)).javaRDD();
    }
        
    @Parameters(separators = "=")
    private static class HiveBasedHtmlPayloadsImporterJobParameters {
        
        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;
        
        @Parameter(names = "-inputDatabaseName", required = true)
        private String inputDatabaseName;
        
        @Parameter(names = "-hiveMetastoreUris", required = true)
        private String hiveMetastoreUris;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
    
}
