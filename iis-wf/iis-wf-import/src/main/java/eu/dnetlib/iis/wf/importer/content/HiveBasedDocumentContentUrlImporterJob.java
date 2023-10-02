package eu.dnetlib.iis.wf.importer.content;

import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithHiveEnabledSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

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

import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkConfHelper;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;


/**
 * {@link DocumentContentUrl} importer reading data from hive table being a part of pdf aggregation subsystem. 
 * 
 * @author mhorst
 *
 */
public class HiveBasedDocumentContentUrlImporterJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static final String COUNTER_IMPORTED_RECORDS_TOTAL = "import.content.urls.fromAggregator";
    
    public static void main(String[] args) throws Exception {

        HiveBasedDocumentContentUrlImporterJobParameters params = new HiveBasedDocumentContentUrlImporterJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = SparkConfHelper.withKryo(new SparkConf());
        if (!StringUtils.isEmpty(params.hiveMetastoreUris)) {
            conf.set("hive.metastore.uris", params.hiveMetastoreUris);    
        }
        
        runWithHiveEnabledSparkSession(conf, params.isSparkSessionShared, sparkSession -> {
            
            HdfsUtils.remove(sparkSession.sparkContext().hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sparkSession.sparkContext().hadoopConfiguration(), params.outputReportPath);
            
            Dataset<Row> result = sparkSession.sql("select id, location, mimetype, size, hash from "
                    + params.inputTableName + " where location is not null");
            
            JavaRDD<DocumentContentUrl> documentContentUrl = buildOutputRecord(result);
            documentContentUrl.cache();
            
            JavaRDD<ReportEntry> reports = generateReportEntries(sparkSession, documentContentUrl.count()); 
            
            avroSaver.saveJavaRDD(documentContentUrl, DocumentContentUrl.SCHEMA$, params.outputPath);
            avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, params.outputReportPath);
        });
    }
    
    private static JavaRDD<ReportEntry> generateReportEntries(SparkSession sparkSession, long recordsCount) {
        return sparkSession.createDataset(
                Lists.newArrayList(
                        ReportEntryFactory.createCounterReportEntry(COUNTER_IMPORTED_RECORDS_TOTAL, recordsCount)),
                Encoders.kryo(ReportEntry.class)).javaRDD();
    }
    
    private static JavaRDD<DocumentContentUrl> buildOutputRecord(Dataset<Row> source) {
        Dataset<Row> resultDs = source.select(
                concat(lit(InfoSpaceConstants.ROW_PREFIX_RESULT), col("id")).as("id"),
                col("location").as("url"),
                col("mimetype").as("mimeType"),
                col("size").cast("long").divide(1024).as("contentSizeKB"),
                col("hash").as("contentChecksum")
                );
        return AvroDataFrameSupport.toDS(resultDs, DocumentContentUrl.class).toJavaRDD();
    }
    
    @Parameters(separators = "=")
    private static class HiveBasedDocumentContentUrlImporterJobParameters {
        
        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;
        
        @Parameter(names = "-inputTableName", required = true)
        private String inputTableName;
        
        @Parameter(names = "-hiveMetastoreUris", required = true)
        private String hiveMetastoreUris;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
    
}
