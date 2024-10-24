package eu.dnetlib.iis.wf.importer.software.origins;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.SoftwareHeritageOrigin;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;


/**
 * {@link SoftwareHeritageOrigin} importer reading data from HDFS Apache ORC files. 
 * 
 * @author mhorst
 *
 */
public class SoftwareHeritageOriginsImporterJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static final String COUNTER_IMPORTED_RECORDS_TOTAL = "import.software.origins";
    
    public static void main(String[] args) throws Exception {

        SoftwareHeritageOriginsImporterJobParameters params = new SoftwareHeritageOriginsImporterJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        try (SparkSession session = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext()); 
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            Dataset<Row> orcOrigins = session.read().format("orc").load(params.inputPath);
            
            JavaRDD<SoftwareHeritageOrigin> results = buildOutputRecord(orcOrigins, session);
            results.cache();
            
            JavaRDD<ReportEntry> reports = generateReportEntries(session, results.count()); 
            
            avroSaver.saveJavaRDD(results, SoftwareHeritageOrigin.SCHEMA$, params.outputPath);
            avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, params.outputReportPath);
        }
    }
    
    private static JavaRDD<ReportEntry> generateReportEntries(SparkSession sparkSession, long recordsCount) {
        return sparkSession.createDataset(
                Lists.newArrayList(
                        ReportEntryFactory.createCounterReportEntry(COUNTER_IMPORTED_RECORDS_TOTAL, recordsCount)),
                Encoders.kryo(ReportEntry.class)).javaRDD();
    }
    
    private static JavaRDD<SoftwareHeritageOrigin> buildOutputRecord(Dataset<Row> source, SparkSession spark) {
        return AvroDataFrameSupport.toDS(source, SoftwareHeritageOrigin.class).toJavaRDD();
    }
    
    @Parameters(separators = "=")
    private static class SoftwareHeritageOriginsImporterJobParameters {
        
        @Parameter(names = "-inputPath", required = true)
        private String inputPath;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
    
}
