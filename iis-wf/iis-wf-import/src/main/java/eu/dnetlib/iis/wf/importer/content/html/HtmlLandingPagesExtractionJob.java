package eu.dnetlib.iis.wf.importer.content.html;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentTextWithDOI;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Iterates over the CSV input file to get the tar.gz package locations in S3 and HTML file names to be extracted from those packages.
 * Provides {@link DocumentTextWithDOI} avro records at the output.
 * 
 * @author mhorst
 */
public class HtmlLandingPagesExtractionJob {
    
    private static final String INPUT_CSV_FIELD_ARCHIVE_S3_LOCATION = "archive_s3_location";
    private static final String INPUT_CSV_FIELD_HTML_FILENAME = "html_filename";
    private static final String INPUT_CSV_FIELD_HTML_SIZE = "html_size";
    private static final String INPUT_CSV_FIELD_ID = "id";
    private static final String INPUT_CSV_FIELD_PID_TYPE = "pid_type";
    private static final String INPUT_CSV_FIELD_PID = "pid";
    
    private static final String PID_VALUE_DOI = "doi";

    private static final String COUNTER_CSVINPUT= "import.html_landing_pages.csvinput";
    private static final String COUNTER_OUTPUT = "import.html_landing_pages.output";
    
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    public static void main(String[] args) throws Exception {
        
        CsvToAvroWithHtmlExtractionJobParameters params = new CsvToAvroWithHtmlExtractionJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (SparkSession session = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());
            
            final Configuration hadoopDriverConf = sc.hadoopConfiguration();
            
            HdfsUtils.remove(hadoopDriverConf, params.outputPath);
            HdfsUtils.remove(hadoopDriverConf, params.outputReportPath);

            // Read the CSV
            Dataset<Row> csv = session.read().option("header", "true")
                    .option("multiLine", "false")
                    .csv(params.inputCsv);
            // caching due to generating both reports and output from this dataset
            csv.cache();
            
            final long inputRecordsCount = csv.count();
            final int maxHtmlPageLength = params.maxHtmlPageSize;
            
            // Group by archive_s3_location to minimize downloads
            JavaPairRDD<String, Iterable<Row>> grouped = csv.javaRDD()
                    .filter(x -> Integer.valueOf(x.getAs(INPUT_CSV_FIELD_HTML_SIZE)) <= maxHtmlPageLength)
                    .groupBy(row -> row.getAs(INPUT_CSV_FIELD_ARCHIVE_S3_LOCATION));

            HashMap<String, String> confAsMap = new HashMap<>();
            for (Entry<String, String> entry : hadoopDriverConf) {
                confAsMap.put(entry.getKey(), entry.getValue());
            }
            
            // broadcasting the configuration to all executors in order to access S3 files
            Broadcast<HashMap<String, String>> broadcastedConf = sc.broadcast(confAsMap);
            
            JavaRDD<DocumentTextWithDOI> output = grouped.flatMap(tuple -> {
                // TODO we should consider changing it in the Lampros' code to avoid this kind of replacement here
                String archivePath = tuple._1.replaceFirst("s3://", "s3a://");
                Iterable<Row> rows = tuple._2;

                Map<String, List<Tuple2<String, String>>> htmlMap = new HashMap<>();
                for (Row r : rows) {
                    String htmlFilename = r.getAs(INPUT_CSV_FIELD_HTML_FILENAME);
                    String id = r.getAs(INPUT_CSV_FIELD_ID);
                    String pidType = r.getAs(INPUT_CSV_FIELD_PID_TYPE);
                    String doi = PID_VALUE_DOI.equalsIgnoreCase(pidType) ? r.getAs(INPUT_CSV_FIELD_PID) : null;
                    htmlMap.computeIfAbsent(htmlFilename, k -> new ArrayList<>())
                           .add(new Tuple2<>(id, doi));
                }

                // Download and extract archive from S3
                List<DocumentTextWithDOI> records = new ArrayList<>();
                
                Path path = new Path(archivePath);

                // obtaining s3 related broadcasted configuration parameters
                Configuration executorConfig = new Configuration(false);
                for (Entry<String, String> entry : broadcastedConf.value().entrySet()) {
                    executorConfig.set(entry.getKey(), entry.getValue());
                };

                InputStream s3Stream = path.getFileSystem(executorConfig).open(path);
                                
                try (TarArchiveInputStream tarInput = new TarArchiveInputStream(new GZIPInputStream(s3Stream))) {
                    TarArchiveEntry entry;
                    while ((entry = tarInput.getNextTarEntry()) != null) {
                        if (!entry.isFile()) continue;
                        if (htmlMap.containsKey(entry.getName())) {
                            byte[] content = new byte[(int) entry.getSize()];
                            tarInput.read(content, 0, content.length);
                            String htmlText = new String(content, "UTF-8");
    
                            for (Tuple2<String, String> pair : htmlMap.get(entry.getName())) {
                                DocumentTextWithDOI.Builder documentTextBuilder = DocumentTextWithDOI.newBuilder();
                                documentTextBuilder.setId(pair._1);
                                documentTextBuilder.setDoi(pair._2);
                                documentTextBuilder.setText(htmlText);
                                records.add(documentTextBuilder.build());
                            }
                        }
                    }
                    return records.iterator();
                }
            }).persist(StorageLevel.MEMORY_AND_DISK());

            long totalOutputCount = output.count();
            
            storeInOutput(output,
                    generateReportEntries(sc, inputRecordsCount, totalOutputCount),
                    params.outputPath, params.outputReportPath);
        }
    }
    
    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, long inputRecordsCount,
            long outputRecordsCount) {
        return sparkContext.parallelize(Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(COUNTER_CSVINPUT, inputRecordsCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_OUTPUT, outputRecordsCount)), 1);
    }
    
    private static void storeInOutput(JavaRDD<DocumentTextWithDOI> results, JavaRDD<ReportEntry> reports,
            String resultOutputPath, String reportOutputPath) {
        avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, reportOutputPath);
        avroSaver.saveJavaRDD(results, DocumentTextWithDOI.SCHEMA$, resultOutputPath);
    }
    
    @Parameters(separators = "=")
    private static class CsvToAvroWithHtmlExtractionJobParameters {
        
        @Parameter(names = "-inputCsv", required = true)
        private String inputCsv;
        
        @Parameter(names = "-maxHtmlPageSize", required = true)
        private int maxHtmlPageSize;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
    
}
