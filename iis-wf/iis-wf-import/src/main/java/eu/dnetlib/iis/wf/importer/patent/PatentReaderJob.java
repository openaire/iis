package eu.dnetlib.iis.wf.importer.patent;

import java.util.Objects;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job responsible for reading {@link ImportedPatent} records from TSV file.
 *
 * @author mhorst
 */
public class PatentReaderJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    public static final Logger log = Logger.getLogger(PatentReaderJob.class);

    private static final String COUNTER_READ_TOTAL = "import.patents";


    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {

        PatentReaderJobParameters params = new PatentReaderJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkSession session = SparkSessionFactory.withConfAndKryo(new SparkConf());
        
        try {
            JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext()); 
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            JavaRDD<ImportedPatent> results = session.read()
                    .option("sep", "\t")
                    .option("header","true")
                    .csv(params.inputTsvLocation)
                    .toJavaRDD()
                    .map(PatentReaderJob::buildEntry);

            storeInOutput(results, generateReportEntries(sc, results), params.outputPath, params.outputReportPath);
        } finally {
            if (Objects.nonNull(session)) {
                session.stop();
            }
        }
    }

    //------------------------ PRIVATE --------------------------

    private static ImportedPatent buildEntry(Row row) {
        return ImportedPatent.newBuilder()
                .setApplnAuth("EP")
                .setApplnNr(row.getAs("appln_nr"))
                .setPublnAuth(row.getAs("publn_auth"))
                .setPublnNr(row.getAs("publn_nr"))
                .setPublnKind(row.getAs("publn_kind"))
                .build();
    }

    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext,
                                                              JavaRDD<?> entries) {
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        ReportEntry fromCacheEntitiesCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_READ_TOTAL, entries.count());
        return sparkContext.parallelize(Lists.newArrayList(fromCacheEntitiesCounter));
    }

    private static void storeInOutput(JavaRDD<ImportedPatent> results, JavaRDD<ReportEntry> reports,
                                      String resultOutputPath, String reportOutputPath) {
        avroSaver.saveJavaRDD(results, ImportedPatent.SCHEMA$, resultOutputPath);
        avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, reportOutputPath);
    }

    @Parameters(separators = "=")
    private static class PatentReaderJobParameters {
        @Parameter(names = "-inputTsvLocation", required = true)
        private String inputTsvLocation;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
