package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.spark.avro.AvroDatasetWriter;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.export.schemas.Citations;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public final class CitationRelationExporterIOUtils {

    private static final Logger logger = LoggerFactory.getLogger(CitationRelationExporterIOUtils.class);

    private CitationRelationExporterIOUtils() {
    }

    public static void clearOutput(SparkSession spark,
                                   String outputRelationPath,
                                   String outputReportPath) {
        clearOutput(outputRelationPath, outputReportPath, path -> {
            try {
                HdfsUtils.remove(spark.sparkContext().hadoopConfiguration(), path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void clearOutput(String outputRelationPath,
                                   String outputReportPath,
                                   Consumer<String> removeFn) {
        logger.info("Removing output location: {}.", outputRelationPath);
        removeFn.accept(outputRelationPath);

        logger.info("Removing output location: {}.", outputReportPath);
        removeFn.accept(outputReportPath);
    }

    public static Dataset<Row> readCitations(SparkSession spark,
                                             String inputCitationsPath) {
        return readCitations(inputCitationsPath, path -> new AvroDataFrameReader(spark).read(path, Citations.SCHEMA$));
    }

    public static Dataset<Row> readCitations(String inputCitationsPath,
                                             Function<String, Dataset<Row>> readFn) {
        logger.info("Reading citations from path: {}.", inputCitationsPath);
        return readFn.apply(inputCitationsPath);
    }

    public static void storeSerializedActions(SparkSession spark,
                                              Dataset<Text> serializedActions,
                                              String outputRelationPath) {
        storeSerializedActions(serializedActions, outputRelationPath, (javaPairRDD, path) ->
                RDDUtils.saveTextPairRDD(javaPairRDD, path, spark.sparkContext().hadoopConfiguration()));
    }

    public static void storeSerializedActions(Dataset<Text> serializedActions,
                                              String outputRelationPath,
                                              BiConsumer<JavaPairRDD<Text, Text>, String> writeFn) {
        logger.info("Writing serialized actions to path: {}.", outputRelationPath);
        writeFn.accept(datasetToPairRDD(serializedActions), outputRelationPath);
    }

    private static JavaPairRDD<Text, Text> datasetToPairRDD(Dataset<Text> serializedActions) {
        return serializedActions.javaRDD()
                .mapToPair((PairFunction<Text, Text, Text>) content -> new Tuple2<>(new Text(), content));
    }

    public static void storeReportEntries(SparkSession spark,
                                          Dataset<ReportEntry> reportEntries,
                                          String outputReportPath) {
        storeReportEntries(reportEntries, outputReportPath, (ds, path) ->
                new AvroDatasetWriter<>(ds).write(path, ReportEntry.SCHEMA$));
    }

    public static void storeReportEntries(Dataset<ReportEntry> reportEntries,
                                          String outputReportPath,
                                          BiConsumer<Dataset<ReportEntry>, String> writeFn) {
        logger.info("Storing report data in path {}.", outputReportPath);
        writeFn.accept(reportEntries.repartition(1), outputReportPath);
    }
}
